#!/usr/bin/env python
__coding__ = "utf-8"

import abc
import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from cn.itcastUser.bean.EsMetadata import EsMetaData

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	tagsClass
   Author      :	'钱有霄'
   Date	       :	2023/9/17
-------------------------------------------------
"""

SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

# 构建SparkSession对象
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQL APP") \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()


def get_mysql_tags_df():
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false"
    tbname = "tbl_basic_tag"
    prop = {"user": "root", "password": "123456"}
    return spark.read.jdbc(url=url, table=tbname, properties=prop)


def change_to_dict(rule: str):
    new_dict = {}
    rule_dict = rule.split('##')
    for i in rule_dict:
        item = i.split('=')
        new_dict[item[0]] = item[1]
    return new_dict


def get_es_meta(mysql_tags_df, level4_id):
    rule_df: DataFrame = mysql_tags_df.where(f'id={level4_id}').select('rule')
    rule_dict = rule_df.rdd.map(lambda row: change_to_dict(row.rule)).collect()[0]
    return EsMetaData.getEsMetaFromDict(input=rule_dict)


def get_user_data_from_es(es_meta):
    es_df = spark.read.format('es')\
        .option('es.nodes', es_meta.esNodes)\
        .option('es.resource', f'{es_meta.esIndex}/{es_meta.esType}')\
        .option('es.read.field.include', es_meta.selectFields) \
        .option("es.mapping.date.rich", "false")\
        .load()
    return es_df


def get_mysql_tags_level5_df(mysql_tags_df, level4_id):
    mysql_tags_level5_df = mysql_tags_df.where(f'pid={level4_id}').select('rule', 'id')
    return mysql_tags_level5_df


def get_history_tags(es_meta):
    old_tags_df = spark.read.format('es')\
        .option('es.nodes', es_meta.esNodes)\
        .option('es.resource', f'tfec_userprofile_result/{es_meta.esType}') \
        .option("es.read.field.include", "userId, tagsId")\
        .load()
    return old_tags_df


@F.udf
def merge_tags(new_tags, old_tags):
    # 如果新标签为空，就直接返回老标签
    if new_tags is None:
        return old_tags
    # 如果老标签为空，就直接返回新标签
    elif old_tags is None:
        return new_tags
    # 两份标签都不为空，就直接拼接
    else:
        new_tags_list = str(new_tags).split(",")  # [5]
        old_tags_list = str(old_tags).split(",")  # [5, 18]
        # 拼接
        rs_tags_list = new_tags_list + old_tags_list  # [5,5,18]
        # 返回结果：要一个字符串，考虑是否重复的问题
        return ",".join(set(rs_tags_list))  # "5,18"


def merge_tags_rs(new_tags, old_tags):
    rs_df = new_tags.join(other=old_tags, on='userId', how='left')\
        .select(new_tags['userId'], merge_tags(new_tags['tagsId'], old_tags['tagsId']).alias('tagsId'))
    return rs_df


def write_to_es(rs_df):
    rs_df.write.mode('append').format('es')\
        .option('es.nodes', 'up01:9200') \
        .option("es.index.auto.create", "true")\
        .option('es.resource', 'tfec_userprofile_result/_doc')\
        .option('es.write.operation', 'upsert') \
        .option("es.mapping.id", "userId")\
        .save()


class Tags_basic_Template(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_level4_id(self):
        pass

    @abc.abstractmethod
    def compute(self, es_df, mysql_tags_level5_df):
        pass

    def executor(self):
        # todo:step1: 从MySQL中读取标签体系数据: tbl_basic_tag
        mysql_tags_df: DataFrame = get_mysql_tags_df()

        # todo:step2: 从MySQL中过滤四级标签的数据，将四级标签的rule转换成字典,再将字典构建成实体类对象
        # 获取当前标签的四级标签id
        level4_id = self.get_level4_id()
        # 获取es_meta
        es_meta = get_es_meta(mysql_tags_df, level4_id)

        # todo:step3: 根据rule规则的字典内容从ES中读取相应的业务数据
        es_df: DataFrame = get_user_data_from_es(es_meta)

        # todo:step4: 从MySQL中读取五级标签的数据[pid = 四级标签]
        mysql_tags_level5_df: DataFrame = get_mysql_tags_level5_df(mysql_tags_df, level4_id)

        # todo:step5: 通过ES中的业务数据与MySQL的五级标签进行匹配，构建新的标签数据
        new_tags: DataFrame = self.compute(es_df, mysql_tags_level5_df)

        # todo:step6: 从ES中读取老的用户画像标签
        old_tags: DataFrame = get_history_tags(es_meta)

        # todo:step7: 将老的用户画像标签与新的标签进行合并，得到最终标签
        rs_df: DataFrame = merge_tags_rs(new_tags, old_tags)

        # todo:step8: 将最终的结果写入ES中
        write_to_es(rs_df)
