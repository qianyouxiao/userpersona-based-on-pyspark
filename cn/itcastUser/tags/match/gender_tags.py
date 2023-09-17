#!/usr/bin/env python
__coding__ = "utf-8"
__author__ = "itcast team"

import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from cn.itcastUser.bean.EsMetadata import EsMetaData

"""
-------------------------------------------------
   Description :	TODO：性别标签
   SourceFile  :	marital_status_tags
   Author      :	86134
   Date	       :	2023/9/17
-------------------------------------------------
"""

if __name__ == '__main__':
    # todo:0-设置系统环境变量
    SPARK_HOME = '/export/server/spark'
    PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
    # 导入路径
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

    # todo:1-构建SparkSession
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("SparkSQL APP") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据：将数据变成一个DataFrame
    # step①:先用SparkSQL到MySQL中读取标签的rule规则：标签的id14
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false"
    tbname = "tbl_basic_tag"
    prop = {"user": "root", "password": "123456"}

    tag_df = spark.read.jdbc(url=url, table=tbname, properties=prop)
    tag_df.show()
    rule_df = tag_df.where('id=4').select('rule')
    rule_df.show(truncate=False)


    # 将读取到的数据切割放到字典，并返回字典
    def change_to_dict(rule: str):
        rule_dict = {}
        rule_list = rule.split('##')
        for i in rule_list:
            item = i.split('=')
            rule_dict[item[0]] = item[1]
        return rule_dict


    es_dict = rule_df.rdd.map(lambda row: change_to_dict(row.rule)).collect()[0]
    print(es_dict)

    # step②基于rule规则到ES中读取用户数据：user表、id、birthday
    es_metadata = EsMetaData.getEsMetaFromDict(input=es_dict)
    es_df = (spark
             .read
             .format('es')
             .option('es.nodes', es_metadata.esNodes)
             .option('es.resource', f'{es_metadata.esIndex}/{es_metadata.esType}')
             .option('es.read.field.include', es_metadata.selectFields)
             .option('es.mapping.date.rich', 'false')
             .load())
    es_df.show()

    # step③基于四级标签读取对应五级标签：pid = 14
    tag_five_df: DataFrame = tag_df.where('pid=4').select('rule', 'id')
    tag_five_df.show()

    # step④基于用户数据和五级标签进行匹配：Join关联
    # join_df = es_df.join(tag_five_df, es_df['marriage'] == tag_five_df['rule'])\
    #     .select(es_df['id'].alias('userId'), tag_five_df['id'].alias('marital_status'))
    # join_df.show()
    my_dict = tag_five_df.rdd.collectAsMap()
    broadcast_dict = spark.sparkContext.broadcast(my_dict)


    @F.udf
    def get_tags(gender):
        return broadcast_dict.value.get(str(gender))


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


    res_df: DataFrame = es_df.select(es_df['id'].alias('userId'), get_tags(es_df['gender']).alias('tagsId'))

    res_df.show()

    old_tags_df = (spark
                   .read
                   .format('es')
                   .option('es.nodes', es_metadata.esNodes)
                   .option('es.resource', '	tfec_userprofile_result/_doc')
                   .load())
    old_tags_df.show()
    new_tags_df = res_df.join(old_tags_df, on='userId', how='left')\
        .select(res_df['userId'],
                merge_tags(res_df['tagsId'], old_tags_df['tagsId']).alias('tagsId'))
    new_tags_df.show()
    # step2: 处理数据：使用SQL或者DSL对DF进行转换
    # step3: 保存结果：打印或者保存
    # step⑤将最后画像的结果写入ES中
    new_tags_df.write \
        .mode('append') \
        .format('es') \
        .option('es.index.auto.create', 'true') \
        .option('es.nodes', 'up01:9200')\
        .option('es.resource', 'tfec_userprofile_result/_doc')\
        .option('es.write.operation', 'upsert')\
        .option('es.mapping.id', 'userId')\
        .save()

    # todo:3-关闭SparkSession
    spark.stop()
