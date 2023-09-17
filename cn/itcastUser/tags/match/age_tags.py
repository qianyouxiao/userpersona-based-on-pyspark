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
   Description :	TODO：
   SourceFile  :	age_tags
   Author      :	86134
   Date	       :	2023/9/15
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
    rule_df = tag_df.where('id=14').select('rule')
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
    tag_five_df = tag_df.where('pid=14').select('id', 'rule')
    tag_five_df.show()

    # step④基于用户数据和五级标签进行匹配：Join关联
    birth_df: DataFrame = es_df.select(es_df['id'].alias('userId'),
                                       F.regexp_replace(es_df['birthday'], '-', '').alias('birth'))
    birth_df.show()

    age_tag_df: DataFrame = tag_five_df.select(tag_five_df['id'].alias('tagsId'),
                                               F.split(tag_five_df['rule'], '-')[0].alias('start'),
                                               F.split(tag_five_df['rule'], '-')[1].alias('end'))
    age_tag_df.show()

    join_df = birth_df.join(age_tag_df).where(birth_df['birth'].between(age_tag_df['start'], age_tag_df['end'])) \
        .select(birth_df['userId'], age_tag_df['tagsId'].cast(StringType()))
    join_df.show()

    # step2: 处理数据：使用SQL或者DSL对DF进行转换
    # step3: 保存结果：打印或者保存
    # step⑤将最后画像的结果写入ES中
    join_df.write \
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
