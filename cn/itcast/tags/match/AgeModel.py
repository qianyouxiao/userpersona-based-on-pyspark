#!/usr/bin/env python
# @desc : 匹配标签：年龄段标签
__coding__ = "utf-8"
__author__ = "itcast team"


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StringType

from cn.itcast.tags.bean.ESMeta import ESMeta
import os

# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
SPARK_HOME = 'E:\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'D:\InstallFolder\Python\Python37\python.exe'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON


def megerDF(newTagsId: str, oldTagsId: str):
    if newTagsId is None:
        return oldTagsId
    elif oldTagsId is None:
        return newTagsId
    else:
        newArr: list = str(newTagsId).split(",")
        oldArr: list = oldTagsId.split(",") # 5 6
        resultArr: list = newArr + oldArr
        # set可以自动去重
        return ",".join(set(resultArr))


def ruleMapfunction(ruleRow: str):
    defaultMap: dict = {}
    ruleFields = ruleRow.split("##")
    for field in ruleFields:
        kv = field.split("=")
        defaultMap[kv[0]] = kv[1]
    return defaultMap


if __name__ == '__main__':
    # 0.准备Spark开发环境
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("AgeModel") \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    # 解决错误：py4j.protocol.Py4JJavaError: An error occurred while calling o45.load.
    #   解决方法：把依赖jar包拷贝到执行环境的SPARK_HOME/jars目录下

    # 1.读取MySQL中的数据
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    tableName = "tbl_basic_tag"
    mysqlDF = spark.read.jdbc(url, tableName)
    # mysqlDF.show(truncate=False)

    # 2.读取和年龄段标签相关的4级标签rule并解析
    fourRuleDS = mysqlDF.select("rule").where("id=14")

    # 解析rule为dict => map python中的dict类型对应scala的map集合
    fourRuleMap = fourRuleDS.rdd.map(lambda row: ruleMapfunction(row.rule)) \
        .collect()[0]
    # print(fourRuleMap)

    # 3.根据解析出的rule读取es数据
    esMeta = ESMeta.fromDictToEsMeta(fourRuleMap)
    # print(esMeta)

    # spark读取es数据源，过滤字段参数：es.read.field.include
    # schema = StructType()\
    #         .add("user_id", StringType(), nullable=True)\
    #         .add("birthday", StringType(), nullable=True)
    # source: org.elasticsearch.spark.sql or es
    esDF = spark.read \
        .format("es") \
        .option("es.resource", f"{esMeta.esIndex}/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", f"{esMeta.selectFields}") \
        .load()
    # .schema("user_id string, birthday timestamp")
    esDF.show(truncate=False)
    esDF.printSchema()

    # 4.读取和年龄段标签相关的5级标签(根据4级标签的id作为pid查询)
    fiveDS = mysqlDF.select("id", "rule").where("pid=14")
    fiveDS.show(truncate=False)
    fiveDS.printSchema()

    from pyspark.sql.functions import regexp_replace

    # 5.根据ES数据和5级标签数据进行匹配,得出userId,tagsId
    # 5.1.统一格式,将1999-09-09统一为:19990909
    esDF2 = esDF.select(esDF.id.alias("userId"), regexp_replace(esDF.birthday[0:10], "-", "").alias("birthday"))
    esDF2.show(truncate=False)

    # 5.2.将fiveDS拆分为("tagsId","start","end")
    fiveDS2 = fiveDS.rdd.map(lambda row:
                             (row.id, row.rule.split("-")[0], row.rule.split("-")[1])
                             ) \
        .toDF(["tagsId", "start", "end"])
    fiveDS2.show(truncate=False)

    # 5.3.将esDF2和fiveDS2直接join
    newDF = esDF2 \
        .join(fiveDS2) \
        .where(esDF2.birthday.between(fiveDS2.start, fiveDS2.end)) \
        .select(esDF2.userId.cast('string'), fiveDS2.tagsId.cast('string'))
    newDF.show(truncate=False)
    newDF.printSchema()

    # 6.查询ES中的oldDF 此方式查询数据，必须要在sparksql创建前建立索引与对应的field？索引不存在此方法报错
    # oldDF = spark.read \
    #     .format("es") \
    #     .option("es.resource", f"tfec_userprofile_result/{esMeta.esType}") \
    #     .option("es.nodes", f"{esMeta.esNodes}") \
    #     .option("es.index.read.missing.as.empty", "yes") \
    #     .option("es.query", "?q=*") \
    #     .option("es.read.field.include", "userId,tagsId") \
    #     .load()
    # oldDF.show(truncate=False)
    # oldDF.printSchema()

    # 7.合并newDF和oldDF 自定义DSL风格的UDF并注册udf
    # merge = udf(megerDF, StringType())
    #
    # resultDF = newDF \
    #     .join(oldDF, newDF.userId == oldDF.userId, "left") \
    #     .select(newDF.userId, merge(newDF.tagsId, oldDF.tagsId).alias("tagsId"))
    #
    # resultDF.show(truncate=False)

    # 8.将最终结果写到ES
    newDF.write \
        .format("es") \
        .option("es.resource", f"tfec_userprofile_result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.mapping.id", "userId") \
        .option("es.mapping.name", "userId:userId,tagsId:tagsId") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()
