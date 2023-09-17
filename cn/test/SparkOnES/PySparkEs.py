#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark import SparkContext
from pyspark.sql import SparkSession

# 0.准备Spark开发环境
from pyspark.sql.types import StructType, StringType
import os


# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
SPARK_HOME = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'F:\\ProgramCJ\\Python\\Python37\\python'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AgeModel") \
    .config("es.index.auto.create", "true")\
    .getOrCreate()

sc: SparkContext = spark.sparkContext
sc.setLogLevel("WARN")

schema = StructType()\
        .add("userId", StringType(), nullable=True)\
        .add("tagsId", StringType(), nullable=True)

df = spark\
    .read\
    .format("es")\
    .option("es.nodes", "192.168.88.166:9200")\
    .option("es.resource", "insurance-profile-result/_doc")\
    .option("es.index.read.missing.as.empty", "yes")\
    .option("es.query", "?q=*") \
    .option("es.write.operation","upsert")\
    .schema(schema)\
    .load()

df.printSchema()
df.show()
print(len(df.head(1)) > 0)