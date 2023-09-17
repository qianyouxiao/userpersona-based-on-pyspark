# -*- coding: utf-8 -*-
# Program function：
import os
from pyspark.sql import SparkSession
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
from pyspark.sql.functions import regexp_extract

os.environ['SPARK_HOME'] = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = "F:\\ProgramCJ\\Python\\Python37\\python"
# 服务器路径
# os.environ['SPARK_HOME'] = '/export/servers/spark'
# PYSPARK_PYTHON = "/root/anaconda3/envs/pyspark_env/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

if __name__ == '__main__':
    # 0.准备Spark开发环境
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("AgeModel") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    # 1-构建数据
    df = spark.createDataFrame([('100-200',)], ['str'])
    # 2-DSL方式
    df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).show()
    # 3-SQL方式
    df.createOrReplaceTempView("test")
    spark.sql("""
       SELECT str,regexp_extract(str, '(\\\d+)-(\\\d+)', 1) testColumn \
       from  test""").show()
    # 简单的方式-sql中对\d正则字符进行转义需要使用\\进行
    spark.sql("SELECT regexp_extract('100-200', '(\\\d+)-(\\\d+)', 1)").show()
    spark.sql("select current_timestamp()").show(truncate=False)
    # +-----------------------+
    # | current_timestamp() |
    # +-----------------------+
    # | 2022 - 06 - 11 15: 48:37.475 |
    # +-----------------------+
    spark.sql("select date_sub(current_timestamp(), 60)").show(truncate=False)
    # +-----------------------------------------------+
    # | date_sub(CAST(current_timestamp() AS DATE), 60) |
    # +-----------------------------------------------+
    # | 2022 - 04 - 12 |
    # +-----------------------------------------------+
    # spark.sql("select from_unixtime('2003-04-28 00:00:00',format='yyyy-MM-dd')").show(truncate=False)
    indexAndRFMDS = spark.createDataFrame([(1, 3.892179195140471), (0, 3.0038167938931295), (2, 2.0)],
                                          ['index', 'rfm_center'])
    indexAndRFMDS.select(indexAndRFMDS["index"].alias("eeee")).show()
    # indexAndRFMDS.show()
    spark.stop()