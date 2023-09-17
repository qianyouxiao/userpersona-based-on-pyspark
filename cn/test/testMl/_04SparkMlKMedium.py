#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os
from pyspark.ml.feature import VectorAssembler

# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
from pyspark.sql import SparkSession

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
    .appName("UserProfileModel") \
    .getOrCreate()

# 1-Loads data.
dataset = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load("./medium.txt")
dataset.show()
# +-----------+----+
# |Weightindex|PH值|
# +-----------+----+
# |        1.0| 1.0|
# |        2.0| 1.0|
# |        4.0| 3.0|
# |        5.0| 4.0|
# +-----------+----+

assembler = VectorAssembler(
    inputCols=["Weightindex", "PH值"],
    outputCol="features")
output = assembler.transform(dataset)
# 2-Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(output)

# 3-Make predictions
predictions = model.transform(output)

# 4-Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# 5-Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)