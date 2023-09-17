#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os

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
sc = spark.sparkContext

'''
  * 目的-四个样本的药品的聚类
  * 1-准备SPark环境
  * 2-准备数据
  * 3-准备算法
  * 4-建立模型
  * 5-模型预测分析
  * 6-打印数据的聚类中心
'''
if __name__ == '__main__':
    from numpy import array
    from math import sqrt

    from pyspark.mllib.clustering import KMeans, KMeansModel

    # Load and parse the data
    data = sc.textFile("./mediumNone.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))
    print(type(parsedData))#<class 'pyspark.rdd.PipelinedRDD'>
    # Build the model (cluster the data)
    clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")

    # Evaluate clustering by computing Within Set Sum of Squared Errors
    def error(point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x ** 2 for x in (point - center)]))

    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))
    #Within Set Sum of Squared Error = 2.414213562373095
    # 聚类中心
    print(clusters.clusterCenters) # [1.5, 1.0], [4.5, 3.5]
    # WSSSE
    print(clusters.computeCost(parsedData)) # 1.5