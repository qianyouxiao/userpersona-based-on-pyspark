#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os
from pyspark.ml.feature import VectorAssembler, MinMaxScaler

# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
if __name__ == '__main__':
    # 1-Loads data.
    dataset = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("./iris.csv")
    #3-特征工程
    vectran =VectorAssembler()\
        .setInputCols(["sepal_length", "sepal_width", "petal_length", "petal_width"])\
        .setOutputCol("features")
    datatrans1 = vectran.transform(dataset)
    #3 - 特征工程 - --最大值最小化的处理 - -----【0, 1】区间
    sclaer= MinMaxScaler().setInputCol("features").setOutputCol("scaledfeatures")
    scalerModel = sclaer.fit(datatrans1)
    datatrans = scalerModel.transform(datatrans1)
    trainset, testset= datatrans.randomSplit([0.8, 0.2], seed=123)
    #  4-建模实战
    kmeans = KMeans().setFeaturesCol("features").setPredictionCol("predictions").setK(3)
    kmeanModel = kmeans.fit(trainset)
    #5 - 预测分析
    testResult = kmeanModel.transform(testset)
    testResult.show()
    testResult.groupBy(["petal_length", "predictions"]).agg({"predictions":"count"}).show()
    #6 - 模型校验分析 - wssse打印结果等
    print("wssse", kmeanModel.computeCost(testResult)) # (wssse, 2.5189564869286865)
    print(kmeanModel.clusterCenters())

    print("肘部法ks-----------------------------------")
    ks= [2, 3, 4]
    for cluster in ks:
        KMeans().setFeaturesCol("scaledfeatures").setPredictionCol("predictions").setK(cluster)
        kmeanModel = kmeans.fit(trainset)
        testResult = kmeanModel.transform(testset)
        print("wssse，when cluster=", cluster, kmeanModel.computeCost(testResult)) #(wssse, 2.5189564869286865)
        print(kmeanModel.clusterCenters())