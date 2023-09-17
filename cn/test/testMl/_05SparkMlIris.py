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
        .load("./taix.txt")
    dataset.show()
    #为读取的数据创建schema
    taxiSchema = StructType(
        [StructField("id", IntegerType(), True),
        StructField("tid", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("time", StringType(), True)]
    )
    path = "./taix.txt"
    data = spark.read.schema(taxiSchema).csv(path)
    data.show()
    # +---+---------+----------+------+
    # | id | tid | lat | time |
    # +---+---------+----------+------+
    # | 111 | 30.655325 | 104.072573 | 173749 |
    # | 111 | 30.655346 | 104.072363 | 173828 |
    # | 111 | 30.655377 | 104.120252 | 124057 |
    # | 111 | 30.655439 | 104.088812 | 142016 |
    # +---+---------+----------+------+
    data.printSchema()
    # root
    # | -- id: integer(nullable=true)
    # | -- tid: double(nullable=true)
    # | -- lat: double(nullable=true)
    # | -- time: string(nullable=true)
    assembler = VectorAssembler(
        inputCols=["tid","lat"],
        outputCol="features")
    data = assembler.transform(data)
    #按照8：2的比例随即分割数据，分别用于训练和测试
    splitRait = [0.8, 0.2]
    train, test = data.randomSplit(splitRait)
    # 2-Trains a k-means model.
    kmeans = KMeans().setK(2).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction")
    model = kmeans.fit(train)

    # 3-Make predictions,由于数据集有限可以取全部数据预测
    # predictions = model.transform(test)
    predictions = model.transform(data)

    # 4-Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # 5-Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    time_prediction=predictions.select(substring("time", 0, 2).alias("hour"), "prediction")
    time_prediction.show()
    result=time_prediction\
    .groupBy(["hour", "prediction"])\
    .agg({"prediction": "count"}) \
    .orderBy("count(prediction)")\
    .filter("hour == 17")\
    .take(10)
    print(result)#[Row(hour='17', prediction=1, count(prediction)=2)]