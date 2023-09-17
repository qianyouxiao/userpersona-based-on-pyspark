#!/usr/bin/env python
# @desc : 挖掘标签：使用KMeans算法+RFE打分计算用户活跃度模型
__coding__ = "utf-8"
__author__ = "itcast team"

import os

import numpy as np
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StringType

from cn.itcast.tags.base.BaseModelES import BaseModelAbstract

SPARK_HOME = 'D:\\ProgramCj\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'D:\\ProgramCJ\\Python\\python'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
# TODO 0.准备Spark开发环境(重复)
spark = SparkSession \
    .builder \
    .appName("TfecProfile") \
    .master("local[*]") \
    .getOrCreate()
sc: SparkContext = spark.sparkContext

# 构建Spark的Windows环境
SPARK_HOME = 'D:\\ProgramCj\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'D:\\ProgramCj\\Python\\python'
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON


class RFEmodel(BaseModelAbstract):
    def getTagId(self):
        return 45

    def compute(self, esdf: DataFrame, fivedf: DataFrame):
        esdf.show(truncate=False)
        # +--------------+-------------------------------------------------------------------------------+-------------------+
        # |global_user_id|loc_url                                                                        |log_time           |
        # +--------------+-------------------------------------------------------------------------------+-------------------+
        # |184           |http://www.eshop.com/product/11335.html?ebi=ref-se-1-1                         |2019-07-20 09:47:47|
        # |301           |http://www.eshop.com/                                                          |2019-07-30 01:11:44|
        # |151           |http://www.eshop.com/l/2729-0-0-0-297-0-0-0-0-0-0-0.html                       |2019-07-23 11:55:13|
        fivedf.show()
        # +---+----+ hive
        # | id | rule |
        # +---+----+
        # | 46 | 1 |
        # | 47 | 2 |
        # | 48 | 3 |
        # | 49 | 4 |
        # +---+----+
        # 0.定义常量字符串,避免后续拼写错误
        recencyStr = "recency"
        frequencyStr = "frequency"
        engagementsStr = "engagements"
        featureStr = "feature"
        scaleFeatureStr = "scaleFeature"
        predictStr = "predict"
        # Recency:最近一次访问时间,用户最后一次访问距今时间
        # Frequency:访问频率,用户一段时间内访问的页面总次数,
        # Engagements:页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览量、下载量、 视频播放数量等

        recencyAggColumn: Column = F.datediff(F.date_sub(F.current_timestamp(), 1080), F.max("log_time")).alias(
            recencyStr)
        frequencyAggColumn: Column = F.count("loc_url").alias(frequencyStr)
        engagementsAggColumn: Column = F.countDistinct("loc_url").alias(engagementsStr)
        # 1.根据用户id进行分组,求出RFE
        tempDF: DataFrame = esdf.groupBy("global_user_id") \
            .agg(recencyAggColumn, frequencyAggColumn, engagementsAggColumn)
        tempDF.show(10, truncate=False)
        # +--------------+-------+---------+-----------+
        # | global_user_id | recency | frequency | engagements |
        # +--------------+-------+---------+-----------+
        # | 107 | 3 | 636 | 280 |
        # | 110 | 3 | 514 | 249 |
        # | 12 | 3 | 537 | 266 |
        # | 137 | 3 | 659 | 290 |
        recencyScore: Column = F.when(F.col(recencyStr).between(0, 15), 5) \
            .when(F.col(recencyStr).between(16, 30), 4) \
            .when(F.col(recencyStr).between(31, 45), 3) \
            .when(F.col(recencyStr).between(46, 60), 2) \
            .when(F.col(recencyStr) > 60, 1) \
            .alias(recencyStr)

        frequencyScore: Column = F.when(F.col(frequencyStr) > 560, 5) \
            .when(F.col(frequencyStr).between(560, 760), 4) \
            .when(F.col(frequencyStr).between(400, 559), 3) \
            .when(F.col(frequencyStr).between(250, 499), 2) \
            .when(F.col(frequencyStr) < 250, 1) \
            .alias(frequencyStr)

        engagementsScore: Column = F.when(F.col(engagementsStr) > 300, 5) \
            .when(F.col(engagementsStr).between(250, 300), 4) \
            .when(F.col(engagementsStr).between(200, 250), 3) \
            .when(F.col(engagementsStr).between(50, 200), 2) \
            .when(F.col(engagementsStr)< 49, 1) \
            .alias(engagementsStr)
        FREScoreDF: DataFrame = tempDF \
            .select(tempDF["global_user_id"].alias("userId"), recencyScore, frequencyScore, engagementsScore)\
            .where(f"userId is not null and {recencyStr} is  not null and {frequencyStr} is  not null and {engagementsStr} is  not null")
        FREScoreDF.show(10, truncate=False)
        # +------+-------+---------+-----------+
        # | userId | recency | frequency | engagements |
        # +------+-------+---------+-----------+
        # | 107 | 5 | 5 | 4 |
        # | 110 | 5 | 3 | 3 |
        # | 12 | 5 | 3 | 4 |
        # | 137 | 5 | 5 | 4 |

        # todo 5-2组合成向量列vectorAssemble -->feature
        vector = VectorAssembler().setInputCols([recencyStr, frequencyStr,engagementsStr]).setOutputCol(featureStr)
        vectorDF = vector.transform(FREScoreDF)
        # todo 5-3使用Kmeans算法对df进行训练
        kMeans: KMeans = KMeans() \
            .setK(4) \
            .setSeed(10) \
            .setMaxIter(10) \
            .setFeaturesCol(featureStr) \
            .setPredictionCol(predictStr)
        model: KMeansModel = kMeans.fit(vectorDF)
        # 根据模型预测结果
        resultDF: DataFrame = model.transform(vectorDF)
        resultDF.show()
        # +------+-------+---------+-----------+-------------+-------+
        # | userId | recency | frequency | engagements | feature | predict |
        # +------+-------+---------+-----------+-------------+-------+
        # | 107 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 110 | 5 | 3 | 3 | [5.0, 3.0, 3.0] | 1 |
        # | 12 | 5 | 3 | 4 | [5.0, 3.0, 4.0] | 3 |
        # | 137 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 147 | 5 | 5 | 3 | [5.0, 5.0, 3.0] | 0 |
        # | 163 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 164 | 5 | 5 | 3 | [5.0, 5.0, 3.0] | 0 |
        # | 199 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 200 | 5 | 5 | 3 | [5.0, 5.0, 3.0] | 0 |
        # | 201 | 5 | 3 | 3 | [5.0, 3.0, 3.0] | 1 |
        # | 207 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 212 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 217 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 254 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 275 | 5 | 5 | 3 | [5.0, 5.0, 3.0] | 0 |
        # | 296 | 5 | 3 | 3 | [5.0, 3.0, 3.0] | 1 |
        # | 31 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # | 339 | 5 | 3 | 3 | [5.0, 3.0, 3.0] | 1 |
        # | 34 | 5 | 5 | 3 | [5.0, 5.0, 3.0] | 0 |
        # | 343 | 5 | 5 | 4 | [5.0, 5.0, 4.0] | 2 |
        # +------+-------+---------+-----------+-------------+-------+

        # todo 5-4获取聚类中心,,取和后,加索引,排序,然后predictstr列跟fivedf合并
        center = model.clusterCenters()
        # print("center",center)
        # center[array([5., 4.96815287, 3.]), array([5., 3., 3.]), array([5., 5., 4.00483871]), array([5., 3.11111111, 4.])]
        # todo 5-5 求和
        list1 = [float(np.sum(x)) for x in center]
        print("sum cluer:",list1)
        #sum cluer: [12.96815286624204, 11.0, 14.004838709677419, 12.11111111111111]
        # todo 5-6 加索引,形成字典
        dict1 = {}
        for i in range(len(list1)):
            dict1[i] = list1[i]
        print("from dict",dict1)
        #from dict {0: 12.96815286624204, 1: 11.0, 2: 14.004838709677419, 3: 12.11111111111111}
        # todo 5-7 转为列表,==>df==>rdd排序==?合并
        list2 = [[k, v] for (k, v) in dict1.items()]
        print(list2)
        #[[0, 12.96815286624204], [1, 11.0], [2, 14.004838709677419], [3, 12.11111111111111]]
        centerdf: DataFrame = spark.createDataFrame(list2, ['predict', 'center'])
        centerdf.show()
        # +-------+------------------+
        # | predict | center |
        # +-------+------------------+
        # | 0 | 12.96815286624204 |
        # | 1 | 11.0 |
        # | 2 | 14.004838709677419 |
        # | 3 | 12.11111111111111 |
        # +-------+------------------+
        # 转化为如下结构，方便打标签
        # | predict | center |
        # +-------+------------------+
        # | 0 | 11.801204819277109 |
        # | 1 | 10.701923076923077 |
        # | 5 | 10.0 |
        centersortrdd = centerdf.rdd.repartition(1).sortBy(lambda x: x[1], ascending=False)
        print("sort partition")
        centersortrdd.foreach(lambda x: print(x))
        # Row(predict=2, center=14.004838709677419)
        # Row(predict=0, center=12.96815286624204)
        # Row(predict=3, center=12.11111111111111)
        # Row(predict=1, center=11.0)
        # # todo 5-7 合并fivedf,,先union一个空rdd,再降分区,map,最后zip
        temprdd = centersortrdd.union(sc.parallelize([]))
        unionrdd = temprdd.repartition(1).map(lambda x: x).zip(fivedf.rdd.repartition(1))
        unionrdd.foreach(lambda x: print(x))
        # (Row(predict=2, center=14.004838709677419), Row(id=46, rule='1'))
        # (Row(predict=0, center=12.96815286624204), Row(id=47, rule='2'))
        # (Row(predict=3, center=12.11111111111111), Row(id=48, rule='3'))
        # (Row(predict=1, center=11.0), Row(id=49, rule='4'))
        # # todo 5-8 合并后的rdd,取predict列和fivedf的id列,形成字典
        fivedict = unionrdd.map(lambda row: (row[0][0], row[1][0])).collectAsMap()
        print(fivedict)
        # {2: 46, 0: 47, 3: 48, 1: 49}
        # # todo 5-9 根据predict列和id列的对应关系,,匹配resultdf和fivedf
        newDF: DataFrame = resultDF.select(resultDF['userId'],
                                           udf(lambda x: fivedict[x], returnType=StringType())(resultDF['predictstr']).alias('tagsId'))
        newDF.show()
        # return newDF

if __name__ == '__main__':
    rfemodel = RFEmodel()
    rfemodel.execute()
