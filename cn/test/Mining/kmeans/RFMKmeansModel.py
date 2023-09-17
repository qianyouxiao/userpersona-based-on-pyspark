#!/usr/bin/env python
# @desc : 挖掘标签：
#       1.使用KMeans算法根据用户的RFM的值计算客户价值模型
#       2.使用KMeans算法根据用户的RFM的值计算客户价值模型+实现模型的保存和加载
#       3.使用KMeans算法根据用户的RFM的值计算客户价值模型+实现模型的保存和加载+K值选取
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, functions, SparkSession, Column
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from cn.itcast.tags.base.BaseModelES import BaseModelES
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

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("UserProfileModel") \
    .getOrCreate()


class RFMKmeansModel(BaseModelES):

    def getTagId(self):
        return 37

    def compute(self, esDF: DataFrame, fiveDS: DataFrame):
        print("Execute the compute method of the subclass!")
        esDF = esDF.where(esDF.memberid.isNotNull())
        esDF.show()
        # +----------+--------+-----------+--------------------+
        # | finishtime | memberid | orderamount | ordersn |
        # +----------+--------+-----------+--------------------+
        # | 1563941154 | 13822773 | 1.0 | jd_15062716252125282 |
        # | 1564117286 | 13822773 | 3699.0 | jd_15062720080896457 |
        # | 1563746025 | 4035291 | 2699.0 | jd_15062817103347299 |
        fiveDS.show()
        # +---+----+
        # | id | rule |
        # +---+----+
        # | 38 | 1 |
        # | 39 | 2 |
        # | 40 | 3 |
        # | 41 | 4 |
        # | 42 | 5 |
        # | 43 | 6 |
        # | 44 | 7 |
        # +---+----+
        # 定义常量字符串, 避免后续拼写错误
        recencyStr = "recency"
        frequencyStr = "frequency"
        monetaryStr = "monetary"
        featureStr = "feature"
        predictStr = "predict"
        # 1. 按用户id进行聚合获取客户RFM
        # 客户价值模型 - RFM:
        # Rencency: 最近一次消费, 最后一次订单距今时间
        # Frequency: 消费频率, 订单总数量
        # Monetary: 消费金额, 订单总金额
        # https: // blog.csdn.net / liam08 / article / details / 79663018
        recencyAggColumn = functions.datediff(date_sub(current_timestamp(), 1205), from_unixtime(max("finishtime"))).alias(recencyStr)
        frequencyAggColumn = functions.count("ordersn").alias(frequencyStr)
        monetaryAggColumn = functions.sum("orderamount").alias(monetaryStr)

        RFMResult = esDF.groupBy("memberid").agg(recencyAggColumn, frequencyAggColumn, monetaryAggColumn)
        RFMResult.show()
        RFMResult.printSchema()
        # root
        # | -- memberid: long(nullable=true)
        # | -- recency: integer(nullable=true)
        # | -- frequency: long(nullable=false)
        # | -- monetary: double(nullable=true)
        # 2.为RFM打分
        # R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
        # F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
        # M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
        recencyScore: Column = functions.when(
            (RFMResult[recencyStr] >= 1) & (RFMResult[recencyStr]  <= 3), 5) \
            .when((RFMResult[recencyStr]  >= 4) & (RFMResult[recencyStr]  <= 6), 4) \
            .when((RFMResult[recencyStr]  >= 7) & (RFMResult[recencyStr]   <= 9), 3) \
            .when((RFMResult[recencyStr]  >= 10) & (RFMResult[recencyStr]  <= 15), 2) \
            .when(RFMResult[recencyStr]  >= 16, 1) \
            .otherwise(0)\
            .alias(recencyStr)

        frequencyScore: Column = functions.when(RFMResult[frequencyStr] >= 200, 5) \
            .when((RFMResult[frequencyStr] >= 150) & (RFMResult[frequencyStr] <= 199), 4) \
            .when((RFMResult[frequencyStr] >= 100) & (RFMResult[frequencyStr] <= 149), 3) \
            .when((RFMResult[frequencyStr] >= 50) & (RFMResult[frequencyStr] <= 99), 2) \
            .when((RFMResult[frequencyStr] >= 1) & (RFMResult[frequencyStr] <= 49), 1) \
            .otherwise(0) \
            .alias(frequencyStr)

        monetaryScore: Column = functions.when(RFMResult[monetaryStr] >= 200000, 5) \
            .when(RFMResult[monetaryStr].between(100000, 199999), 4) \
            .when(RFMResult[monetaryStr].between(50000, 99999), 3) \
            .when(RFMResult[monetaryStr].between(10000, 49999), 2) \
            .when(RFMResult[monetaryStr] <= 9999, 1) \
            .otherwise(0) \
            .alias(monetaryStr)
        print("rfmscore")
        RFMScoreResult: DataFrame = RFMResult.select("memberid", recencyScore, frequencyScore, monetaryScore)
        RFMScoreResult.show(10)
        #3.聚类
        # 为方便后续模型进行特征输入，需要部分列的数据转换为特征向量，并统一命名，VectorAssembler类就可以完成这一任务。
        # VectorAssembler是一个transformer，将多列数据转化为单列的向量列
        vectorDF: DataFrame = VectorAssembler()\
        .setInputCols([recencyStr, frequencyStr, monetaryStr])\
        .setOutputCol(featureStr)\
        .transform(RFMScoreResult)
        vectorDF.show(10)
        # +--------+-------+---------+--------+-------------+
        # | memberid | recency | frequency | monetary | feature |
        # +--------+-------+---------+--------+-------------+
        # | 26 | 0 | 3 | 5 | [0.0, 3.0, 5.0] |
        # | 29 | 0 | 3 | 5 | [0.0, 3.0, 5.0] |
        kMeans: KMeans = KMeans() \
        .setK(3) \
        .setSeed(10) \
        .setMaxIter(2) \
        .setFeaturesCol(featureStr) \
        .setPredictionCol(predictStr)

        #4.训练模型
        model: KMeansModel = kMeans.fit(vectorDF)
        # model.save("/model/RFMModel/")
        # model = KMeansModel.load("/model/RFMModel/")

        # #5.预测
        result: DataFrame = model.transform(vectorDF)
        result.show(10)
        # | memberid | recency | frequency | monetary | feature | predict |
        # +--------+-------+---------+--------+-------------+-------+
        # | 26 | 0 | 3 | 5 | [0.0, 3.0, 5.0] | 0 |
        # | 29 | 0 | 3 | 5 | [0.0, 3.0, 5.0] | 0 |
        # | 4033555 | 0 | 3 | 5 | [0.0, 3.0, 5.0] | 0 |
        #6.测试时看下聚类效果
        ds = result \
        .groupBy(predictStr) \
        .agg(max(result[recencyStr] + result[frequencyStr] + result[monetaryStr]),
             min(result[recencyStr] + result[frequencyStr] + result[monetaryStr]))\
        .sort(result[predictStr], ascending=True)
        ds.show()
        # +-------+---------------------------------------+---------------------------------------+
        # | predict | max(((recency + frequency) + monetary)) | min(((recency + frequency) + monetary)) |
        # +-------+---------------------------------------+---------------------------------------+
        # | 0 | 10 | 6 |
        # | 1 | 7 | 6 |
        # | 2 | 5 | 0 |
        # +-------+---------------------------------------+---------------------------------------+
        print(model.clusterCenters())
        # [array([0., 3.03230148, 5.]),
        # array([0., 2.96666667, 4.]),
        # array([0.04545455, 1.13636364, 1.27272727])]
        # numpy.ndarray
        import numpy as np
        print([np.sum(c) for c in model.clusterCenters()])#[8.032301480484522, 6.966666666666667, 2.4545454545454546]

        center= {}
        for i in range(len(model.clusterCenters())):
            center[i]=[float(np.sum(c)) for c in model.clusterCenters()][i]
        print(center)
        # {0: 8.032301480484522, 1: 6.966666666666667, 2: 2.4545454545454546}
        # 要的格式如下：
        # (聚类索引/编号, 聚类中心的RFM的和)
        # (0 8.032301480484522)
        # (1 6.966666666666667)
        # (2 2.4545454545454546)
        #问题: 每一个簇的ID是无序的, 但是我们将分类簇和rule进行对应的时候, 需要有序
        #7.按质心排序, 质心大, 该类用户价值大
        #[(质心id, 质心值)]
        print("center--------------------------")
        #Python写法
        #sortedIndexAndRFM=sorted(center.items(),key=lambda x:x[1],reverse=True)
        #[(1, 3.892179195140471), (0, 3.0038167938931295), (2, 2.0)]
        '''
        dictlist=[]
        for key, value in center.items():
            temp = [key, value]
            dictlist.append(temp)
        '''
        dictlist=[(k, v) for k, v in center.items()]
        indexAndRFM=spark.createDataFrame(dictlist, ['predict', 'rfm_center'])
        # indexAndRFM.show()
        sortedIndexAndRFM=indexAndRFM.rdd.sortBy(lambda x:x[1], ascending=False)
        # print(sortedIndexAndRFM.collect())
        #[Row(predict=1, rfm_center=3.892179195140471), Row(predict=0, rfm_center=3.0038167938931295), Row(predict=2, rfm_center=2.0)]
        #7.将上面的排好序的聚类编号和聚类中心与5级规则分别转化为DataSet进行对应，在转化为RDD的repartition进行用zip拉链操作，最后进行选择predict | tagIds
        '''
        5级规则fiveDF:
            +---+----+
            | id|rule|
            +---+----+
            |103|   1|
            |104|   2|
            |105|   3|
            +---+----+
            目标:
            +-------+---
            |predict | id |
            +------+----
            | 4 | 103 |
            | 0 | 104 |
            | 1 | 105|
            | 6 | 103 |
            +--------+--
        '''
        # print(fiveDS.rdd.collect())

        sortedIndexAndRFMRDD = sortedIndexAndRFM.union(spark.sparkContext.parallelize([]))
        tempRDD=sortedIndexAndRFMRDD.repartition(1).map(lambda x:x).zip((fiveDS.rdd.repartition(1)))
        # print(tempRDD.collect())
        print(tempRDD.count())
        # https://developer.aliyun.com/article/396655 zip在pysaprk中需要借助于map和union一起使用
        # [(Row(predict=1, rfm_center=3.892179195140471), Row(id=103, rule='1')),
        #  (Row(predict=0, rfm_center=3.0038167938931295), Row(id=104, rule='2')),
        #  (Row(predict=2, rfm_center=2.0), Row(id=105, rule='3'))]
        print("===========================选择tempRDD的预测值和标签Id======================================")
        # 选择tempRDD的预测值和标签Id
        # print(type(tempRDD))#<class 'pyspark.rdd.RDD'>
        # print(tempRDD.map(lambda t: (t[0][0],t[1][0])).collect())
        # #[(1, 103), (0, 104), (2, 105)]
        ruleDF: DataFrame = tempRDD.map(lambda t: (t[0][0],t[1][0])).toDF(["predict", "tagIds"])
        ruleDF.show()
        # +-------+------+
        # | predict | tagIds |
        # +-------+------+
        # | 1 | 103 |
        # | 0 | 104 |
        # | 2 | 105 |
        # +-------+------+
        #8 将ruleDF首先转化为DataSet转化为map类型，使用rdd的collectAsMap操作，在使用udf转换，比如预测为predict = 4 代表的是38号标签
        ruleMap = ruleDF.rdd.collectAsMap()
        # print(ruleMap)#{1: 103, 0: 104, 2: 105}
        # print(ruleMap[1])#这里是字典类型，key=1，得到tagsId=103
        predict2Tag = udf(lambda predict:ruleMap[predict],IntegerType())
        # result.select(predict2Tag("predict")).show()
        result.printSchema()
        # root
        # | -- memberid: string(nullable=true)
        # | -- recency: integer(nullable=false)
        # | -- frequency: integer(nullable=false)
        # | -- monetary: integer(nullable=false)
        # | -- feature: vector(nullable=true)
        # | -- predict: integer(nullable=false)
        # 使用经过Kmeans聚类的result结果选择memberId,'predict并作为一个newDF的datafrmae写入hbase
        newDF: DataFrame = result.select(result["memberid"].alias("userId"), predict2Tag("predict").alias("tagsId"))
        # +--------+-------+---------+--------+-------------+-------+
        # | memberid | recency | frequency | monetary | feature | predict |
        # +--------+-------+---------+--------+-------------+-------+
        # | 446 - 51 | 1 | 1 | 2 | [1.0, 1.0, 2.0] | 1 |
        newDF.show()
        return newDF


if __name__ == '__main__':
    rfmKmeansModel = RFMKmeansModel()
    rfmKmeansModel.execute()
