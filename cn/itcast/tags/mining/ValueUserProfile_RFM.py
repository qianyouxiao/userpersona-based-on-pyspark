#!/usr/bin/env python
# @desc : 实现客户价值挖掘标签开发
__coding__ = "utf-8"
__author__ = "itcast team"

import os

import numpy as np
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import DataFrame, SparkSession
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


class Valuemodel(BaseModelAbstract):
    def getTagId(self):
        return 37

    def compute(self, esdf: DataFrame, fivedf: DataFrame):
        # esdf.show()
        # +----------+---------+-----------+--------------------+
        # | finishtime | memberid | orderamount | ordersn |
        # +----------+---------+-----------+--------------------+
        # | 1564669123 | 4035099 | 1139.0 | amazon_7965239286... |
        # | 1565406885 | 13823237 | 1669.0 | gome_795747706954617 |
        # | 1563555590 | 138230929 | 1099.0 | suning_7965732029... |
        # | 1564130740 | 13823365 | 1999.0 | amazon_7957041033... |
        # | 1563764893 | 13823017 | 1.0 | jd_14090322531369629 |
        # | 1565352355 | 4034959 | 1099.0 | jd_14090322532451531 |
        # | 1564320586 | 4033354 | 1388.0 | gome_795751300602358 |
        # fivedf.show()
        # +---+----+
        # | id|rule|
        # +---+----+
        # | 38|   1| 超高价值
        # | 39|   2|
        # | 40|   3|
        # | 41|   4|
        # | 42|   5|
        # | 43|   6|
        # | 44|   7| 超低价值1
        #  todo 5根据会员id分组,获取每个会员最新的购买时间距离当前时间的天数,订单总金额,以及订单量
        #  mysql查表：SELECT FROM_UNIXTIME(finishtime, '%Y%m%d' ) FROM `tags_dat`.`tbl_orders`; 20200703到20200801，距离20220804相差733天
        finishtimecolum = F.datediff(F.date_sub(F.current_date(), 680),
                                     F.from_unixtime(F.max(esdf['finishtime']))).alias('days')
        orderamountcolum = F.sum(esdf['orderamount']).alias('sumamount')
        ordersncolum = F.count(esdf['ordersn']).alias('cntsn')
        esdf1: DataFrame = esdf.groupby(esdf['memberid']).agg(finishtimecolum, orderamountcolum, ordersncolum)
        # esdf1.show()
        # +--------+----+------------------+-----+
        # | memberid | days | sumamount | cntsn |
        # +--------+----+------------------+-----+
        # | 13823489 | 49 | 202613.69989013672 | 116 |
        # | 26 | 49 | 212949.05004882812 | 111 |
        # | 4035177 | 49 | 217628.1700036619 | 125 |
        # | 29 | 49 | 249794.74002075195 | 137 |
        # todo 5-1对上述三列值打分(业务定打分规则),进行归一化操作(0-5)
        finishtimescore = F.when((esdf1['days'] < 20), 5) \
            .when((esdf1['days'] >= 20) & (esdf1['days'] < 40), 4) \
            .when((esdf1['days'] >= 40) & (esdf1['days'] < 60), 3) \
            .when((esdf1['days'] >= 60) & (esdf1['days'] < 80), 2) \
            .when((esdf1['days'] >= 80), 1) \
            .otherwise(0) \
            .alias('days')
        orderamountscore = F.when((esdf1['sumamount'] >= 850000), 5) \
            .when((esdf1['sumamount'] >= 500000) & (esdf1['sumamount'] < 850000), 4) \
            .when((esdf1['sumamount'] >= 350000) & (esdf1['sumamount'] < 500000), 3) \
            .when((esdf1['sumamount'] >= 100000) & (esdf1['sumamount'] < 350000), 2) \
            .when((esdf1['sumamount'] < 100000), 1) \
            .otherwise(0) \
            .alias('sumamount')
        ordersnscore = F.when((esdf1['cntsn'] >= 440), 5) \
            .when((esdf1['cntsn'] >= 330) & (esdf1['cntsn'] < 440), 4) \
            .when((esdf1['cntsn'] >= 220) & (esdf1['cntsn'] < 330), 3) \
            .when((esdf1['cntsn'] >= 110) & (esdf1['cntsn'] < 220), 2) \
            .when((esdf1['cntsn'] < 110), 1) \
            .otherwise(0) \
            .alias('cntsn')
        esdfuni = esdf1.select(esdf['memberid'], finishtimescore, orderamountscore, ordersnscore)
        # esdfuni.show()
        # +--------+----+---------+-----+
        # | memberid | days | sumamount | cntsn |
        # +--------+----+---------+-----+
        # | 13823489 | 3 | 4 | 2 |
        # | 26 | 3 | 4 | 2 |
        # | 4035177 | 3 | 4 | 3 |
        # todo 5-2组合成向量列vectorAssemble -->feature
        vector = VectorAssembler().setInputCols(['days', 'sumamount', 'cntsn']).setOutputCol('feature')
        vecesdf = vector.transform(esdfuni)
        # vecesdf.show()
        # | memberid | days | sumamount | cntsn | feature |
        # +--------+----+---------+-----+-------------+
        # | 13823489 | 3 | 4 | 2 | [3.0, 4.0, 2.0] |
        # | 26 | 3 | 4 | 2 | [3.0, 4.0, 2.0] |
        # | 4035177 | 3 | 4 | 3 | [3.0, 4.0, 3.0] |
        # todo 5-3使用Kmeans算法对df进行训练
        kMeans: KMeans = KMeans() \
            .setK(7) \
            .setSeed(10) \
            .setMaxIter(2) \
            .setFeaturesCol('feature') \
            .setPredictionCol('predictstr')
        model: KMeansModel = kMeans.fit(vecesdf)
        # 根据模型预测结果
        resultdf: DataFrame = model.transform(vecesdf)
        # resultdf.show()
        # +--------+----+---------+-----+-------------+----------+
        # | memberid | days | sumamount | cntsn | feature | predictstr |
        # +--------+----+---------+-----+-------------+----------+
        # | 13823489 | 3 | 4 | 2 | [3.0, 4.0, 2.0] | 2 |
        # | 26 | 3 | 4 | 2 | [3.0, 4.0, 2.0] | 2 |
        # | 4035177 | 3 | 4 | 3 | [3.0, 4.0, 3.0] | 5 |
        # todo 5-4获取聚类中心,,取和后,加索引,排序,然后predictstr列跟fivedf合并
        center = model.clusterCenters()
        # print(center)
        # [array([3., 4.46686747, 4.33433735]), array([3., 5., 2.70192308]), array([2.99145299, 4.07692308, 1.8034188]),array([2.91044776, 2.43283582, 1.]), array([3., 3., 3.1875]), array([3., 4., 3.]), array([3., 3., 2.])]
        # todo 5-5 求和
        list1 = [float(np.sum(x)) for x in center]
        # print(list1)
        # [8.088397790055248, 9.0, 12.958333333333332, 7.0, 11.906976744186046, 10.046511627906977, 8.0]
        # todo 5-6 加索引,形成字典
        dict1 = {}
        for i in range(len(list1)):
            dict1[i] = list1[i]
        # print(dict1)
        # todo 5-7 转为列表,==>df==>rdd排序==?合并
        list2 = [[k, v] for (k, v) in dict1.items()]
        # print(list2)
        #[[0, 8.088397790055248], [1, 9.0], [2, 12.958333333333332], [3, 7.0], [4, 11.906976744186046], [5, 10.046511627906977], [6, 8.0]]
        centerdf: DataFrame = spark.createDataFrame(list2, ['predict', 'center'])
        # centerdf.show()
        # +-------+------------------+
        # | predict | center |
        # +-------+------------------+
        # | 0 | 8.088397790055248 |
        # | 1 | 9.0 |
        # | 2 | 12.958333333333332 |
        # | 3 | 7.0 |
        # | 4 | 11.906976744186046 |
        # | 5 | 10.046511627906977 |
        # | 6 | 8.0 |
        # +-------+------------------+
        # 转化为如下结构，方便打标签
        # | predict | center |
        # +-------+------------------+
        # | 0 | 11.801204819277109 |
        # | 1 | 10.701923076923077 |
        # | 5 | 10.0 |
        # | 4 | 9.1875 |
        # | 2 | 8.871794871794874 |
        # | 6 | 8.0 |
        # | 3 | 6.343283582089552 |
        centersortrdd = centerdf.rdd.repartition(1).sortBy(lambda x: x[1], ascending=False)
        print("sort partition")
        centersortrdd.foreach(lambda x:print(x))
        # Row(predict=4, center=11.906976744186046)
        # Row(predict=5, center=10.046511627906977)
        # Row(predict=1, center=9.0)
        # Row(predict=0, center=8.088397790055248)
        # Row(predict=6, center=8.0)
        # Row(predict=3, center=7.0)
        # # todo 5-7 合并fivedf,,先union一个空rdd,再降分区,map,最后zip
        temprdd = centersortrdd.union(sc.parallelize([]))
        unionrdd = temprdd.repartition(1).map(lambda x: x).zip(fivedf.rdd.repartition(1))
        unionrdd.foreach(lambda x:print(x))
        # (Row(predict=2, center=12.958333333333332), Row(id=38, rule='1'))
        # (Row(predict=4, center=11.906976744186046), Row(id=39, rule='2'))
        # (Row(predict=5, center=10.046511627906977), Row(id=40, rule='3'))
        # (Row(predict=1, center=9.0), Row(id=41, rule='4'))
        # (Row(predict=0, center=8.088397790055248), Row(id=42, rule='5'))
        # (Row(predict=6, center=8.0), Row(id=43, rule='6'))
        # (Row(predict=3, center=7.0), Row(id=44, rule='7'))
        # # todo 5-8 合并后的rdd,取predict列和fivedf的id列,形成字典
        fivedict = unionrdd.map(lambda row: (row[0][0], row[1][0])).collectAsMap()
        print(fivedict)
        # {2: 38, 4: 39, 5: 40, 1: 41, 0: 42, 6: 43, 3: 44}
        # # todo 5-9 根据predict列和id列的对应关系,,匹配resultdf和fivedf
        newdf: DataFrame = resultdf.select(resultdf['memberid'].alias('userId'),
                                           udf(lambda x: fivedict[x], returnType=StringType())(resultdf['predictstr']).alias('tagsId'))
        newdf.show()
        # # +--------+------+
        # # | memberid | tagsId |
        # # +--------+------+
        # # | 13823489 | 42 |
        # # | 26 | 42 |
        # # | 4035177 | 40 |
        # # | 29 | 38 |
        # # | 4033555 | 38 |
        # # | 4034769 | 40 |
        # # | 4034679 | 40 |
        # # | 4034681 | 41 |
        # # | 4034239 | 39 |
        # # | 4035069 | 43 |
        # # | 13822793 | 38 |
        # # | 13823131 | 40 |
        return newdf
        # # rsdf
        # # +------+----------+
        # # | userId | tagsId |
        # # +------+----------+
        # # | 26 | 12, 20, 42, 6 |
        # # | 29 | 17, 38, 6, 11 |
        # # | 65 | 17, 8, 5, 40 |
        # # | 19 | 11, 42, 19, 6 |
        # # | 54 | 12, 5, 17, 38 |
        # # | 22 | 20, 39, 6, 13 |
        # # | 7 | 12, 17, 38, 6 |
        # # | 77 | 43, 8, 17, 6 |


if __name__ == '__main__':
    valuemodel = Valuemodel()
    valuemodel.execute()
