# !/user/bin/env python
# @desc:RFM
__coding__ = 'utf-8'
__author__ = 'itcast team'

from cn.itcast.tags.base.BaseModelES import BaseModelAbstract

"""
计算流程:
1-调用基类完成RFM计算
2-设定评分规则,避免量纲影响计算
3-调用VectorAssemler类构造向量列,调用transform形成带向量列的DF
4-调用Kmeans算法,创建kmeans对象,并使用fit训练模型model
5-通过模型model测试vecDF,形成predict预测列
6-调用clusterCenters方法,获取聚类中心
7-导入包numpy调用sum方法完成RFM点的聚合,并加入下标构造tagsId和pid的DF
8-转换字典,通过udf函数获取tagsid
"""
from pyspark import RDD, SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import numpy

spark: SparkSession = SparkSession \
    .builder \
    .appName("testAgeModel") \
    .master("local[*]") \
    .getOrCreate()
sc: SparkContext = spark.sparkContext
sc.setLogLevel("WARN")


class PSMModel(BaseModelAbstract):
    def getTagId(self):
        return 37

    def compute(self,esDF:DataFrame,fiveDF:DataFrame):
        # esDF.show()
        # R:finishtime
        # F:ordersn订单号
        # M:orderamount订单总金额
        # memberid-->userid
        """
        |finishtime|memberid|orderamount|             ordersn|
        +----------+--------+-----------+--------------------+
        |1563941154|13822773|        1.0|jd_15062716252125282|
        """
        # fiveDF.show()
        """
        |rule| id|
        +----+---+
        |   1| 38|
        """
        # 分别计算RFM的值  都以memberid进行分组聚合计算
        # todo: 1-聚合统计
        days = F.datediff(F.date_sub(F.current_date(),1000),F.from_unixtime(F.max(esDF['finishtime']))).alias('days')
        order_cnt = F.count(esDF['ordersn']).alias('order_cnt')
        order_sum = F.round(F.sum(esDF['orderamount']),2).alias('order_sum')
        esDF01: DataFrame = esDF.groupby(esDF['memberid'].alias('userid'))\
                                .agg(days,order_cnt,order_sum)
        # esDF01.show()
        """
        |   userid|days|order_cnt|order_sum|
        +---------+----+---------+---------+
        | 13822773|  49|      181|497099.49|
        """
        # todo: 2-设定评分标准
        # R:0-7天,5  8-15,4   16-30,3   31-60,2   61-100,1  其他,0
        # F:>100,5  80-99,4   60-79,3   40-59,2  0-39,1  其他,0
        # M:>200000,5  100000-200000,4  50000-99999,3  20000-49999,2   1000-19999,1  其他,0
        rencency = F.when(esDF01['days'].between(0,7),5)\
            .when(esDF01['days'].between(8,15),4)\
            .when(esDF01['days'].between(16,30),3)\
            .when(esDF01['days'].between(31,60),2)\
            .when(esDF01['days'].between(61,100),1)\
            .otherwise(0)\
            .alias('rencency')
        frequency = F.when(esDF01['order_cnt']>100,5) \
            .when(esDF01['order_cnt'].between(80,99), 4) \
            .when(esDF01['order_cnt'].between(60,79), 3) \
            .when(esDF01['order_cnt'].between(40,59), 2) \
            .when(esDF01['order_cnt'].between(0,39), 1) \
            .otherwise(0)\
            .alias('frequency')
        moneny = F.when(esDF01['order_sum']>200000,5) \
            .when(esDF01['order_sum'].between(100000,200000), 4) \
            .when(esDF01['order_sum'].between(50000,99999), 3) \
            .when(esDF01['order_sum'].between(20000,49999), 2) \
            .when(esDF01['order_sum'].between(1000,19999), 1) \
            .otherwise(0) \
            .alias('moneny')
        esDF02: DataFrame = esDF01.select(esDF01['userid'],rencency,frequency,moneny)
        # esDF02.show()
        """
        |   userid|rencency|frequency|moneny|
        +---------+--------+---------+------+
        | 13822773|       2|        5|     5|
        """
        # todo: 3-构造特征向量列
        vector = VectorAssembler(inputCols=(['rencency','frequency','moneny']),outputCol='features')
        vecDF: DataFrame = vector.transform(esDF02)
        # vecDF.show()
        """
        |   userid|rencency|frequency|moneny|     features|
        +---------+--------+---------+------+-------------+
        | 13822773|       2|        5|     5|[2.0,5.0,5.0]|
        """
        # todo: 4-创建kmeans对象,训练模型
        # 4.1:创建kmeans对象
        kmeans = KMeans()\
            .setK(7)\
            .setSeed(123) \
            .setFeaturesCol('features') \
            .setPredictionCol('predict') \
            .setInitMode("k-means||")
        # 4.2:算法对象调用fit训练模型
        model = kmeans.fit(vecDF)
        # 4.3:模型调用transform使用测试集校验准确率,此mlDF作为后续关联使用
        mlDF = model.transform(vecDF)
        # mlDF.show()
        """
        |   userid|rencency|frequency|moneny|     features|predict|
        +---------+--------+---------+------+-------------+-------+
        | 13822773|       2|        5|     5|[2.0,5.0,5.0]|      0|
        |  4035067|       2|        4|     4|[2.0,4.0,4.0]|      3|
        """
        # todo: 5-获取聚类中心
        # 5.1:获取聚类中心,存放于数组中
        center = model.clusterCenters()
        # [array([2.        , 4.99865229, 5.        ]),
        # array([0.14285714, 1.        , 0.14285714]),
        # array([2. , 1. , 2.5]),
        # array([2.        , 4.96634615, 4.        ]),
        # array([1.66666667, 0.33333333, 4.33333333]),
        # array([0., 5., 0.]),
        # array([1.8, 1. , 0.9])]
        # 5.2:引用numpy库中sum求和,将RFM三点求和
        # [11.99865229110512, 1.2857142857142856, 5.5, 10.966346153846153, 6.333333333333333, 5.0, 3.6999999999999997]
        center_sum = [float(numpy.sum(x)) for x in center]
        # print(center_sum)
        # 5.3:构造聚类中心字典
        center_dict = {}
        cm: int = len(center_sum)
        # range(i, j) produces i, i+1, i+2, ..., j-1.
        for i in range(cm):
            center_dict[i] = center_sum[i]
        # print(center_dict)
        # {0: 11.99865229110512, 1: 1.2857142857142856, 2: 5.5, 3: 10.966346153846153, 4: 6.333333333333333, 5: 5.0, 6: 3.6999999999999997}
        # 5.4:将聚类中心字典构造DF
        c_list = [x for x in center_dict.items()]
        # print(c_list)
        centerDF = spark.createDataFrame(c_list,['predictid','center'])
        # centerDF.show()
        """
        |predictid|            center|
        +---+------------------+
        |  1| 11.99865229110512|
        |  2|1.2857142857142856|
        |  3|               5.5|
        |  4|10.966346153846153|
        |  5| 6.333333333333333|
        |  6|               5.0|
        |  7|3.6999999999999997|
        """
        # 5.5:将centerDF进行降序排序,值越大,表明消费能力越强
        centerDF01: DataFrame = centerDF.orderBy(centerDF['center'].desc())
        # centerDF01.show()
        """
        |  1| 11.99865229110512|
        |  4|10.966346153846153|
        |  5| 6.333333333333333|
        |  3|               5.5|
        |  6|               5.0|
        |  7|3.6999999999999997|
        |  2|1.2857142857142856|
        """
        # 5.6将五级标签数据fiveDF与centerDF01进行拉链组合,进而可以将cid与tagsId可以对应
        # 使用zip函数注意点:需要先使用union函数合并空rdd,同时在zip前使用map
        # zip:两者必须都为一个分区的rdd
        fiveRDD: RDD = fiveDF.rdd.union(spark.sparkContext.parallelize([]))
        fiveRDD01: RDD = fiveRDD.coalesce(1).map(lambda x:x).zip(centerDF01.rdd.coalesce(1))
        # for i in fiveRDD01.collect():
        #     print(i)
        # [(Row(rule='1', id=38), Row(predictid=1, center=11.99865229110512)),
        # (Row(rule='2', id=39), Row(predictid=4, center=10.966346153846153)),
        # (Row(rule='3', id=40), Row(predictid=5, center=6.333333333333333)),
        # (Row(rule='4', id=41), Row(predictid=3, center=5.5)),
        # (Row(rule='5', id=42), Row(predictid=6, center=5.0)),
        # (Row(rule='6', id=43), Row(predictid=7, center=3.6999999999999997)),
        # (Row(rule='7', id=44), Row(predictid=2, center=1.2857142857142856))]
        # 5.7构造只有tagsId和cid的DF,并将其转换为字典
        fiveDF01: DataFrame = fiveRDD01.map(lambda x:(x[1][0],x[0][1])).toDF(['pid','tagsid'])
        # fiveDF01.show()
        """
        |pid|tagsid|
        +---+------+
        |  1|    38|
        |  4|    39|
        |  5|    40|
        |  3|    41|
        |  6|    42|
        |  7|    43|
        |  2|    44|
        """
        # 5.8:将fiveDF01转换为字典,通过字典得到tagsid
        fivedict: dict = fiveDF01.rdd.collectAsMap()
        # todo: 关联匹配,定义udf函数给每个用户userid打上标签tagsid
        def gettagsid(predict):
            tagsid = fivedict.get(int(predict),'')
            return tagsid
        newDF: DataFrame = mlDF.select(mlDF['userid'],
                           udf(gettagsid,StringType())(mlDF['predict']).alias('tagsid'))
        newDF.show()
        """
        |   userid|tagsid|
        +---------+------+
        | 13822773|    38|
        |  4035067|    39|
        """
        # return newDF


if __name__ == '__main__':
    purchase_rfm = PSMModel()
    purchase_rfm.execute()



