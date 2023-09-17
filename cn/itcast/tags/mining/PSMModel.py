#!/usr/bin/env python
# @desc : 实现价格敏感度挖掘标签开发
__coding__ = "utf-8"
__author__ = "itcast team"

import os

from pyspark import SparkContext
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
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
sc:SparkContext=spark.sparkContext

class PriceSenmodel(BaseModelAbstract):
    def getTagId(self):
        return 50

    def compute(self, esdf:DataFrame, fivedf:DataFrame):
        esdf.show()
        # +---------------+---------+-----------+--------------------+
        # | couponcodevalue | memberid | orderamount | ordersn |
        # +---------------+---------+-----------+--------------------+
        # | 0.0 | 4035099 | 1139.0 | amazon_7965239286... |
        # | 0.0 | 13823237 | 1669.0 | gome_795747706954617 |
        # | 0.0 | 138230929 | 1099.0 | suning_7965732029... |
        # | 0.0 | 13823365 | 1999.0 | amazon_7957041033... |
        # | 0.0 | 13823017 | 1.0 | jd_14090322531369629 |
        # | 0.0 | 4034959 | 1099.0 | jd_14090322532451531 |
        # | 0.0 | 4033354 | 1388.0 | gome_795751300602358 |
        # | 0.0 | 4035281 | 1669.0 | gome_795333557364617 |
        # | 0.0 | 13823291 | 1.0 | jd_14090322540513737 |
        # | 0.0 | 4034699 | 1039.0 | amazon_7965190416... |
        fivedf.show()
        # | id | rule |
        # +---+----+
        # | 51 | 1 | 极度敏感
        # | 52 | 2 |
        # | 53 | 3 |
        # | 54 | 4 |
        # | 55 | 5 | 极度不敏感

        #影响价格敏感度的三个值:
        # 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比
        # *优惠订单占比 - -->    (优惠的次数 / 购买次数)
        # *平均优惠金额占比 - -->  (优惠金额平均数 / 平均每单应收金额)
        # *优惠金额平均数 = 优惠总金额 / 优惠的次数
        # *平均每单应收金额 = 订单应收总金额 / 购买次数
        # *优惠总金额占比 - -->   (优惠总金额 / 订单的应收总金额)
        #需要计算的值:优惠次数,,购买次数,,优惠金额,,订单应收金额,,

        #todo 1-处理esdf,标记优惠订单状态,,增加订单应收总金额列=couponcodevalue+orderamount
        #定义订单状态列
        statecolum=F.when(esdf['couponcodevalue'].cast('int')==0.0,0).\
                                  when(esdf['couponcodevalue'].cast('int')!=0.0,1).alias('state')
        orderamo=(esdf['couponcodevalue']+esdf['orderamount']).alias('orderamo')
        esdf1=esdf.select(statecolum,'couponcodevalue','memberid','orderamount',orderamo,'ordersn')
        # esdf1.show()
        # | state | couponcodevalue | memberid | orderamount | orderamo | ordersn |
        # +-----+---------------+---------+-----------+--------+--------------------+
        # | 0 | 0.0 | 4035099 | 1139.0 | 1139.0 | amazon_7965239286... |
        # | 0 | 0.0 | 13823237 | 1669.0 | 1669.0 | gome_795747706954617 |
        # | 0 | 0.0 | 138230929 | 1099.0 | 1099.0 | suning_7965732029... |
        # | 0 | 0.0 | 13823365 | 1999.0 | 1999.0 | amazon_7957041033... |
        # todo 2-需要计算的值: 优惠次数discountcnt, , 购买次数purchasecnt,, 优惠金额sumdiscount,, 订单应收总金额sumorderamount,,
        discountcnt=F.sum(esdf1['state']).alias('discountcnt')
        purchasecnt=F.count(esdf1['state']).alias('purchasecnt')
        sumdiscount=F.sum(esdf1['couponcodevalue']).alias('sumdiscount')
        sumorderamount=F.sum(esdf1['orderamo']).alias('sumorderamount')
        esdf2=esdf1.groupby(esdf1['memberid']).agg(discountcnt,purchasecnt,sumdiscount,sumorderamount)
        # esdf2.show()
        # +--------+-----------+-----------+-----------+------------------+
        # | memberid | discountcnt | purchasecnt | sumdiscount | sumorderamount |
        # +--------+-----------+-----------+-----------+------------------+
        # | 13823489 | 2 | 116 | 300.0 | 202913.69989013672 |
        # | 26 | 3 | 111 | 450.0 | 213399.05004882812 |
        # | 4035177 | 2 | 125 | 250.0 | 217878.1700036619 |
        # | 29 | 5 | 137 | 900.0 | 250694.74002075195 |
        # | 4033555 | 2 | 143 | 1100.0 | 234753.96007324196 |
        # | 4034769 | 0 | 120 | 0.0 | 204884.16040039062 |
        # | 4034679 | 1 | 124 | 200.0 | 209654.74002075195 |
        # | 4034681 | 3 | 129 | 600.0 | 196708.8101196289 |
        # | 4034239 | 3 | 127 | 500.0 | 261191.18999267556 |
        # | 4035069 | 3 | 112 | 450.0 | 174422.1700439453 |
        # todo 3-计算: 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比
        # 过滤掉没有使用优惠码的用户
        esdf3 = esdf2.select(esdf2['memberid'], esdf2['discountcnt'] , esdf2['purchasecnt'], esdf2['sumdiscount'],esdf2['sumorderamount']).where(esdf2['discountcnt']!=0)

        discountcnt_rate = (esdf3['discountcnt'] / esdf3['purchasecnt']).alias('discountcnt_rate')
        avgdiscount_rate=((esdf3['sumdiscount']/esdf3['discountcnt']) / (esdf3['sumorderamount'] / esdf3['purchasecnt'])).alias('avgdiscount_rate')
        sumdiscount_rate=(esdf3['sumdiscount'] / esdf3['sumorderamount']).alias('sumdiscount_rate')

        esdf4:DataFrame=esdf3.select(esdf3['memberid'],discountcnt_rate,avgdiscount_rate,sumdiscount_rate)
        # esdf4.show()
        # +--------+--------------------+-------------------+--------------------+
        # | memberid | discountcnt_rate | avgdiscount_rate | sumdiscount_rate |
        # +--------+--------------------+-------------------+--------------------+
        # | 13823489 | 0.017241379310344827 | 0.08575074038579385 | 0.001478461041134... |
        # | 26 | 0.02702702702702703 | 0.07802284029001202 | 0.002108725413243... |
        # | 4035177 | 0.016 | 0.07171438974238396 | 0.001147430235878... |
        # | 29 | 0.0364963503649635 | 0.09836664302553257 | 0.00359002346808513 |
        # | 4033555 | 0.013986013986013986 | 0.3350316219392492 | 0.004685756950199289 |
        # | 4034679 | 0.008064516129032258 | 0.1182897176450447 | 9.539493358471347E-4 |
        # | 4034681 | 0.023255813953488372 | 0.1311583349231266 | 0.003050193835421... |
        # todo 4-计算: 根据优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比,获取psm列
        #三个值都是越大,用户价格敏感度越高,三个值相加
        psm=(esdf4['discountcnt_rate']+esdf4['avgdiscount_rate']+esdf4['sumdiscount_rate']).alias('psm')
        esdf5=esdf4.select(esdf4['memberid'],psm)
        esdf5.show()
        # | memberid | psm |
        # +--------------------+-------------------+
        # | 13823489 | 0.10447058073727304 |
        # | 26 | 0.10715859273028262 |
        # | 4035177 | 0.0888618199782621 |

        # todo 5-聚类: 为方便后续模型进行特征输入，需要将psm的数据转换为特征向量
        vectdf=VectorAssembler().setInputCols(['psm']).setOutputCol('feature').transform(esdf5)
        # vectdf.show()
        # | memberid | psm | feature |
        # +--------+-------------------+--------------------+
        # | 13823489 | 0.10447058073727304 | [0.10447058073727... |
        # | 26 | 0.10715859273028262 | [0.10715859273028... |
        # | 4035177 | 0.0888618199782621 | [0.0888618199782621] |
        # | 29 | 0.1384530168585812 | [0.1384530168585812] |
        # | 4033555 | 0.3537033928754625 | [0.3537033928754625] |

        # todo 6-使用Kmeans算法对df进行训练
        kMeans:KMeans = KMeans()\
        .setK(5) \
        .setSeed(10) \
        .setMaxIter(2) \
        .setFeaturesCol('feature') \
        .setPredictionCol('predictstr')
        model:KMeansModel=kMeans.fit(vectdf)
        # 根据模型预测结果
        resultdf:DataFrame=model.transform(vectdf)
        # resultdf.show()
        # +--------------------+-------------------+--------------------+----------+
        # | memberid | psm | feature | predictstr |
        # +--------------------+-------------------+--------------------+----------+
        # | 13823489 | 0.10447058073727304 | [0.10447058073727... | 1 |
        # | 26 | 0.10715859273028262 | [0.10715859273028... | 1 |
        # | 4035177 | 0.0888618199782621 | [0.0888618199782621] | 1 |
        # | 29 | 0.1384530168585812 | [0.1384530168585812] | 0 |

        # todo 7-获取聚类中心,加索引,排序,然后predictstr列跟fivedf合并
        center=model.clusterCenters()
        # print(center)
        # [array([0.14113081]), array([0.09675275]), array([0.36030053]), array([0.24467312]), array([0.60442997])]
        # [0.14113081][0.09675275][0.36030053][0.24467312][0.60442997]

        #todo 8- 加索引,形成字典
        dict1 = {}
        for i in range(len(center)):
             dict1[i]=float(center[i][0])
        # print(dict1)
        # {0: 0.141130811056604, 1: 0.09675274913886216, 2: 0.3603005265949139, 3: 0.24467312420217655,
        #  4: 0.6044299731603698}

        #todo 9- 转为列表,==>df==>rdd排序==>合并
        list2=[[k,v] for (k,v) in dict1.items()]
        # print(list2)
        # [[0, 0.141130811056604], [1, 0.09675274913886216], [2, 0.3603005265949139], [3, 0.24467312420217655], [4, 0.6044299731603698]]
        centerdf:DataFrame=spark.createDataFrame(list2,['predict','center'])
        # centerdf.show()
        # +-------+-------------------+
        # | predict | center |
        # +-------+-------------------+
        # | 0 | 0.141130811056604 |
        # | 1 | 0.09675274913886216 |
        # | 2 | 0.3603005265949139 |
        # | 3 | 0.24467312420217655 |
        # | 4 | 0.6044299731603698 |

        centersortrdd=centerdf.rdd.sortBy(lambda x:x[1],ascending=False)
        # print(centersortrdd.collect())
        # [Row(predict=4, center=0.6044299731603698), Row(predict=2, center=0.3603005265949139),
        #  Row(predict=3, center=0.24467312420217655), Row(predict=0, center=0.141130811056604),
        #  Row(predict=1, center=0.09675274913886216)]

        # todo 10- 合并fivedf,,先union一个空rdd,再降分区,map,最后zip
        temprdd=centersortrdd.union(sc.parallelize([]))
        unionrdd=temprdd.repartition(1).map(lambda x:x).zip(fivedf.rdd.repartition(1))
        # unionrdd.foreach(lambda x:print(x))
        # (Row(predict=4, center=0.6044299731603698), Row(id=51, rule='1'))
        # (Row(predict=2, center=0.3603005265949139), Row(id=52, rule='2'))
        # (Row(predict=3, center=0.24467312420217655), Row(id=53, rule='3'))
        # (Row(predict=0, center=0.141130811056604), Row(id=54, rule='4'))
        # (Row(predict=1, center=0.09675274913886216), Row(id=55, rule='5'))

        # todo 11- 合并后的rdd,取predict列和fivedf的id列,形成字典
        fivedict = unionrdd.map(lambda row: (row[0][0], row[1][0])).collectAsMap()
        # print(fivedict)
        #{4: 51, 2: 52, 3: 53, 0: 54, 1: 55}
        # todo 12- 根据predict列和id列的对应关系,,匹配resultdf和fivedf
        newdf:DataFrame=resultdf.select(resultdf['memberid'].alias('userId'),
                                        udf(lambda x:fivedict[x],returnType=StringType())(resultdf['predictstr']).alias('tagsId'))
        newdf.show()
        # +------+------+
        # | userId | tagsId |
        # +------+------+
        # | 898 | 53 |
        # | 196 | 53 |
        return newdf
        # rsdf



if __name__ == '__main__':
    priceSenmodel=PriceSenmodel()
    priceSenmodel.execute()
