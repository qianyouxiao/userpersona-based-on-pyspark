#!/usr/bin/env python
# @desc : 统计标签：支付方式
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType

from cn.itcast.tags.base.BaseModelES import BaseModelAbstract
from pyspark.sql import DataFrame, SparkSession

# TODO 0.准备Spark开发环境(重复)
spark = SparkSession \
    .builder \
    .appName("TfecProfile") \
    .master("local[*]") \
    .config('spark.sql.shuffle.partitions', 10) \
    .getOrCreate()

class PaymentModel(BaseModelAbstract):
    def getTagId(self):
        return 29

    def compute(self, esDF: DataFrame, fiveDS: DataFrame):
        print("Execute the compute method of the subclass!")
        fiveDS.show(truncate=False)
        # fiveDS.printSchema()
        # +---+--------+
        # | id | rule |
        # +---+--------+
        # | 30 | alipay |
        # | 31 | wxpay |
        # | 32 | chinapay |
        # | 33 | kjtpay |
        # | 34 | cod |
        # | 35 | other |
        # +---+--------+

        # 1.根据用户id+缴费周期进行分组并计数
        esDF.printSchema()
        tempDF = esDF \
            .groupBy(esDF["memberId"], esDF["paymentCode"]) \
            .agg(F.count(esDF.paymentcode).alias("counts")) \
            .select(esDF.memberid.alias("userId"), esDF.paymentcode, "counts") \
            .where(esDF.memberid.isNotNull())
        tempDF.show(truncate=False)

        # 2.使用开窗函数进行组内排序,取Top1(每个用户使用最多的缴费周期)
        # 方式一:DSL风格开窗函数
        tempDF2 = tempDF.withColumn("rn",
                                    F.row_number()
                                    .over(Window
                                          .partitionBy(tempDF.userId)
                                          .orderBy(tempDF.counts.desc())
                                          )
                                    )
        # tempDF2.show(truncate=False)
        rankDF = tempDF2.where(tempDF2.rn == 1)
        rankDF.show(truncate=False)

        # 方式二:SQL风格开窗函数
        tempDF.createOrReplaceTempView("t_temp")
        sql = "select userId, paymentcode, counts, row_number() over(partition by userId order by counts desc) rn from t_temp"
        tempDF3 = spark.sql(sql)

        # tempDF3.show(truncate=False)
        rankDF2 = tempDF3.where(tempDF3.rn == 1)
        # rankDF2.show(truncate=False)

        # 3.将rankDF和fiveDS进行匹配
        # 3.1.将fiveDS转为map[缴费周期, tagsId]
        # fiveDS.as[(tagsId, 缴费周期)]
        # fiveMap: dict{缴费周期:tagsId}
        fiveDict = fiveDS.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()

        # print(fiveDict)
        # 3.2将rankDF中的缴费周期换成tagsId
        def paymentPeriod2tagsId(payment: str) -> str:
            return fiveDict.get(payment)

        # 注册udf函数
        paymentPeriod2TagsIdUDF = F.udf(paymentPeriod2tagsId, LongType())
        newDF = rankDF.select(rankDF.userId, paymentPeriod2TagsIdUDF(rankDF.paymentcode).alias("tagsId"))
        newDF.show(truncate=False)
        return newDF


if __name__ == '__main__':
    paymentModel = PaymentModel()
    paymentModel.execute()
