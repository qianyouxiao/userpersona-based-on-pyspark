#!/usr/bin/env python
# @desc : 统计标签：消费周期
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from cn.itcast.tags.base.BaseModelES import BaseModelAbstract


class ConsumptionCycleModel(BaseModelAbstract):

    def getTagId(self):
        return 23

    def compute(self, esDF: DataFrame, fiveDS: DataFrame):
        print("Execute the compute method of the subclass!")
        # 1.求用户最近一次消费时间
        # 根据用户分组取finishTime最大值即可
        esDF.show(truncate=False)
        tempDF = esDF \
            .groupBy(esDF["memberid"].alias("userId")) \
            .agg(F.max(esDF["finishtime"]).alias("maxFinishTime"))
        tempDF.show(truncate=False)
        # 2.求用户最近一次消费时间距离今天的天数
        # from_unixtime: 将maxFinishTime转为时间对象
        # current_date: 获取当前时间对象
        # datediff: 求当前时间和maxFinishTime时间的天数差
        # date_sub: 表示把时间减去多少天
        # 声明daysCoulmn该如何计算, 但是并没有真正的执行, 得去select才可以
        # 数据中，购买时间范围：2001-04-06~2003-04-03 2003年4月3日距离2022年7月04日 7028天
        daysCoulmn = F.datediff(F.date_sub(F.current_date(), 200), F.from_unixtime('maxFinishTime')).alias("days")
        # daysCoulmn = ((F.unix_timestamp(F.current_timestamp()))-1042*86400-tempDF.maxFinishTime)/86400
        # daysCoulmn = F.datediff(F.from_unixtime('currentTime'), F.from_unixtime('maxFinishTime')).alias("days")
        tempDF2 = tempDF.select(tempDF.userId, daysCoulmn)
        tempDF2.show(truncate=False)

        # 3.将fiveDS拆成:("tagsId","start","end")
        fiveDS2 = fiveDS.rdd.map(lambda row:(row.id, row.rule.split("-")[0], row.rule.split("-")[1])) \
            .toDF(["tagsId", "start", "end"])
        fiveDS2.show(truncate=False)

        # 4.将tempDF2和fiveDS2进行匹配
        newDF = tempDF2 \
            .join(fiveDS2) \
            .where(tempDF2.days.between(fiveDS2.start, fiveDS2.end)) \
            .select(tempDF2.userId, fiveDS2.tagsId).where(tempDF2.userId.isNotNull())
        newDF.show(truncate=False)
        return newDF


if __name__ == '__main__':
    consCycleModel = ConsumptionCycleModel()
    consCycleModel.execute()
