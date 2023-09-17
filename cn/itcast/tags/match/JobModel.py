#!/usr/bin/env python
# @desc : 匹配标签：职业标签
__coding__ = "utf-8"
__author__ = "itcast team"


from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from cn.itcast.tags.base.BaseModelES import BaseModelAbstract


class JobModel(BaseModelAbstract):
    def getTagId(self):
        return 7

    def compute(self, esDF:DataFrame, fiveDS:DataFrame):
        print("Execute the compute method of the subclass!")
        # 5.将esDF和fiveDS进行匹配, 得到如下的数据:
        # 5.1.将fiveDF转为map, 方便后续自定义UDF操作
        from pyspark.sql.functions import udf
        fiveDS.show(truncate=False)
        fiveDict = fiveDS.rdd.map(lambda row: (row[1], row[0])).collectAsMap()
        print(fiveDict)

        # 5.2.使用单表 + UDF完成esDF和fiveDS的匹配
        # 自定义DSL风格的udf, 将gender转为tagid
        def convertJobToTagId(job) -> str:
            return fiveDict[str(job)]

        convertTagId = udf(convertJobToTagId, StringType())

        resultDF = esDF.select(esDF.id.alias("userId"), convertTagId(esDF.job).alias("tagsId"))
        resultDF.show(truncate=False)
        resultDF.printSchema()
        return resultDF


if __name__ == '__main__':
    jobModel = JobModel()
    jobModel.execute()
