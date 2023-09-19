#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	payway_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/18
-------------------------------------------------
"""
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQL APP") \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()


class PayWayTags(Tags_basic_Template):
    def get_level4_id(self):
        return 29

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        es_df.createOrReplaceTempView('tmp')
        new_df = spark.sql("""
            with t1 as (
                select memberid,
                       paymentcode,
                       count(*) as cnt,
                       row_number()over(partition by memberid order by count(*) desc) as rn
                from tmp
                group by memberid,paymentcode)
            select memberid,paymentcode from t1 where rn=1
        """)
        my_dict = mysql_tags_level5_df.rdd.collectAsMap()
        @F.udf
        def get_tags(paymentcode):
            return my_dict.get(paymentcode)
        rs_df = new_df.select(new_df['memberid'].alias('userId'), get_tags(new_df['paymentcode']).alias('tagsId'))
        return rs_df


if __name__ == '__main__':
    payway_tags = PayWayTags()
    payway_tags.executor()

