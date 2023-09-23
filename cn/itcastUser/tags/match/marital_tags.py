#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	marital_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/18
-------------------------------------------------
"""


class MaritalTags(Tags_basic_Template):
    def get_level4_id(self):
        return 65

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        lever5_tags_dict = mysql_tags_level5_df.rdd.collectAsMap()

        @F.udf
        def get_tags(marriage):
            return lever5_tags_dict.get(marriage)

        new_tags_df = es_df.select(es_df['id'].alias('userId'), get_tags(es_df['marriage']).alias('tagsId'))
        print(new_tags_df.count())


if __name__ == '__main__':
    marital_df = MaritalTags()
    marital_df.executor()
