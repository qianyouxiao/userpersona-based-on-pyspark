#!/usr/bin/env python
__coding__ = "utf-8"

import os

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	profession_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/18
-------------------------------------------------
"""


class ProfessionTags(Tags_basic_Template):
    def get_level4_id(self):
        return 7

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        level5_tags_dict = mysql_tags_level5_df.rdd.collectAsMap()

        @F.udf
        def get_tags(job):
            return level5_tags_dict.get(job)
        new_tags_df = es_df.select(es_df['id'].alias('userId'), get_tags(es_df['job']).alias('tagsId'))
        return new_tags_df


if __name__ == '__main__':
    profession_tags = ProfessionTags()
    profession_tags.executor()
