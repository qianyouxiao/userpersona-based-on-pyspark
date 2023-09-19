#!/usr/bin/env python
__coding__ = "utf-8"


from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	political_affiliation
   Author      :	'钱有霄'
   Date	       :	2023/9/18
-------------------------------------------------
"""


class PoliticalAffiliationTags(Tags_basic_Template):
    def get_level4_id(self):
        return 61

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        level5_tags_dict = mysql_tags_level5_df.rdd.collectAsMap()

        @F.udf
        def get_tags(politicalface):
            return level5_tags_dict.get(str(politicalface))
        new_tags_df = es_df.select(es_df['id'].alias('userId'), get_tags(es_df['politicalface']).alias('tagsId'))
        return new_tags_df


if __name__ == '__main__':
    political_affiliation_tags = PoliticalAffiliationTags()
    political_affiliation_tags.executor()
