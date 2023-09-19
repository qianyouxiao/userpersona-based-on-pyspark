#!/usr/bin/env python
__coding__ = "utf-8"

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from cn.itcastUser.tags.match.tagsClass import Tags_basic_Template

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	consumption_cycle_tags
   Author      :	'钱有霄'
   Date	       :	2023/9/18
-------------------------------------------------
"""


class ConsumptionCycleTags(Tags_basic_Template):
    def get_level4_id(self):
        return 23

    def compute(self, es_df: DataFrame, mysql_tags_level5_df: DataFrame):
        new_df = es_df.groupBy(es_df['memberid'].alias('userId')).agg(F.datediff(F.date_sub(F.current_date(), 1125),
                                                                                 F.from_unixtime(
                                                                                     F.max(es_df['finishtime']),
                                                                                     'yyyy-MM-dd')).alias('days'))

        mysql_tags_level5_new_df = mysql_tags_level5_df.select(mysql_tags_level5_df['id'],
                                                               F.split(mysql_tags_level5_df['rule'], '-')[0].alias(
                                                                   'start'),
                                                               F.split(mysql_tags_level5_df['rule'], '-')[1].alias(
                                                                   'end'))

        rs_df = new_df.join(other=mysql_tags_level5_new_df) \
            .where(new_df['days'].between(mysql_tags_level5_new_df['start'], mysql_tags_level5_new_df['end'])) \
            .select(new_df['userId'], mysql_tags_level5_new_df['id'].alias('tagsId'))
        return rs_df


if __name__ == '__main__':
    consumption_cycle_tags = ConsumptionCycleTags()
    consumption_cycle_tags.executor()
