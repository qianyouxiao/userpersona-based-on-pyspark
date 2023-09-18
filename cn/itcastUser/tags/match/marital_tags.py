#!/usr/bin/env python
__coding__ = "utf-8"

import os

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

    def compute(self, es_df, mysql_tags_level5_df):
        pass


if __name__ == '__main__':
    marital_df = MaritalTags()
    marital_df.executor()
