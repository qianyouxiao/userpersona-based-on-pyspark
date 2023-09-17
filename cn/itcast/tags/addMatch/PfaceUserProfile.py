#!/usr/bin/env python
# @desc : 实现用户政治面貌标签开发
# 四级标签id=65,五级标签为1,2,3，其中'政治面貌：1群众、2党员、3无党派人士'
#`nationality` '国籍：1中国大陆、2中国香港、3中国澳门、4中国台湾、5其他'
__coding__ = "utf-8"
__author__ = "itcast team"

import os
from pyspark import SparkContext
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType



# 构建Spark的Windows环境
from cn.itcast.tags.base.BaseModelES import BaseModelAbstract

SPARK_HOME = 'D:\\ProgramCj\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'D:\\ProgramCj\\Python\\python'
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON


class Pfacemodel(BaseModelAbstract):
    def getTagId(self):
        return 61

    def compute(self, esdf:DataFrame, fivedf:DataFrame):
        # esdf.show()
        # +---+-------------+
        # | id | politicalface |
        # +---+-------------+
        # | 1 | 1 |
        # | 2 | 3 |
        # | 3 | 3 |
        # | 4 | 1 |
        # | 5 | 1 |
        # | 6 | 2 |

        # fivedf.show()
        # +---+----+
        # | id | rule |
        # +---+----+
        # | 62 | 1 |
        # | 63 | 2 |
        # | 64 | 3 |
        # +---+----+
        # todo 6 匹配5级标签和es数据
        #todo 6-1处理fivedfd 反转成字典
        fivedfdict=fivedf.rdd.map(lambda row:(row['rule'],row['id'])).collectAsMap()
        print(fivedfdict)
        # {'1': 62, '2': 63, '3': 64}
        # todo 6-2 匹配5级标签和es数据
        newdf=esdf.select(esdf['id'].alias('userId'),udf(lambda x:fivedfdict[str(x)])(esdf['politicalface']).alias('tagsId'))
        newdf.show()
        # +------+------+
        # | userId | tagsId |
        # +------+------+
        # | 1 | 62 |
        # | 2 | 64 |
        # | 3 | 64 |
        # | 4 | 62 |
        # | 5 | 62 |
        # | 6 | 63 |
        # | 7 | 64 |

        # return newdf
        # rsdf
        # +------+-------------------+
        # | userId | tagsId |
        # +------+-------------------+
        # | 26 | 30, 42, 12, 63, 27, 20, 6 |
        # | 29 | 62, 17, 30, 38, 27, 11, 6 |
        # | 474 | 12, 17, 5, 64 |
        # | 65 | 17, 5, 40, 30, 8, 63, 27 |
        # | 191 | 20, 62, 5, 13 |
        # | 418 | 19, 6, 63, 13 |
        # | 541 | 12, 17, 64, 6 |
        # | 558 | 62, 8, 6, 19 |


if __name__ == '__main__':
    pfacemodel=Pfacemodel()
    pfacemodel.execute()
