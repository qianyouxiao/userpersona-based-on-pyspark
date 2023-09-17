#!/usr/bin/env python
# @desc : 实现用户婚姻状况标签开发
# 四级标签id=65,五级标签为1,2,3，其中1为未婚，2位已婚，3为未知
__coding__ = "utf-8"
__author__ = "itcast team"

import os
from pyspark import SparkContext
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
from cn.itcast.tags.base.BaseModelES import BaseModelAbstract

SPARK_HOME = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'F:\\ProgramCJ\\Python\\Python37\\python'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

class Marrymodel(BaseModelAbstract):
    def getTagId(self):
        return 65

    def compute(self, esdf:DataFrame, fivedf:DataFrame):
        # esdf.show()
        # +---+--------+
        # | id | marriage |
        # +---+--------+
        # | 1 | 2 |
        # | 2 | 2 |
        # | 3 | 2 |
        # | 4 | 1 |
        # | 5 | 3 |
        # | 6 | 1 |
        # | 7 | 2 |

        # fivedf.show()
        # +---+----+
        # | id | rule |
        # +---+----+
        # | 66 | 1 |
        # | 67 | 2 |
        # | 68 | 3 |
        # todo 6 匹配5级标签和es数据
        #todo 6-1处理fivedfd 反转成字典
        fivedfdict=fivedf.rdd.map(lambda row:(row['rule'],row['id'])).collectAsMap()
        # print(fivedfdict)
        # {'1': 66, '2': 67, '3': 68}

        # todo 6-2 匹配5级标签和es数据
        newdf=esdf.select(esdf['id'].alias('userId'),udf(lambda x:fivedfdict[x])(esdf['marriage']).alias('tagsId'))
        newdf.show()
        # +------+------+
        # | userId | tagsId |
        # +------+------+
        # | 1 | 67 |
        # | 2 | 67 |
        # | 3 | 67 |
        # | 4 | 66 |
        # | 5 | 68 |
        # | 6 | 66 |
        # | 7 | 67 |

        # return newdf
        # rsdf
        # +------+-------------------+
        # | userId | tagsId |
        # +------+-------------------+
        # | 26 | 30, 42, 66, 12, 27, 20, 6 |
        # | 29 | 17, 30, 38, 67, 27, 11, 6 |
        # | 474 | 12, 17, 5, 67 |
        # | 65 | 17, 5, 40, 30, 8, 67, 27 |
        # | 191 | 20, 5, 66, 13 |
        # | 418 | 19, 6, 66, 13 |
        # | 541 | 12, 17, 67, 6 |
        # | 558 | 19, 8, 6, 66 |
        # | 222 | 19, 6, 66, 13 |
        # | 270 | 10, 19, 5, 66 |
        # | 293 | 12, 17, 5, 67 |
        # | 730 | 20, 6, 66, 13 |
        # | 938 | 19, 11, 6, 66 |
        # | 243 | 8, 67, 19, 6 |
        # | 278 | 13, 67, 19, 6 |

if __name__ == '__main__':
    marrymodel=Marrymodel()
    marrymodel.execute()
