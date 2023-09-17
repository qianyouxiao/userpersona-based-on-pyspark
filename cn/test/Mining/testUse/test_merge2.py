import abc

# todo:1 准备spark环境
import os

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
SPARK_HOME = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'F:\\ProgramCJ\\Python\\Python37\\python'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = (SparkSession.builder
         .appName('testapp')
         .master('local[*]')
         .config('spark.sql.shuffle.partitions', 10)
         .getOrCreate()
         )
spark.sparkContext.setLogLevel('WARN')

# todo:模拟MySQL 5级标签的id
l = [('1',), ('2',), ('3',), ('4',)]
mysql_df_5 = spark.createDataFrame(l, ['id'])
mysql_df_5.show()
# +---+
# | id|
# +---+
# |  1|
# |  2|
# |  3|
# |  4|
# +---+


# todo:模拟新获取的到tagsid
n = [('a', '1'), ('b', '2'), ('c', '2'), ('d', '1')]
new_df = spark.createDataFrame(n, ['userid', 'tagsid'])
new_df.show()
# +------+------+
# |userid|tagsid|
# +------+------+
# |     a|     1|
# |     b|     2|
# |     c|     2|
# |     d|     1|
# +------+------+
# todo:模拟从es获取到的旧数据
k = [('a', '1,n'), ('b', '2,k'), ('c', '3,l'), ('d', '4,j')]
old_df = spark.createDataFrame(k, ['userid', 'tagsid'])
old_df.show()
# +------+------+
# |userid|tagsid|
# +------+------+
# |     a|   1,n|
# |     b|   2,k|
# |     c|   3,l|
# |     d|   4,j|
# +------+------+

print('----------------------------0-上课讲的-----------------------------------------')


# todo ：上课讲的 合并新老 标签 id 的方法
@udf
def merge_old(newTagsId: str, oldTagsId: str):
    if newTagsId is None:
        return oldTagsId
    elif oldTagsId is None:
        return newTagsId
    else:
        new: list = str(newTagsId).split(',')
        old: list = str(oldTagsId).split(',')
        re_tags: list = new + old
        return ','.join(set(re_tags))


# todo:新的和旧的合并
re_df_old = new_df.join(old_df, new_df['userid'] == old_df['userid'], 'left') \
    .select(new_df['userid'], merge_old(new_df['tagsid'], old_df['tagsid']).alias('tagsid'))
re_df_old.show()
# todo ：可以看出 c和d出现了既有3,2  既有1,4 这种一个标签，两个情况的现象
# +------+------+
# |userid|tagsid|
# +------+------+
# |     a|   1,n|
# |     b|   k,2|
# |     c| l,3,2|
# |     d| 4,j,1|
# +------+------+

print('----------------------------1-我修正后，不报错的-----------------------------------------')

# todo abc:从把5级标签的id也就是我们要的tagsid字段变成列表，

new_all_tags = mysql_df_5.select(mysql_df_5['id']).rdd.map(lambda row: row['id']).collect()


# print(type(new_all_tags))  # <class 'list'>
# ['1', '2', '3', '4']

# todo:新旧合并udf函数。这里面通过remove，移除旧数据，关于这个标签所涉及的所用相关tagsid，并把更新的tagsid和旧的tagsid（去除本次标签相关的）合并
@udf
def merge(newTagsId: str, oldTagsId: str):
    if newTagsId is None:
        return oldTagsId
    elif oldTagsId is None:
        return newTagsId
    else:
        new: list = str(newTagsId).split(',')
        old: list = str(oldTagsId).split(',')

        for i in new_all_tags:  # new_all_tags 是5级标签的id也就是相关的所有tagsid
            if i in old:
                old.remove(i)
        re_tags: list = new + old
        return ','.join(re_tags)


# todo:新的和旧的合并
re_df = new_df.join(old_df, new_df['userid'] == old_df['userid'], 'left')\
    .select(new_df['userid'], merge(new_df['tagsid'], old_df['tagsid']).alias('tagsid'))
re_df.show()
# todo:join之后使用merge这个udf函数合并新老标签如下，可以看到 c 和 d 的3,4除去了。
#  并不会成为，既有2又有3，既有1和4这种情况
# +------+------+
# |userid|tagsid|
# +------+------+
# |     a|   1,n|
# |     b|   2,k|
# |     c|   2,l|
# |     d|   1,j|
# +------+------+
