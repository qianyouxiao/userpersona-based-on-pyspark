#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"
'''
 \#0.准备Spark开发环境(重复)
 \#1.读取MySQL中的数据(重复)
 \#2.读取模型/标签相关的4级标签rule并解析--=**=标签id不一样==**
  性别标签 id=4**
  年龄段的标签 id=14**
  \#3.【ES的数据源】根据解析出来的rule读取ES数据(重复)
  \#4.【5级标签】读取模型/标签相关的5级标签(根据4级标签的id作为pid查询)---**==标签id不一样==**
  \#====5.根据ES数据和5级标签数据进行匹配,得出userId,tagsId---**==实现代码不一样==**
  \#6.查询elasticsearch中的oldDF(重复)
  \#7.合并newDF和oldDF(重复)
  \#8.将最终结果写到ES(重复)
  接下来重构代码：
  * 思路：重点将同样的部分抽取到基类中(父类/父接口)，不一样的部分可以在子类中实现**
  * 1-首先将相同的代码直接在基类实现即可
  * 2-对于不同的业务代码，可以在基类中定义为抽象方法，在子类中实现该抽象方法即可
'''
import abc

import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from cn.itcast.tags.bean.ESMeta import ESMeta

SPARK_HOME = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
PYSPARK_PYTHON = 'F:\\ProgramCJ\\Python\\Python37\\python'
# 2-服务器路径
# SPARK_HOME = '/export/server/spark'
# PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
# TODO 0.准备Spark开发环境(重复)
spark = SparkSession \
    .builder \
    .appName("testAgeModel") \
    .master("local[*]") \
    .getOrCreate()
# 这里的SparkContext就是返回值，为了在使用sc的时候可以直接代码提示
sc: SparkContext = spark.sparkContext
sc.setLogLevel("WARN")

# spark通过jdbc获得mysql标签规则数据
def getMySQLData() -> DataFrame:
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    tableName = "tbl_basic_tag"
    mysqlDF = spark.read.jdbc(url, tableName)
    return mysqlDF

def ruleMapFuntions(ruleStr: str) -> dict:
    '''
    :param ruleStr:  传入的str类型的ryle字典
    :return: 字典
    '''
    # 1-定义字典存放key和value的值
    defaultDict = {}
    # 2-ruleStr根据##进行切分
    ruleSplit = ruleStr.split("##")
    # 3-上述结果切分后有很多kv，通过for循环进一步rule根据=进行切分
    for rule in ruleSplit:
        kv = rule.split("=")  # {kv[0],kv[1]},{inType,Elasticsearch},{esNodes,192.168.88.166:9200}
        defaultDict[kv[0]] = kv[1]
    # 4-返回结果
    return defaultDict

def getFourRule(mysqlDF:DataFrame, id:int):
    #  TODO 3.读取和性别标签相关的4级标签rule并解析
    fourRuleDF: DataFrame = mysqlDF.select("rule").where(f"id={id}")
    fouRuleict = fourRuleDF.rdd.map(lambda row: ruleMapFuntions(row["rule"])).collect()[0]
    esMeta:ESMeta = ESMeta.fromDictToEsMeta(fouRuleict)
    return esMeta


def getEsDF(esMeta:ESMeta):
    #  TODO 4.根据4级标签加载ES数据(从hive中导入的数据，源数据)
    esDF = spark.read.format("es") \
        .option("es.resource", f"{esMeta.esIndex}/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", f"{esMeta.selectFields}") \
        .load()
    return esDF


def getFiveRuleDF(mysqlDF:DataFrame, id:int):
    #  TODO 5.读取和性别标签相关的5级标签(根据4级标签的id作为pid查询)
    fiveRuleDF: DataFrame = mysqlDF.select("id", "rule").where(f"pid={id}")
    return fiveRuleDF


def getEsOldDF(esMeta:ESMeta):
    oldDF = spark.read.format("es") \
        .option("es.resource", f"insurance-profile-result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", "userId,tagsId") \
        .load()
    return oldDF

def mergeLabelUDF(tagIdList: str,newTagsId: str, oldTagsId: str):
    # 1-如果newTagsID为空，直接返回oldTagsId
    if newTagsId is None:
        return oldTagsId
    # 2-如果oldTagsId为空，直接返回newTagsId
    elif oldTagsId is None:
        return newTagsId
    # 3-直接使用+ newArr标签和oldArr标签进行合并，这里的newArr和oldArr都是list
    oldTagsList = str(oldTagsId).split(',')
    # 如果存在oldTagsId，从oldTagsList去除，再添加newTagsId
    for id in tagIdList.split(','):
        if oldTagsList.__contains__(id):
            oldTagsList.remove(id)
            # print("id----------",id)
    # print("oldTagsList--------",oldTagsList)
    resultList = oldTagsList + str(newTagsId).split(',')
    print("re",resultList)
    # print(resultList)
    # 去重可以不需要
    # 哪一种集合可以自动去重: set
    return ",".join(resultList)
def getFiveRuleIdList(fiveDF: DataFrame):
    return fiveDF.rdd.map(lambda row: row["tagsId"]).collect()
import pyspark.sql.functions  as F
def mergeDF(fiveTagsList,newDF:DataFrame, oldDF:DataFrame):
    #  TODO 8.合并newDF和oldDF
    fiveTags = ''  # 定义变量接受五级标签结果当做一列
    for fiveId in fiveTagsList:
        fiveTags += str(fiveId) + ','
        print(fiveTags)
    # lit : 添加新的列 列名 列的值  列数据类型不支持list类型[可以通过int，str传参]：是上面fiveId转str类型的原因
    newDF = newDF.withColumn('fiveTagsList', F.lit(fiveTags))
    print("change newDF")
    newDF.show()
    # +------+------+------------+
    # | userId | tagsId | fiveTagsList |
    # +------+------+------------+
    # | 1 | 6 | 5, 6, |
    # | 2 | 5 | 5, 6, |
    # | 3 | 5 | 5, 6, |
    # userId=1,如果更新标签去掉oldDF的6，更新为5或者其他值
    resultDF = newDF \
        .join(oldDF, newDF["userId"] == oldDF["userId"], "left") \
        .select(newDF["userId"],
                udf(mergeLabelUDF, StringType())(newDF["fiveTagsList"], newDF["tagsId"], oldDF["tagsId"]).alias(
                    "tagsId"))
    resultDF.show()
    return resultDF


def saveToES(resultDF:DataFrame, esMeta:DataFrame):
    resultDF.write.format("es") \
        .option("es.resource", f"insurance-profile-result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.mapping.id", f"userId") \
        .option("es.mapping.name", f"userId:userId,tagsId:tagsId") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()
# 获得fiveDF tagsId集合，返回list
def getFiveRuleIdList(self, fiveDF: DataFrame):
    return fiveDF.rdd.map(lambda row: row.tagsId).collect()

# class BaseModelAbstract(abc.ABC):
class BaseModelAbstract(metaclass=abc.ABCMeta):
    # 2- 对于8个步骤中个的3个步骤需要提取出来成为抽象函数
    # 对于四级标签抽象
    @abc.abstractmethod
    def getTagId(self):
        pass
    # 对于打标签业务逻辑抽象
    @abc.abstractmethod
    def compute(self,esDF:DataFrame,fiveDF:DataFrame):
        pass
    # 1- 通过一个函数compute实现8个步骤
    def execute(self):
        # TODO 1.读取MySQL中的数据(重复)
        mysqlDF:DataFrame = getMySQLData()
        # TODO 2.读取模型/标签相关的4级标签rule并解析--=标签id不一样==
        id:int = self.getTagId()# 该方法需要在子类中实现
        esMeta:ESMeta = getFourRule(mysqlDF, id)
        # TODO 3.【ES的数据源】根据解析出来的rule读取ES数据(重复)
        esDF:DataFrame = getEsDF(esMeta)
        # TODO 4.【5级标签】读取模型/标签相关的5级标签(根据4级标签的id作为pid查询)---==标签id不一样==
        fiveDF:DataFrame=getFiveRuleDF(mysqlDF,id)#上面得到id就是这里面的pid
        # TODO ====5.根据ES数据和5级标签数据进行匹配,得出userId,tagsId---==实现代码不一样==
        newDF:DataFrame = self.compute(esDF, fiveDF)
        # TODO 6.查询elasticsearch中的oldDF(重复)
        oldDF:DataFrame = getEsOldDF(esMeta)
        # todo 获得五级标签id集合：在合并newDF和oldDF时，针对标签结果进行更新
        fiveTagsList = self.getFiveRuleIdList(fiveDF)
        # TODO 7.合并newDF和oldDF(重复)
        resultDF:DataFrame = mergeDF(fiveTagsList,newDF, oldDF)
        # TODO 8.将最终结果写到ES(重复)
        saveToES(resultDF,esMeta)

