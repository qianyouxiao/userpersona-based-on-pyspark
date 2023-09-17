#!/usr/bin/env python
# @desc : 匹配标签：年龄段标签
__coding__ = "utf-8"
__author__ = "itcast team"


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from cn.itcast.tags.bean.ESMeta import ESMeta
import os

'''
* 1-准备Spark的环境
* 2-读取MySQL数据
* 3-读取和性别标签相关的{4级}标签解析rule字段{id=4}
* 4-根据4级标签加载{es}数据
* 5-读取和性别标签相关的{5}标签数据{根据pid=4进行查询}
* 6-根据ES的数据和5级标签的数据进行匹配得到userId和tagsId
  * 关键业务代码撰写
* 7-将userid和tagsid存入es中
'''

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

def ruleMapfunction(ruleStr:str):
    # 1-定义一个空字典，存放返回的值
    defaultDict={}
    # 2-对ruleStr进行按照==切分
    ruleSplit = ruleStr.split("##")
    # 3- 使用for循环根据=进一部分切分为字段的kv结构
    for rule in ruleSplit:
        kv = rule.split("=")
        defaultDict[kv[0]]=kv[1]
    # 4-返回数据
    return defaultDict

def mergeLabelUDF(newTagsId: str, oldTagsId: str):
    # 1-如果newTagsID为空，直接返回oldTagsId
    if newTagsId is None:
        return oldTagsId
    # 2-如果oldTagsId为空，直接返回newTagsId
    elif oldTagsId is None:
        return newTagsId
    # 3-直接使用+ newArr标签和oldArr标签进行合并，这里的newArr和oldArr都是list
    else:
        # Return a list of the words in the string
        newArr: list = str(newTagsId).split(",")
        oldArr: list = str(oldTagsId).split(",")
        resultArr = newArr + oldArr
        # 4-针对合并后的标签进行去重,各个标签通过逗号分隔
        res = ",".join(set(resultArr))
        return res


if __name__ == '__main__':
    # TODO* 1-准备Spark的环境
    print("------------------------------1-准备Spark的环境------------------------------------------")
    spark: SparkSession = SparkSession \
        .builder \
        .appName("testAgeModel") \
        .master("local[*]") \
        .getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    # 解决错误：py4j.protocol.Py4JJavaError: An error occurred while calling o45.load.
    # 解决方法：把依赖jar包拷贝到执行环境的SPARK_HOME/jars目录下
    # TODO* 2-读取MySQL的数据  https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    print("------------------------------2-读取MySQL的数据------------------------------------------")
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456"
    tableName = "tbl_basic_tag"
    mysqlDF = spark.read.jdbc(url, tableName)
    # mysqlDF.show(truncate=False)
    # mysqlDF.printSchema()

    # TODO* 3-读取和年龄段标签相关的4级标签和rule字段，年龄段的四级标签的id={14}
    # TODO# * 3-读取和性别标签相关的{4级}标签解析rule字段{id=4}
    print("------------------------------------3-读取和性别标签相关的{4级}标签解析rule字段{id=4}--------------------------------------------")
    fourRuleDS = mysqlDF.select("rule").where("id=4")
    # +---+----+
    # | id | rule |
    # +---+----+
    # | 5 | 1 |
    # | 6 | 2 |
    # +---+----+
    # fourRuleDS.show()
    # fourRuleDS.printSchema()
    # 下一步从fourRuleDS转化为字典的结构
    fourRuleMap = fourRuleDS.rdd.map(lambda row: ruleMapfunction(row["rule"])).collect()[0]
    # [{'inType': 'Elasticsearch'},{},{},{}]现在想取出来第一个字典，请问大家应该怎么做？
    # print(ruleDict)# 获取的是collect之后的list数据
    # print(ruleDict[0]) # 获取的是list的第一个字典的数据

    # TODO* 4-从es中读取和年龄段相关业务数据
    print("------------------------------------4-根据4级标签加载{es}数据--------------------------------------------")
    esMeta = ESMeta.fromDictToEsMeta(fourRuleMap)
    # 导入的包采用-from cn.itcast.tags.bean.ESMeta import ESMeta
    esDF = spark.read \
        .format("es") \
        .option("es.resource", f"{esMeta.esIndex}/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", f"{esMeta.selectFields}") \
        .load()
    esDF.show(truncate=False)
    esDF.printSchema()
    # root
    # | -- sex: string(nullable=true)
    # | -- user_id: string(nullable=true)
    # TODO# * 5-读取和性别标签相关的{5}标签数据{根据pid=4进行查询}
    print("------------------------------------5-读取和性别标签相关的{5}标签数据{根据pid=4进行查询}--------------------------------------------")
    fiveDS = mysqlDF.select("id", "rule").where("pid=4")
    fiveDS.show()
    fiveDS.printSchema()
    # 需求：如何从fiveDS转化为fiveDIct？？
    # 1-fiveDS.rdd之后 下表为0的字段为id=tagsId，下标为1代表rule
    # 2-下面的代码先rule在tagsid
    # 3-为了形成字典类型数据，spark中提供了collectAsmap的api可以转换
    # row[1]代表的是五级标签的rule字段，row[0]代表的是tagsid的字段
    # (rule,tagsid)
    # fiveDict(1),5
    # fiveDict(2),6
    fiveDict = fiveDS.rdd.map(lambda row: (row[1], row[0])).collectAsMap()
    print(fiveDict)  # {rule='1': tasg=5, rule='2': tags=6}
    # TODO# * 6-根据ES的数据和5级标签的数据进行匹配得到userId和tagsId
    print("------------------------------------6-根据ES的数据和5级标签的数据进行匹配得到userId和tagsId--------------------------------------------")
    def gender2Tag(gender: str)->str:
        return fiveDict[str(gender)]
    # TODO#*关键业务代码撰写
    converSexTag = udf(gender2Tag, StringType())
    # 调用udf函数
    newDF = esDF.select(esDF["id"].alias("userId"), converSexTag(esDF["gender"]).alias("tagsId"))
    newDF.show()
    newDF.printSchema()
    # +--------+-------+
    # |  userId|tagsId|
    # +--------+------+
    # | 65-4851|     5|
    # | 101-552|     5|
    # only showing top 20 rows
    #
    # root
    #  |-- userId: string (nullable = true)
    #  |-- tagsId: string (nullable = true)

    #  TODO 7.查询ES中的oldDF--如何从es中查询出已有的标签，使用api？
    # 之前的读取es的数据是es的业务数据用户投保表数据，
    # 但是在这里使用的是es的标签存储结果表insurance-profile-result/_doc
    oldDF = spark.read \
        .format("es") \
        .option("es.resource", f"tfec_userprofile_result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", "userId,tagsId") \
        .load()
    oldDF.show()
    # +------+--------+
    # | tagsId | userId |
    # +------+--------+
    # | 16 | 138 - 460 |
    # | 18 | 20 - 5252 |
    # | 16 | 69 - 509 |
    # | 16 | 13 - 1241 |
    oldDF.printSchema()
    # root
    # | -- tagsId: long(nullable=true)
    # | -- userId: string(nullable=true)
    '''
    参考如下代码：
    lista = ['1','1','2','2','3','4','5']
    a = ';'.join(set(lista))
    print(type(a))
    print(a) #4,2,5,3,1
    '''
    #  TODO 8.合并newDF和oldDF
    resultDF = newDF \
        .join(oldDF, newDF["userId"] == oldDF["userId"], "left") \
        .select(newDF["userId"], udf(mergeLabelUDF, StringType())(newDF["tagsId"], oldDF["tagsId"]).alias("tagsId"))
    resultDF.show()
    # +--------+------+
    # |  userId|tagsId|
    # +--------+------+
    # |  1-3791|  5,15|
    # |   1-501|  18,5|
    # |   1-519|  16,6|
    # |   1-845|  6,15|
    # |   1-898|  18,5|
    # +--------+------+
    # only showing top 20 rows
    #  TODO 9.将最终结果写到ES
    # es.mapping.id 指的是 es的唯一主键
    # es.mapping.name 指的是 spark和es对应的字段的名称
    # es.write.operation 写入的方式，这里选择的upsert的方式，如果没有数据就插入，如果有数据就更新
    resultDF.write \
        .format("es") \
        .option("es.resource", f"tfec_userprofile_result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.mapping.id", f"userId") \
        .option("es.mapping.name", f"userId:userId,tagsId:tagsId") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()
    print("==========================all result is finsised========================================")