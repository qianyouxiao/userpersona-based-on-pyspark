#!/usr/bin/env python
# @desc : 匹配标签：年龄段标签
__coding__ = "utf-8"
__author__ = "itcast team"


from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext
from pyspark.sql.types import StringType

from cn.itcast.tags.bean.ESMeta import ESMeta
import os

'''
* 1-准备Spark的环境
* 2-读取MySQL的数据
* 3-读取和年龄段标签相关的4级标签和rule字段，年龄段的四级标签的id={14}
* 4-从es中读取和年龄段相关业务数据
* 5-通过Spark读取MySQL的五级标签，五级标签可以通过父标签定位，pid={14}
* 6-根据ES的数据和5级别标签的数据进行匹配，得出userid和tagsid
  * 6-1 根据用户的birthday的信息中1958-03-21，对数据处理得到19580321
  * 6-2 根据五级标签得到的{19500101-19591231}进行匹配，首先将{19500101-19591231}拆分为start时间和end的时间
  * 6-3 可以将19580321使用sparksql的函数判断是否在start和end之间
* 7-将结果数据直接写入到es的结果表中
'''

# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
# 1-本地路径
# SPARK_HOME = 'F:\\ProgramCJ\\spark-2.4.8-bin-hadoop2.7'
# PYSPARK_PYTHON = 'F:\\ProgramCJ\\Python\\Python37\\python'
# 2-服务器路径
SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

def ruleMapfunction(ruleRow: str):
    defaultMap: dict = {}
    ruleFields = ruleRow.split("##")
    for field in ruleFields:
        kv = field.split("=")
        defaultMap[kv[0]] = kv[1]
    return defaultMap

if __name__ == '__main__':
    # TODO* 1-准备Spark的环境
    print("------------------------------1-准备Spark的环境------------------------------------------")
    spark: SparkSession = SparkSession \
        .builder \
        .appName("testAgeModel") \
        .master("local[*]") \
        .config('spark.sql.shuffle.partitions', 10) \
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
    print("------------------------------3-读取和年龄段标签相关的4级标签和rule字段------------------------------------------")
    fourRuleDS = mysqlDF.select("rule").where("id=14")

    # 解析rule为dict => map python中的dict类型对应scala的map集合
    fourRuleMap = fourRuleDS.rdd.map(lambda row: ruleMapfunction(row["rule"])).collect()[0]
    # print(fourRuleMap)


    # TODO* 4-从es中读取和年龄段相关业务数据
    print("------------------------------4-从es中读取和年龄段相关业务数据------------------------------------------")
    esMeta = ESMeta.fromDictToEsMeta(fourRuleMap)
    # print(esMeta)
    # spark读取es数据源，过滤字段参数：es.read.field.include
    # schema = StructType()\
    #         .add("user_id", StringType(), nullable=True)\
    #         .add("birthday", StringType(), nullable=True)
    # source: org.elasticsearch.spark.sql or es
    esDF = spark.read \
        .format("es") \
        .option("es.resource", f"{esMeta.esIndex}/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.index.read.missing.as.empty", "yes") \
        .option("es.query", "?q=*") \
        .option("es.read.field.include", f"{esMeta.selectFields}") \
        .option("es.mapping.date.rich", "false") \
        .load()
    # .schema("user_id string, birthday timestamp")
    esDF.show(truncate=False)
    esDF.printSchema()
    # TODO* 5-通过Spark读取MySQL的五级标签，五级标签可以通过父标签定位，pid={14}
    print("------------------------------5-通过Spark读取MySQL的五级标签------------------------------------------")
    fiveDS = mysqlDF.select("id", "rule").where("pid=14")
    # fiveDS.show(truncate=False)
    # fiveDS.printSchema()

    from pyspark.sql.functions import regexp_replace

    # TODO* 6-根据ES数据和5级标签数据进行匹配,得出userId,tagsId
    print("------------------------------6-根据ES的业务数据和5级别标签的数据进行匹配，得出userid和tagsid------------------------------------------")
    # TODO  * 6-1 根据用户的birthday的信息中1948-03-21 00:00:00对数据处理得到19580321
    esDF2 = esDF.select(esDF.id.alias("userId"), regexp_replace(esDF.birthday[0:10], "-", "").alias("birthday"))
    # esDF2.show(truncate=False)
    # TODO  * 6-2 根据五级标签得到的{19500101-19591231}进行匹配，首先将{19500101-19591231}拆分为start时间和end的时间
    print("------------------------------6-2 根据五级标签得到的{19500101-19591231}进行匹配，首先将{19500101-19591231}拆分为start时间和end的时间------------------------------------------")
    fiveDS2 = fiveDS.rdd.map(lambda row: (row["id"], row["rule"].split("-")[0], row["rule"].split("-")[1])) \
        .toDF(["tagsId", "start", "end"])
    fiveDS2.printSchema()
    fiveDS2.show(truncate=False)
    # TODO  * 6-3 可以将19580321使用sparksql的函数判断是否在start和end之间
    print("------------------------------6-3 可以将19580321使用sparksql的函数判断是否在start和end之间------------------------------------------")
    # 1-通过edDF2和fiveDS2进行join
    # 2-通过判断esDF2的birthday是否在fiveDS2的start和end之间，如果在的化直接将tagsId打上标签即可
    newDF = esDF2 \
        .join(fiveDS2) \
        .where(esDF2.birthday.between(fiveDS2.start, fiveDS2.end)) \
        .select(esDF2.userId.cast("string"), fiveDS2.tagsId.cast("string"))
    newDF.show(truncate=False)
    newDF.printSchema()
    print(newDF.count())
    # 8.将最终结果写到ES
    # es.mapping.id:包含文档id的文档字段/属性名称。在es-head中能够通过数据查看到唯一标识
    newDF.write \
        .format("es") \
        .option("es.resource", f"tfec_userprofile_result/{esMeta.esType}") \
        .option("es.nodes", f"{esMeta.esNodes}") \
        .option("es.mapping.id", "userId") \
        .option("es.mapping.name", "userId:userId,tagsId:tagsId") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()
