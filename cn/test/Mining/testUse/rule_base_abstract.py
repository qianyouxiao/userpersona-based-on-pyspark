import abc
import os

from py4j.protocol import Py4JJavaError
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark.sql.types import StringType
from config.configReader import getMysqlConfig

# todo 解决了  服务器 和  本地 环境要来回切换的麻烦
try:
    # 2-服务器路径
    SPARK_HOME = '/export/server/spark'
    PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

    spark = (SparkSession.builder
             .appName('testapp')
             .master('local[*]')
             .config('spark.sql.shuffle.partitions', 10)
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel('ERROR')
except FileNotFoundError as e:
    # 1-本地路径
    SPARK_HOME = r'C:\Develop\yonghuhuaxiang\spark-2.4.8-bin-hadoop2.7'
    PYSPARK_PYTHON = r'C:\Develop\Anaconda3\envs\yonghuhuaxiang\python'
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

    spark = (SparkSession.builder
             .appName('testapp')
             .master('local[*]')
             .config('spark.sql.shuffle.partitions', 10)
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel('ERROR')



def get_mysql_df():
    # todo 读取 MySQL 数据
    url = 'jdbc:mysql://192.168.88.166:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456'
    table = 'tbl_basic_tag'
    # url = getMysqlConfig('jdbc_url')
    # table = getMysqlConfig('table_name')
    # print(url)
    # print(table)
    mysql_df: DataFrame = spark.read.jdbc(url=url,table=table)
    return mysql_df


# todo 筛选 MySQL 4级  标签数据

def get_mysql4(mysql_df: DataFrame, id: int):
    mysql4: DataFrame = mysql_df.select(mysql_df['rule']).where(mysql_df['id'] == id)
    return mysql4


# todo 解析4级标签rule为dict

def rule2dict(rule: str):
    # rule_dict = {}
    # rule_list1 = rule.split('##')  # ['inType=Elasticsearch','esNodes=up01:9200']
    # for i in rule_list1:
    #     rule_list2 = i.split('=')  # ['inType','Elasticsearch']
    #     rule_dict[rule_list2[0]] = rule_list2[1]
    # return rule_dict
    # todo : 写法二
    return {i.split('=')[0]: i.split('=')[1] for i in rule.split('##')}


# todo 将4级标签的dict转为class
class rule2class:
    def __init__(self, rule_dict: dict):
        self.inType = rule_dict.get('inType')
        self.esNodes = rule_dict.get('esNodes')
        self.esIndex = rule_dict.get('esIndex')
        self.esType = rule_dict.get('esType')
        self.selectFields = rule_dict.get('selectFields')


# todo 解析 rule

def get_rule_dict(mysql4: DataFrame):
    rule_dict: dict = mysql4.rdd.map(lambda row: rule2dict(row['rule'])).collect()[0]
    rule_class = rule2class(rule_dict)
    return rule_class


# todo 读取 es数据

def get_es_data(rule_class: rule2class):
    es_data_df: DataFrame = (spark.read.format('es')
                             .option('es.nodes', rule_class.esNodes)
                             .option('es.resource', f'{rule_class.esIndex}/{rule_class.esType}')
                             .option('es.index.read.missing.as.empty', 'yes')
                             .option('es.query', f'?q=*')
                             # .option('es.write.operation', 'upsert')
                             .option('es.read.field.include', rule_class.selectFields)
                             .load()
                             )
    return es_data_df


# todo 筛选 MySQL 5级  标签数据  和  5级tagsid列表
mysql5_list = []



def get_mysql5(mysql_df: DataFrame, id: int):
    mysql5: DataFrame = mysql_df.select(mysql_df['id'].alias('tagsId'), mysql_df['rule']).where(mysql_df['pid'] == id)
    global mysql5_list
    mysql5_list = mysql5.rdd.map(lambda row: str(row['tagsId'])).collect()
    return mysql5


# todo 读取 es旧标签数据

def get_es_old(rule_class: rule2class):
    es_old: DataFrame = (spark.read.format('es')
                         .option('es.nodes', rule_class.esNodes)
                         .option('es.resource', f'tfec_tbl_result/{rule_class.esType}')
                         .option('es.index.read.missing.as.empty', 'yes')
                         .option('es.query', f'?q=*')
                         # .option('es.write.operation', 'upsert')
                         .option('es.read.field.include', 'userId,tagsId')
                         .load()
                         )
    return es_old


# todo 合并 es标签新数据 es旧标签数据  的函数
def merge(new: str, old: str):
    if new is None:
        return old
    elif old is None:
        return new
    else:
        new_tags = str(new).split(',')  # '80' → ['80']
        old_tags = str(old).split(',')  # ['19','70','79']
        for i in old_tags:
            if i in mysql5_list:
                old_tags.remove(i)  # ['19','70']
        return ','.join(old_tags + new_tags)  # '19,70,80'


# todo 获取 MySQL 5级标签列表
# todo 合并在  get_mysql5  函数中了，通过声明  global
# def get_mysql5_list(mysql5: DataFrame):
#     mysql5_list = mysql5.rdd.map(lambda row: str(row['tagsId'])).collect()
#     return mysql5_list


# todo 写入 es结果库
def write2es(re: DataFrame, rule_class: rule2class):
    re.write \
        .format("es") \
        .option("es.resource", f"tfec_tbl_result/{rule_class.esType}") \
        .option("es.nodes", f"{rule_class.esNodes}") \
        .option("es.mapping.id", f"userId") \
        .option("es.mapping.name", f"userId:userId,tagsId:tagsId") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()


class rule_base(abc.ABC):
    @abc.abstractmethod
    def get_id(self):
        pass

    @abc.abstractmethod
    def compute(self, spark: SparkSession, es_data_df: DataFrame, mysql5: DataFrame):
        pass


    def execute(self):
        # todo 读取 MySQL 数据
        mysql_df: DataFrame = get_mysql_df()

        # todo 筛选 MySQL 4级  标签数据
        id = self.get_id()
        mysql4: DataFrame = get_mysql4(mysql_df, id)
        # todo 解析 rule 为对象
        rule_class: rule2class = get_rule_dict(mysql4)
        # todo 读取 es数据
        es_data_df: DataFrame = get_es_data(rule_class)
        # todo 筛选 MySQL 5级  标签数据
        mysql5: DataFrame = get_mysql5(mysql_df, id)

        # todo 处理 es数据 和 MySQL5级数据
        es_new: DataFrame = self.compute(spark, es_data_df, mysql5)
        # todo 这样写可以排除 当结果表为空的时候 报错 py4JJavaError
        try:
            # todo 读取 es旧标签数据
            es_old: DataFrame = get_es_old(rule_class)
            # todo 获取 MySQL 5级标签  列表
            # mysql5_list = get_mysql5_list(mysql5)

            # todo 合并 es标签新数据 es旧标签数据
            re: DataFrame = es_new.join(es_old, es_new['userId'] == es_old['userId'], 'left') \
                .select(es_new['userId'],
                        F.udf(merge, StringType())(es_new['tagsId'], es_old['tagsId']).alias('tagsId'))
        except Py4JJavaError:
            re: DataFrame = es_new.select(es_new['userId'].cast('string'), es_new['tagsId'].cast('string'))

        # todo 写入 es结果库
        write2es(re, rule_class)
