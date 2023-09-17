#!/usr/bin/env python
# @desc : 加载并读取数据库连接信息工具类
__coding__ = "utf-8"
__author__ = "itcast team"

# python moudle:读取文件功能：根据配置文件信息的key，读取value
# 开发环境、测试环境、预生产环境、生产环境
# configparser作用是：把一个配置文件中selection和key=value信息组织起来， 可以根据selection和key得到value
import configparser

# load and read config.ini
import os.path

config = configparser.ConfigParser()
# 读取resource目录下配置文件信息 ： 配置了oracle、hive、sparksql数据库连接信息
# windows文件路径
config.read('F:\\ProgramCJ\\InsuranceUserProfileVersion2\\cn\\itcast\\resource\\config.ini')

# linux 文件目录
# config.read('/root/InsuranceUserProfile/config.ini')


# 根据key获得value
def getProperty(section, key):
    return config.get(section, key)


# 根据key获得oracle数据库连接的配置信息
def getMysqlConfig(key):
    return config.get('MysqlConn', key)


# 根据key获得spark连接hive数据库的配置信息
def getKafkaConfig(key):
    return config.get('KafkaConn', key)
