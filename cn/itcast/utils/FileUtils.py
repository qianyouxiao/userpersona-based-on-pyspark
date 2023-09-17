#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

import time
import gzip


# 写入数据到文件中
def writeDataToFile(path='./', prefix=None, line=""):
    with open(f'{path}/{prefix}-{time.strftime("%Y%m%d", time.localtime())}', 'a', encoding="utf-8") as fileHandler:
        fileHandler.write(line + "\n")
    fileHandler.close()


# 写入数据到压缩文件中
def writeDataToGZFile(path='./', prefix=None, line=""):
    with gzip.open(f'{path}/{prefix}-{time.strftime("%Y%m%d", time.localtime())}.gz', 'at', encoding="utf-8") as fileHandler:
        fileHandler.write(line + "\n")
    fileHandler.close()


# 读取文件数据
def readDataFromFile(path='./', prefix=None):
    with open(f'{path}/{prefix}-{time.strftime("%Y%m%d", time.localtime())}', 'r', encoding="utf-8") as fileHandler:
        resultList = fileHandler.readlines()
    fileHandler.close()
    return resultList


# 读取压缩文件数据
def readDataFromGZFile(path='./', prefix=None):
    with gzip.open(f'{path}/{prefix}-{time.strftime("%Y%m%d", time.localtime())}.gz', 'rt', encoding="utf-8") as fileHandler:
        resultList = fileHandler.readlines()
    fileHandler.close()
    return resultList
