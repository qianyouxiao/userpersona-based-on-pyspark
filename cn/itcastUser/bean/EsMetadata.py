#!/usr/bin/env python
__coding__ = "utf-8"
__author__ = "itcast team"

from dataclasses import dataclass

"""
-------------------------------------------------
   Description :	TODO：用于存放从MySQL中读取到四级标签的rule信息:语法糖
   SourceFile  :	EsMetaData
   Author      :	Frank
-------------------------------------------------
"""


@dataclass
class EsMetaData(object):
    # 属性
    inType: str = None
    esNodes: str = None
    esIndex: str = None
    esType: str = None
    selectFields: str = None

    # 方法
    # def __init__(self, inType, esNodes, esIndex, esType, selectFields):
    #     self.inType = inType
    #     self.esNodes = esNodes
    #     self.esIndex = esIndex
    #     self.esType = esType
    #     self.selectFields = selectFields

    def getEsMetaFromDict(input: dict):
        """
            用于根据MySQL中读取到的rule规则的字典，转换成一个EsMetaData类的对象
        :return: EsMetaData类的对象
        """
        return EsMetaData(
            input.get("inType"),
            input.get("esNodes"),
            input.get("esIndex"),
            input.get("esType"),
            input.get("selectFields")
        )
