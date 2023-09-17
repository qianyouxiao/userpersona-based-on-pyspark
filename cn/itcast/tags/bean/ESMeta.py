#!/usr/bin/env python
# @desc : ES元数据信息对象
"""
@dataclass 标识class时，代表该class是一个数据对象，可以直接有属性，不需要单独设置 @property @setter
"""
__coding__ = "utf-8"
__author__ = "itcast team"

from dataclasses import dataclass


@dataclass
class ESMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str

    def __init__(self, inType, esNodes, esIndex, esType, selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esIndex = esIndex
        self.esType = esType
        self.selectFields = selectFields

    def fromDictToEsMeta(ruleDict: dict):
        return ESMeta(ruleDict.get("inType", ""),
                      ruleDict.get("esNodes", ""),
                      ruleDict.get("esIndex", ""),
                      ruleDict.get("esType", ""),
                      ruleDict.get("selectFields", ""))
