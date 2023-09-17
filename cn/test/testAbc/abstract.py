#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

class Utils:
    def __init__(self,url,webName):
        self.url=url
        self.webname=webName
    def login(self):
        print("登录网页")

class BaiduData(Utils):
    def login(self):
        print("登录Baidu页面")

class LenovoData(Utils):
    def __init__(self,url,webName,userName,password):
        super(LenovoData,self).__init__(url,webName)
        self.userName=userName
        self.password=password
    def login(self):
        print("uname=%s,pwd=%s,成功进入 %s 官网"%(self.userName,self.password,self.webname))

baidu=BaiduData("www.baidu.com","百度")
baidu.login()
util=Utils("www.baidu.com","百度")
util.login()
lenovo_data = LenovoData("www.Lenovo.com", "联想", "1761089", "abc123")
lenovo_data.login()