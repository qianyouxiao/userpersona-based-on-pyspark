#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

import abc


# 将一个类定义成抽象类,一个类中只要有抽象方法该类必须定义为抽象类，子类实现该方法
class Utils(metaclass=abc.ABCMeta):
    def __init__(self, url, webName):
        self.url = url
        self.webname = webName

    @abc.abstractmethod  # abstract抽象，将一个普通方法定义为一个抽象方法
    def login(self):
        pass
        # print("登录网页")


class BaiduData(Utils):
    def login1(self):
        print("登录 %s 页面" % self.webname)


class Lenovo(Utils):
    def __init__(self, url, webName, userName, password):
        super(Lenovo, self).__init__(url, webName)
        self.userName = userName
        self.password = password

    # def login(self):
    #     print("uname=%s,pwd=%s,成功进入 %s 官网" % (self.userName, self.password, self.webname))
if __name__ == '__main__':
    util = Utils("www.baidu.com", "百度")
    util.login()
    baidu = BaiduData("www.baidu.com", "百度")
    baidu.login()
    # lenovo_data = Lenovo("www.Lenovo.com", "联想", "1761089", "abc123")
    # lenovo_data.login()