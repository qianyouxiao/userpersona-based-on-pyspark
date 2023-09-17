#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

# -*- coding: utf-8 -*-
# python连接hdfs,修改host文件，把ip和host写入
import contextlib
import pyhdfs


class HdfsUtil(object):
    def __init__(self, hosts='up01:50070', user_name='root'):
        self.hosts = hosts
        self.user_name = user_name
        self.fs = pyhdfs.HdfsClient(hosts=self.hosts, user_name=self.user_name)

    def listdir(self, dir_name):
        return self.fs.listdir(dir_name)

    def copy_from_local(self, local_file, hdfs_file):
        return self.fs.copy_from_local(local_file, hdfs_file)

    def copy_to_local(self, hdfs_file, local_file):
        return self.fs.copy_to_local(hdfs_file, local_file)

    def delete(self, hdfs_file, recursive=True):
        return self.fs.delete(hdfs_file, recursive=recursive)

    def exists(self, hdfs_file):
        return self.fs.exists(hdfs_file)

    def get_fs(self):
        return self.fs

    def create(self, hdfs_file):
        return self.fs.create(hdfs_file, ''.encode('utf8'))

    def read(self, hdfs_file):
        result = []
        with contextlib.closing(self.fs.open(hdfs_file)) as f:
            line = f.readline()
            while line:
                result.append(str(line, encoding='utf8'))
                line = f.readline()
        return ''.join(result)

    def write(self, hdfs_file, data: bytes):
        if not self.exists(hdfs_file):
            self.create(hdfs_file)
        self.fs.append(hdfs_file, data)



if __name__ == '__main__':
    util = HdfsUtil()
    print(util.exists("/sparklog/local-1656041524900.lz4"))#True
    print(util.listdir("/sparklog"))
    #['app-20220528193244-0000.lz4', 'app-20220529085931-0000.lz4', 'app-20220529092230-0001.lz4', 'app-20220529092249-0002.lz4', 'app-20220529103217-0003.lz4', 'app-20220529103238-0004.lz4', 'app-20220529111324-0005.lz4', 'app-20220529111338-0006.lz4', 'app-20220529111618-0007.lz4', 'application_1653785807196_0001.lz4', 'application_1653920914178_0009.lz4', 'application_1653920914178_0011.lz4', 'application_1654258987861_0015.lz4', 'application_1654258987861_0016.lz4', 'application_1654258987861_0017.lz4', 'application_1654258987861_0018.lz4', 'application_1655195781382_0021.lz4.inprogress', 'application_1655195781382_0022.lz4', 'application_1655993235530_0001.lz4', 'application_1655993235530_0002.lz4', 'application_1655993235530_0003.lz4', 'application_1655993235530_0004.lz4', 'local-1653785829867.lz4', 'local-1653785888447.lz4', 'local-1653786733447.lz4', 'local-1653787085452.lz4', 'local-1653791474788.lz4', 'local-1653791487677.lz4', 'local-1653793955773.lz4', 'local-1653794143992.lz4', 'local-1653796790470.lz4', 'local-1653796957274.lz4', 'local-1653797003338.lz4', 'local-1653797029859.lz4', 'local-1653797045468.lz4', 'local-1653797106922.lz4', 'local-1653808255777.lz4', 'local-1653808660050.lz4', 'local-1653818587390.lz4.inprogress', 'local-1653825575967.lz4', 'local-1653826440718.lz4', 'local-1653826557496.lz4', 'local-1653826780705.lz4', 'local-1653826865039.lz4', 'local-1653827532482.lz4', 'local-1653827564607.lz4', 'local-1654268749516.lz4', 'local-1654337983613.lz4', 'local-1655955733979.lz4', 'local-1655955983026.lz4', 'local-1655957377410.lz4', 'local-1655957562427.lz4', 'local-1655957851947.lz4', 'local-1655957858792.lz4', 'local-1655957877707.lz4', 'local-1655957877920.lz4', 'local-1655957877997.lz4', 'local-1655957878000.lz4', 'local-1656041450414.lz4', 'local-1656041500284.lz4', 'local-1656041506877.lz4', 'local-1656041524105.lz4', 'local-1656041524325.lz4', 'local-1656041524900.lz4', 'local-1656041593046.lz4', 'local-1656041599882.lz4', 'local-1656041615020.lz4', 'local-1656041615131.lz4', 'local-1656041615164.lz4', 'local-1656041615550.lz4']

