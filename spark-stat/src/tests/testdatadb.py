#!/usr/bin/env python
#encoding: utf-8

import sys
sys.path.append(".");
import conf
import unittest
from utils.database.datadb import DataDB

class datadbTest(unittest.TestCase):
    
    __datadb = None;
    ##初始化工作
    def setUp(self):
        self.__datadb = DataDB("MYSQL", conf.DEFAULT_DB_SERVER, conf.DEFAULT_DB_NAME,
                conf.DEFAULT_DB_USER, conf.DEFAULT_DB_PASSWORD, 3306);
    
    #退出清理工作
    def tearDown(self):
        pass;
    
    #具体的测试用例，一定要以test开头
    def testinsert(self):
        pass;

    def testselect(self):
        self.__datadb.executeSQL("SELECT * FROM account WHERE username='superadmin';");

if __name__ =='__main__':
    unittest.main();
