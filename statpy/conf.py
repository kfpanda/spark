#!/usr/bin/env python
#encoding: utf-8
"""配置文件"""

import logging

ROOT_LOG_PATH = "/service/statdata/logs";
ROOT_TEMP_PATH = "/service/statdata/stat_temp"
DIR_PREFIX = "file://"

DEFAULT_DB_SERVER = "192.168.10.138"
DEFAULT_DB_PORT = 3306
DEFAULT_DB_NAME = 'zmstatalpha'
DEFAULT_DB_USER = 'twifi'
DEFAULT_DB_PASSWORD = 'twifi123$'

DEFAULT_MEMDB_SERVER = "192.168.10.137"
DEFAULT_MEMDB_PORT = 6379

DB_STAT_DEL_AT_FIRST = True

logging.basicConfig(level=logging.ERROR,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/spark-stat.log',
                filemode='w');
