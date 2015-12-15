#!/usr/bin/env python
#encoding: utf-8
"""配置文件"""

import logging

ROOT_LOG_PATH = "/mnt/kfpanda";

DEFAULT_DB_SERVER = "192.168.10.183"
DEFAULT_DB_NAME = 'twifi_dev'
DEFAULT_DB_USER = 'twifi'
DEFAULT_DB_PASSWORD = 'twifi123$'


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/spark-stat.log',
                filemode='w');