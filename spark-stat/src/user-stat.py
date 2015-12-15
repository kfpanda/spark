#!/usr/bin/env python
#encoding: utf-8
"""用户日志统计"""

import statfile
from pyspark import SparkContext
from operator import add

hour_arr = ["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18",,"19","20","21","22","23","24"];
def shu_hour_newuser_stat_map(line):
    """商户 新用户认证记录统计"""
	elems = line.split("|");
	return [("auth", 1)];

sc = SparkContext("local", "Simple App");
log_file = statfile.get_hour_log_file("user", "ems_ems_wlan_report_fatap_user", "09");
user_auth_logs = sc.wholeTextFiles(log_file);

newuser_stat = user_auth_logs.map(shu_hour_newuser_stat_map);
newuser_stat = newuser_stat.reduceByKey(add);

print(newuser_stat.collect());
