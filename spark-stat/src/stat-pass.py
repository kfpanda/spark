#!/usr/bin/env python
#encoding: utf-8

from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "Simple App");
#portal_nginx_logs = sc.textFile("hdfs://172.30.1.232:9000/logs/nginx/portal/*.log");
#aa = sc.textFile("/mnt/kfpanda/portal/20150417/access_portal_page_push_log_2015041709.log");

#print(portal_nginx_logs.count());

def type_map(file_tuple):
	lines = file_tuple[1].split("\n");
	auth_count = 0;
	login_count = 0;
	for line in lines :
		elems = line.split("|");
		if( len(elems) > 3 and elems[3] == "auth" ):
			auth_count += 1;
		elif( len(elems) > 3 and elems[3] == "login"):
			login_count += 1;
	return [("auth", auth_count), ("login", login_count)];

portal_push_logs = sc.wholeTextFiles("/mnt/kfpanda/portal/20150417/");
auth_total = portal_push_logs.map(type_map);
auth_total = auth_total.reduceByKey(add);

print(auth_total.collect());
