#!/usr/bin/env python
#encoding: utf-8

from pyspark import SparkContext, SparkConf
from utils.database.datadb import DataDB
import datetime
import conf
from operator import add
import urlparse
import json
import re
import logging


stat_dir = conf.ROOT_LOG_PATH
yesterday = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%Y%m%d")


##将返回的list转化为map
def convert_list(list):
    dict = {}
    for obj in list:
        dict[obj[0]] = obj[1]
    return dict


##主方法根据sql中的统计配置读取相应的文件夹中的日志文件生成统计记录存入数据库
def main(sc, stat_configs, way):
    for data in stat_configs:
        ##文件夹的命名按照/service/log/[project_code]/[label_code]/[日期]/[host_name].log的形式保存
        file_dir = stat_dir + "/" + data["code"] + "/" + data["labelcode"] + "/" + yesterday + "/*.log"
        scid = data["id"]
        try:
            nginx_logs = sc.wholeTextFiles(file_dir)
            ##返回包含pv uv click staytime lostnum的map
            row = convert_list(nginx_logs.flatMap(eval(way)).reduceByKey(add).collect())
            row['scid'] = int(scid)
            ##生成相应的插入语句并执行
            sql = _build_insert_stat_sql_values(row, yesterday)
            db.executeSQL(sql,None)
        except Exception, e:
            logging.error("%s - DB stat insert execution failure %s" % (datetime.datetime.now(), e))

##分析日志文件并返回页面埋点统计结果
def pv_stat(file_tuple):
    ##日志文件中每行文本
    lines = file_tuple[1].split("\n")
    pv = 0
    uv = 0
    ts = 0
    tp = 0
    tp_time = 0
    click = 0

    for line in lines:
        if (len(line) > 0):
            line = line.encode("utf-8")
            ##每行中的有效访问链接
            elems = re.findall(
                "([^ ]*)\\s+-\\s+-\\s+\\[([^\]]*)\\]\\s+\"([^ ]*)\\s+(.*?)\\s+([^ ]*)\"\\s+(-|[0-9]*)\\s+(-|[0-9]*)\\s+(-|[0-9]*)\\s+\"(.+?|-)\"\\s+\"(.+?|-)\"\\s+(.*)"
                , line)
            url = elems[0][3]
            url_param = urlparse.parse_qs((urlparse.urlparse(url)).query, True)
            if url_param.has_key("t"):
                if url_param["t"] == ['p']:
                    pv += 1
                elif url_param["t"] == ['u']:
                    uv += 1
                elif url_param["t"] == ["click"]:
                    click += 1
                elif url_param["t"] == ["ts"]:
                    ts += 1
                elif url_param["t"] == ["tp"]:
                    tp += 1
                    tp_time += float(url_param["tp"][0])
     
    return [('pv', pv), ('uv', uv), ('lostnum', ts), ('click', click), ("staytime", (0 if tp==0 else tp_time/tp) )]


##插入语句头
def _build_insert_stat_sql_header():
    return "insert into statinfo (createtime, statdate, pv, uv, click, staytime, lostnum, scid) values"


##生成插入语句
def _build_insert_stat_sql_values(page_stat, stat_day):
    sql = _build_insert_stat_sql_header() + "(UNIX_TIMESTAMP( CURRENT_TIMESTAMP()),UNIX_TIMESTAMP('%s'),%d,%d,%d,%d,%d,%d)" \
                                            % ( stat_day, page_stat['pv'], page_stat['uv'], page_stat['click'],
                                                int(page_stat['staytime']*1000), page_stat['lostnum'], page_stat['scid'])
    return sql


#查询结果存数组
def dictFetchAll(cursor):
    """Returns all rows from a cursor as a dict"""
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]


##查询统计配置语句
def _build_query_stat_conf_sql():
    return "select s.id,s.labelcode,p.`code` from statconf s left join project p on s.pid = p.id where s.state>= 0"


##执行查询语句并返回结果Map
def _get_stat_conf(sql):
    c = db.getDBHandler().cursor()
    c.execute(sql)
    return dictFetchAll(c)


if __name__ == "__main__":
    db = DataDB("MYSQL", conf.DEFAULT_DB_SERVER, conf.DEFAULT_DB_NAME,
                conf.DEFAULT_DB_USER, conf.DEFAULT_DB_PASSWORD, conf.DEFAULT_DB_PORT)
    configs = _get_stat_conf(_build_query_stat_conf_sql())
    conf = SparkConf().setAppName("page_stat")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    way = "pv_stat"
    main(sc, configs, way)
