#!/usr/bin/env python
#encoding: utf-8

import time
import conf

def get_log_file_path(log_name):
    """获得一个日志文件目录。
    """
    today_time = time.strftime("%Y%m%d");
    dir_path = ROOT_LOG_PATH + "/" + log_name + "/" + today_time;
    return dir_path;

def get_hour_log_file(log_name, file_name, hour_str):
    """获得一个日志文件，该日志文件以小时切割
    """
    hour_time = time.strftime("%Y%m%d") + hour_str;
    l_name = get_log_file_path(log_name);
    file_path = l_name + "/" + file_name + "_" + hour_time + ".log";
    return file_path;