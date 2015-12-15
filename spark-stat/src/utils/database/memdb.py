#!/usr/bin/env python
#encoding: utf-8

import datetime
import redis
import conf
import logging

memdb_handler = None;

def init_memdb_handler():
    global memdb_handler_pool;
    global memdb_handler;
    try:

        if memdb_handler is None:
            memdb_handler = redis.StrictRedis(host=conf.DEFAULT_CACHE_SERVER, port=conf.DEFAULT_CACHE_PORT, db=0);
    except Exception as e:
        memdb_handler = None;
        logging.error( "%s - init cache handler: %s " % (datetime.datetime.now(), str(e)));
        return False;
    return True;

