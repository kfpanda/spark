#!/usr/bin/env python
#encoding: utf-8

import datetime
import redis
import conf
import logging

class MemDB:
    __db_handler = None;
    __db_type = "REDIS";  #REDIS
    __host = None;
    __port = None;
    
    def __init__(self, db_type, host, port):
        self.__host = host;
        self.__port = port;
        self.__db_type = db_type;
        self.__init_handler();
    
    def __init_handler(self):
        try:
            if( self.__db_type == "REDIS" ):
                self.__db_handler = redis.StrictRedis(host=self.__host, port=self.__port, db=0);
        except Exception as e:
            self.__db_handler = None;
            logging.error( "%s - init memdb handler: %s " % (datetime.datetime.now(), str(e)));
            return False;
        return True;
    
    def getDBHandler(self):
        return self.__db_handler;