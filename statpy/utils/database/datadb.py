#!/usr/bin/env python
#encoding: utf-8

import datetime
import MySQLdb
import conf
import logging

class DataDB:
    __db_handler = None;
    __db_type = "MYSQL";  #MYSQL, POSTGRESQL,ORACLE
    __host = None;
    __database = None;
    __user = None;
    __password = None;
    __port = None;
    
    def __init__(self, db_type, host, database, db_user, db_password, port):
        self.__host = host;
        self.__database = database;
        self.__user = db_user;
        self.__password = db_password;
        self.__port = port;
        self.__db_type = db_type;
        self.__init_handler();
    
    def __init_handler(self):
        try:
            if( self.__db_type == "MYSQL" ):
                self.__db_handler = MySQLdb.connect(host=self.__host, user=self.__user, 
                        passwd=self.__password, db=self.__database, port=self.__port);
            else:
                pass;
        except Exception as ex:
            logging.error( "%s - get db connection: %s " % (datetime.datetime.now(), str(ex)));
            return False;
        return True;
    
    def check_and_reconnect_db_connection(self):
        if self.__db_handler is not None:
            cursor = self.__db_handler.cursor();
            try:
                cursor.execute("SELECT NOW()");
            except Exception as ex:
                logging.error( "%s - DB connection check fail, try to reconnect - error: %s" % (datetime.datetime.now(), str(ex)) );
                result = self.__init_handler();
                while result is False:
                    logging.info( "%s - DB reconnect failure, retry after %d seconds" \
                          % (datetime.datetime.now(), conf.USER_STATUS_CONVERT_INTERVAL) );
                    time.sleep(conf.USER_STATUS_CONVERT_INTERVAL);
                    result = self.__init_handler();

                if result is True:
                    logging.info( "%s - DB connection is recovered~" % datetime.datetime.now() );

    def dictFetchAll(cursor):
        """Returns all rows from a cursor as a dict"""
        desc = cursor.description;
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ];
    
    def getDBHandler(self):
        return self.__db_handler;

    def executeSQL(self, sql, values):
        cur = self.__db_handler.cursor();
        try:
            if( not values ):
                cur.execute(sql);
            else:
                cur.execute(sql, values);
            self.__db_handler.commit();
        except Exception as ex:
            logging.error( "sql execute error. %s" % str(ex) );
            self.__db_handler.rollback();
            self.check_and_reconnect_db_connection();
        finally:
            cur.close();