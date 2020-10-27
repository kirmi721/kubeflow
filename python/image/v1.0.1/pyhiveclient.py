#!/usr/bin/python
# -*- coding: UTF-8 -*-
#import pyhs2
from pyhive import hive
import pandas as pd
import time

import findspark
findspark.init('/usr/hdp/2.6.5.0-292/spark2')
from pyspark import SparkContext,SparkConf,HiveContext
sc = SparkContext()
#sc = SparkContext('yarn', 'pyspark')
hc = HiveContext(sc)

from pyspark.sql import SparkSession
import hdfs
client=hdfs.Client("http://10.200.24.219:50070")



current_milli_time = lambda: int(round(time.time() * 1000))

class HiveClient:
    # 初始化
    def __init__(self, db_host, user, password, database, port=10000, authMechanism="LDAP", configuration=None):
        self.conn = hive.connect(host=db_host,
                                  port=port,
                                  auth=authMechanism,
                                  username=user,
                                  password=password,
                                  database=database,
                                  configuration=configuration,
                                  )

    # 查询方法
    def query(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    def execute(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)

    def close(self):
        self.conn.close()


def executeSql(sql):
    #config = {"mapreduce.job.queuename": "default", 'krb_host': 'hiveserve2ip', 'krb_service': 'hive'}
    config = {}
    hive_client = HiveClient(db_host='10.200.24.214', port=10000, user='kduser', password='Dataone@2018', database='default',
                             authMechanism='LDAP', configuration=config)
    print ("exeute sql is ["  + sql +  "]......")
    columns = []  ##空列表
    if sql.lower().find("create") != -1 or sql.lower().find("insert") != -1 or sql.lower().find("truncate") != -1 or sql.lower().find("drop") != -1 :
       result=hive_client.execute(sql)
    else:
       result=hive_client.query(sql)
       for i in range(len(result)):
                #print result[i][0]
           columns.append(result[i][0])
    #print result
    print ("exeute sql is done....")
    return columns

def get_table(tablename):
    #config = {"mapreduce.job.queuename": "default", 'krb_host': 'hiveserve2ip', 'krb_service': 'hive'}
    #hive_client = HiveClient(db_host='10.247.5.143', port=10000, user='kduser', password='Dataone@2018', database='default',
    #                         authMechanism='PLAIN', configuration=config)
    #print  "select * from " + tablename
    sql= "select * from " +  tablename
    #result = hive_client.query(sql)
    #desc_sql=  "desc " + tablename
    #print "get table column query: " + desc_sql
    #columns_result = hive_client.query(desc_sql)

    #columns = []  ##空列表
    #for i in range(len(columns_result)):
    #            if  columns_result[i][0].strip()=='':
    #                    break
    #            else:
    #                    columns.append(columns_result[i][0])
    #print columns
    #df = pd.DataFrame(result, columns=columns)
    df = hc.sql(sql).toPandas()
    return df


def get_table_partition(tablename,partition):
    #config = {"mapreduce.job.queuename": "default", 'krb_host': 'hiveserve2ip', 'krb_service': 'hive'}
    #hive_client = HiveClient(db_host='10.247.5.143', port=10000, user='kduser', password='Dataone@2018', database='default',
    #                         authMechanism='PLAIN', configuration=config)
    sql=  "select * from " + tablename + " where 1=1 and " + partition 
    #print (query sql:  + sql)
    #result = hive_client.query(sql)
    #print result
    #desc_sql=  "desc " + tablename
    #print "get table column query: " + desc_sql 
    #columns_result = hive_client.query(desc_sql)
    
    #columns = []  ##空列表
    #for i in range(len(columns_result)):
    #            if  columns_result[i][0].strip()=='': 
    #            	break
    #		else:
    #            	columns.append(columns_result[i][0])
    #print columns
    #df = pd.DataFrame(result, columns=columns)  
    df = hc.sql(sql).toPandas()
    return df

def write_table(pandas_df,tablename,partition="",mode=""):
    #import findspark
    #findspark.init('/data/spark-2.2.0-bin-hadoop2.7/')
    #from pyspark import SparkContext,SparkConf,HiveContext
    #sc = SparkContext()
    #hc = HiveContext(sc)	
	
    #from pyspark.sql import SparkSession
    timestamp=current_milli_time()
    filePath="/var/data/tmp/pandas_df_"+str(timestamp)+".csv"
    hdfsfilePath="/tmp/pandas_df_"+str(timestamp)+".csv"
    pandas_df.to_csv(filePath,index=False)
    ##上传文件到hdfs 分布式执行需要
    client.upload("/tmp",filePath)
    spark_df=hc.read.csv(hdfsfilePath, header=True)
    if mode != "append":
        mode="overwrite"
        print (mode)  
    if partition == "":
        spark_df.write.format("orc").mode(mode).saveAsTable(tablename)
        refreshSql="REFRESH TABLE " +tablename
        print (refreshSql)
        hc.sql(refreshSql)
    else:
        spark_df.write.partitionBy(partition).format("orc").mode(mode).saveAsTable(tablename)
        refreshSql="REFRESH TABLE " +tablename
        print (refreshSql)
        hc.sql(refreshSql)
    #sc.stop()
