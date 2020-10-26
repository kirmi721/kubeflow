from kazoo.client import KazooClient
from pyhive import hive
import random
import pandas as pd
import os
import time
import pymysql
import datetime
import json

file_name = './zkhiveconnection.txt'
fo = open(file_name,'r')
parameter = fo.readlines()
fo.close()
parameter = json.loads(parameter[0])


class zkhiveConnection():
    def __init__(self,zkhost=parameter['zkhost'],hive_hosts=parameter['hive_hosts'],znodeName=parameter['znodeName'],
                 connectionName = os.getcwd(),data_port = parameter['data_port'],
                 data_username = parameter['data_username'],
                 data_database = parameter['data_database'],
                 data_password = parameter['data_password'],
                 user = parameter['user'],password = parameter['password'],configuration=parameter['configuration'],
                 db = parameter['db'],host = parameter['host'],charset = parameter['charset']):     
        """
            默认参数配置初始化
            详细参数说明:   zkhost : zk地址
                            hive_hosts : hive链接地址
                            znodeName : 节点名称
                            connectionName : 调用用户名称
                            data_port : hive端口
                            data_username : hive用户名称
                            data_database : hive数据库名称(默认xfxb)
                            data_password : hive链接密码
                            user : mysql用户名
                            password : mysql用户密码
                            db : mysql数据库名
                            host : mysqlhost地址
        """
        self.zkhost = zkhost
        self.hive_hosts = hive_hosts
        self.znodeName = znodeName
        self.connectionName = connectionName
        self.data_port = data_port
        self.data_username = data_username
        self.data_database = data_database
        self.data_password = data_password
        self.user = user
        self.configuration = configuration
        self.password = password
        self.db = db
        self.host = host
        self.charset = charset

    def get_servers_list(self):
        
        """
            根据zk获取的hive链接状态,返回可用hive地址
        """
        
        try:
            zkClient = KazooClient(hosts=self.zkhost)
            zkClient.start()
            result = zkClient.get_children(self.znodeName)
            zkClient.stop()
            
            ip = ['.'.join(i.split(';')[0].split(':')[0].split('-')[-2:]) for i in result]
            host_name = []
            for i in ip:
                for j in self.hive_hosts:
                    if j.find(i) != -1:
                        self.host_name.append(j)
            server_list = host_name
        except:
            server_list =  self.hive_hosts
            
        return server_list
    

    def get_server(self):
        """
            返回动态分配的hive链接地址
        """
        server_list = self.get_servers_list()
        return random.choice(server_list)
    
    
    
    def hiveConnection(self,server_url):
        """
            返回hive链接接口
            参数解释：server_url : 动态分配后的hive地址
        """
        
        conn = hive.Connection(host=server_url,port=self.data_port,username=self.data_username,
                               database=self.data_database,
                               password=self.data_password,
                               auth='LDAP',configuration=self.configuration)
        return conn
    

    def close(self,conn):
        """
            关闭hive链接接口,同事插入整体调用时间和动态分配后的server_url地址
        """
        try:
            conn.close()
        except:
            pass
    
    
    def statusRecord(self,connectionTime,closeTime,connectionName,server_url,data_database,sql):
        """
            保存调用过程状态到mysql
            参数解释：connectionTime：调用起始时间,
                      closeTime：结果关闭时间,
                      connectionName：调用用户名称,
                      server_url：动态分配后的hive地址,
                      data_database：hive数据库名称(默认xfxb),
                      sql：查询语句          
        """
        con = pymysql.connect(user=self.user,password=self.password,db=self.db,host=self.host,charset=self.charset)
        cursor = con.cursor()
        sql = """insert into statusRecord values('{}','{}','{}','{}','{}')""".format(connectionTime,closeTime,connectionName,server_url,data_database)
        try:
            cursor.execute(sql)
            con.commit()
            cursor.close()
            con.close()
        except:
            cursor.close()
            con.close()
    
    
    def getDataFrame(self,sql):
        """
            根据传入sql语句，返回dataFrame数据
        """
        server_url = self.get_server()
        connectionTime = str(datetime.datetime.now())
        conn = self.hiveConnection(server_url)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except:
            self.close(conn)
            print('SQL statement error No table data return')
            return None

        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        self.close(conn)
        closeTime = str(datetime.datetime.now())
        self.statusRecord(connectionTime=connectionTime,closeTime=closeTime,connectionName=self.connectionName,
                          server_url=server_url,data_database=self.data_database,sql=sql)
        return df
    
    def insertTable(self,sql):
        """
            根据传入sql语句，插入hive结果表
        """
        server_url = self.get_server()
        connectionTime = str(datetime.datetime.now())
        conn = self.hiveConnection(server_url)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except:
            self.close(conn)
            return 'SQL statement error No table data insert'
        cursor.close()
        self.close(conn)
        closeTime = str(datetime.datetime.now())
        self.statusRecord(connectionTime=connectionTime,closeTime=closeTime,connectionName=self.connectionName,
                          server_url=server_url,data_database=self.data_database,sql=sql)

