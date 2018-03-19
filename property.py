import os
import sys
import subprocess
import json
import base64
import inspect
import boto3
import redis

from hadoop import Hdfs
from sftp import Sftp

sys.path.append(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
from hive import Hive
from db import Database
from ssh_client.remote import RemoteHostClient


class Property:

    def __init__(self,user=None,passwd=None,host=None,schema=None,project=None):
        self.__myProperty = {}
        mysql_user = (os.environ.get('P_MYSQL_USER')) or user or'property'
        mysql_passwd = (os.environ.get('P_MYSQL_PASSWD')) or passwd
        mysql_host = (os.environ.get('P_MYSQL_HOST')) or host or 'qasql.staging.dw.sc.gwallet.com'
        mysql_schema =  schema or 'qa_property'
        mysql_project = project or (os.environ.get('PROJECT')) or 'default'
        conn = Database(host=mysql_host,user=mysql_user,password=mysql_passwd,schema=mysql_schema)
        query='Select p_name,p_key,p_value,p_type,additional from qa_property.property p ' \
              'JOIN qa_property.project_property_map ppm on (p.map_id = ppm.property_map_id) ' \
              'JOIN  qa_property.project pr on (pr.project_map_id = ppm.project_map_id) '\
              'where pr.project_name like "'+mysql_project+'" order by p.map_id'
        mysql_res = conn.execute(query)
        for p_name,p_key,p_value,p_type,additional in mysql_res:
            self.__myProperty[p_name+p_type] = [p_name,p_key,p_value,p_type,additional]


    def getHiveConn(self,my_database=None,tag='hive'):
        conn=None
        for property in  self._Property__myProperty:
            if  self._Property__myProperty[property][3] == tag:
                conn = Hive(host=self._Property__myProperty[property][0]
                            ,port=int(self._Property__myProperty[property][2])
                            ,user=self._Property__myProperty[property][1]
                            ,password='Pass'
                            ,database=my_database)
        return conn

    def getHDFSConn(self,tag='hdfs'):
        conn=None
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                conn = Hdfs(url=self._Property__myProperty[property][0]
                            ,user=self._Property__myProperty[property][1]
                            )
        return conn

    def getSftpConn(self,tag='sftp'):
        conn=None
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                conn = Sftp(host=self._Property__myProperty[property][0]
                           ,user=self._Property__myProperty[property][1]
                           ,password=self._Property__myProperty[property][2])
        return conn

    def getAWSS3Conn(self,tag='aws_s2'):
        conn=None
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                conn = boto3.client('s3',
                                    aws_access_key_id=self._Property__myProperty[property][1],
                                    aws_secret_access_key=self._Property__myProperty[property][2])
        return conn

    def getMysqlConn(self,my_schema=None,tag='mysql'):
        conn=None
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                if not my_schema:
                    if self._Property__myProperty[property][4]:
                        my_schema = self._Property__myProperty[property][4]

                conn = Database(host=self._Property__myProperty[property][0]
                                ,user=self._Property__myProperty[property][1]
                                ,password=self._Property__myProperty[property][2]
                                ,schema=my_schema)
        return conn

    def getRedisConn(self,my_database=None,tag='redis'):
        conn=None
        for property in  self._Property__myProperty:
            if  self._Property__myProperty[property][3] == tag:
                conn = redis.Redis(host=self._Property__myProperty[property][0],
                                  port=self._Property__myProperty[property][1],
                                  db=self._Property__myProperty[property][2])
        return conn

    def getSSHCon(self,tag='ssh'):
        conn=None
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                conn = RemoteHostClient(self._Property__myProperty[property][0],self._Property__myProperty[property][1],None,None,None)
        return conn

    def getSSHUser(self,tag='ssh'):
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                return self._Property__myProperty[property][1]
        return None

    def getSSHHost(self,tag='ssh'):
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                return self._Property__myProperty[property][0]
        return None

    def getProverty(self,tag=None):
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                return self._Property__myProperty[property][0]
        return None

    def getPropertyValue(self,tag=None):
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                return self._Property__myProperty[property][2]

    def getPropertyKey(self,tag):
        for property in  self._Property__myProperty:
            if self._Property__myProperty[property][3] == tag:
                return self._Property__myProperty[property][1]


    def getPropertyFromCSV(self,name):
        res = []
        for property in  self.__myProperty:
            if self.__myProperty[property][3] == 'csvfile' and name == self._Property__myProperty[property][0]:
                ssh = subprocess.Popen(['ssh'
                                           ,self.__myProperty[property][1]
                                           ,'cat'
                                           ,self.__myProperty[property][2]]
                                       ,stdout=subprocess.PIPE)
                for line in ssh.stdout:
                    line = line.replace('\n','').split(',')
                    res.append(line)
        return res

    def getAllEnvironmentVariable(self):
        res={}
        for key in os.environ.keys():
            res[key]=os.environ[key]
        return res

    def getProprtyFromJson(self,name):
        res=[]
        for property in  self.__myProperty:
            if self.__myProperty[property][3] == 'jsonfile' and name == property:
                ssh = subprocess.Popen(['ssh'
                                           ,self.__myProperty[property][1]
                                           ,'cat'
                                           ,self.__myProperty[property][2]]
                                       ,stdout=subprocess.PIPE)
                for line in ssh.stdout:
                    line = json.loads(line.replace('\n',''))
                    res.append(line)
        return res


