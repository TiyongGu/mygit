import os
import traceback
import urllib
from time import sleep

import pymysql
from dbutils.pooled_db import PooledDB
from sqlalchemy import create_engine
import pandas as pd
import pgdb
from urllib.parse import quote_plus as urlquote

from deal_log import get_log
from read_config import read_yaml


class Mysql:
    def __init__(self, pool):
        self.pool = pool
        self.conn = None
        self.cursor = None

    @staticmethod
    def create_pool(db_type, host, database, port, user, pwd):
        create = None
        if isinstance(db_type, str):
            if db_type.lower() == 'mysql':
                create = pymysql
            elif db_type.lower() == 'postgresql':
                create = pgdb
        else:
            create = db_type
        pool = PooledDB(create, mincached=25, host=host, user=user, password=pwd, database=database, port=int(port))
        print(pool.connection())
        return pool

    def _connect_database(self):
        if self.conn:
            self.conn.close()
        self.conn = self.pool.connection()

    def _close_database(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None
        if self.cursor:
            self.cursor.close()
            self.cursor = None

    def read_sql(self, sql: str):
        self._connect_database()
        df = pd.read_sql(sql, self.conn)
        self._close_database()
        return df

    def search_sql(self, sql, args=None, sign=True):
        self._connect_database()
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql, args)
        if sign:
            rows = self.cursor.fetchone()
        else:
            rows = self.cursor.fetchall()
        self._close_database()
        return rows

    def batch_execution(self, sql, tuple_content):
        self._connect_database()
        self.cursor = self.conn.cursor()
        self.cursor.executemany(sql, tuple_content)
        self._close_database()

    def operate_sql(self, db_type, sql):
        self._connect_database()
        self.cursor = self.conn.cursor()
        if db_type.lower() == 'mysql':
            rows = self.cursor.execute(sql)
            self.conn.commit()
            return rows
        elif db_type.lower() == 'postgresql':
            try:
                self.cursor.execute(sql)
                self.conn.commit()
                return True
            except Exception as e:
                return False


class Config_read:
    def __init__(self):
        sign = os.environ
        # sign = {
        #     'ACCESS_KEY_ID': 'A',
        #     'SECRET_ACCESS_KEY': 'A',
        #     'HOST_READ': 'localhost',
        #     'PORT_READ': '5432',
        #     'DATABASE_READ': 'dm',
        #     'USER_READ': 'postgres',
        #     'PASSWORD_READ': '123456',
        #     'SIGN': True
        # }
        try:
            if read_yaml() is not None:
                self.db_host = read_yaml()['Config']['HOST']
                self.db_name = read_yaml()['Config']['DATABASE']
                self.db_port = read_yaml()['Config']['PORT']
                self.db_user = read_yaml()['Config']['USER']
                self.db_passwd = read_yaml()['Config']['PASSWORD']
                self.db_type = read_yaml()['Config']['db_type']
            elif sign.get('SIGN') is not None:
                self.ak = sign.get('ACCESS_KEY_ID')
                self.sk = sign.get('SECRET_ACCESS_KEY')
                # self.flask_host = sign.get('FLASK_HOST')
                self.db_host = sign.get('HOST_READ')
                self.db_port = sign.get('PORT_READ')
                self.db_name = sign.get('DATABASE_READ')
                self.db_user = sign.get('USER_READ')
                self.db_passwd = sign.get('PASSWORD_READ')
                self.db_type = 'postgresql'
            else:
                raise RuntimeError('没有配置文件或没有配置环境变量')
        except Exception as e:
            get_log().error(e)
            print(e)
        finally:
            pass


def create_db_engine():
    try:
        if Config_read().db_type.lower() == 'mysql':
            head = 'mysql+pymysql'
        elif config_read().db_type.lower() == 'postgresql':
            head = 'postgresql+pygresql'
        address = head + f'://{Config_read().db_user}:{urllib.parse.quote_plus(Config_read().db_passwd)}@' \
                         f'{Config_read().db_host}:{Config_read().db_port}/{Config_read().db_name}'
        engine = create_engine(address)
        return engine
    except Exception:
        return 'creat engine fail'
    finally:
        pass


def df_to_db(data, table_name, if_exists='append'):
    # 连接基线数据库需要添加的代码
    from sqlalchemy.dialects.postgresql.base import PGDialect
    PGDialect._get_server_version_info = lambda *args: (9, 2)
    try:
        temp = True
        signin_info = 'postgresql+pygresql://' + Config_read().db_user + ':' + '%s@' % urlquote(
            Config_read().db_passwd) + Config_read().db_host + \
                      ':' + Config_read().db_port + '/' + Config_read().db_name + '?client_encoding=utf8'
        engine = create_engine(signin_info)
        pd.io.sql.to_sql(data, table_name, con=engine, if_exists=if_exists, schema='dm', index=False)
    except Exception as e:
        a = traceback.format_exc()
        print(a)
        temp = False
    finally:
        pass
    return temp


def db_conn():
    try:
        pool = Mysql.create_pool(db_type=Config_read().db_type,
                                 host=Config_read().db_host,
                                 database=Config_read().db_name,
                                 port=Config_read().db_port,
                                 user=Config_read().db_user,
                                 pwd=Config_read().db_passwd)
        conn = Mysql(pool)
        return conn
    except Exception:
        return 'creat connection fail'
    finally:
        pass


def trunate_table(tablename):
    sql = '''
    truncate table dm."%s"
    ''' % tablename
    conn = db_conn()
    flag = conn.operate_sql('postgresql', sql)
    query_sql = '''
    select * from dm."%s"
    ''' % tablename
    df = conn.read_sql(query_sql)
    if flag and df.empty:
        return True
    else:
        return False

# if __name__ == "__main__":
#     print(trunate_table('dm_tobt_v2t_dyn_normal_release_time_ex'))
