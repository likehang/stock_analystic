import psycopg2 as pg
import config as conf
from io import StringIO
from psycopg2 import extras as ex

conn = None
class DBUtil():
    @staticmethod
    def getConnect():
        global conn
        if conn == None:
            conn = pg.connect(
                database=conf.PGSQL_DB,
                user=conf.PGSQL_USER,
                password=conf.PGSQL_PW,
                host=conf.PGSQL_HOST,
                port=conf.PGSQL_PORT,
                sslmode = 'disable'
            )
        return conn

    @staticmethod
    def executesql(sql:str):
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

    @staticmethod
    def save_as_db(executesql:str, pdatas:list):
        # cols = pdata.fields
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        datalist = []
        for pdata in pdatas:
            while pdata.next():
                datalist.append(tuple(pdata.get_row_data()))
        ex.execute_values(cur, executesql, datalist)
        conn.commit()
        return None

    @staticmethod
    def io_save_db(table:str, pdatas:list):
        f = StringIO()
        f.seek(0)
        size = 0
        if len(pdatas) <= 0:
            raise "缺少数据内容"
        cols = pdatas[0].fields
        for pdata in pdatas:
            size += len(pdata)
            while pdata.next():
                f.write('\t'.join(pdata.get_row_data())+'\n')
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        cur.copy_from(f, table, columns=cols,sep='\t', null='\\N', size=size)
        conn.commit()
        return
