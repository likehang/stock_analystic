import psycopg2 as pg
import config as conf
from io import StringIO
from psycopg2 import extras as ex
import re

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
    def execute_sql(sql:str):
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        return

    @staticmethod
    def save_as_db(executesql:str, pdatas:list):
        # cols = pdata.fields
        datalist = []
        count = 1
        for pdata in pdatas:
            while pdata.next():
                datalist.append(tuple(pdata.get_row_data()))
                count += 1
                if count % 3000 == 0:
                    DBUtil._insert_beauty(executesql, datalist)
                    datalist = []
        if len(datalist) > 0:
            DBUtil._insert_beauty(executesql, datalist)
        return None

    @staticmethod
    def io_save_db(schema:str, table:str, pdatas:list):
        f = StringIO()
        f.seek(0)
        size = 0
        table = schema+"."+table
        if len(pdatas) <= 0:
            raise BaseException("缺少数据内容")
        cols = pdatas[0].fields
        print("insert ",table," with ", ",".join(cols))
        for pdata in pdatas:
            size += len(pdata.get_data())
            while pdata.next():
                f.write('\t'.join(pdata.get_row_data())+'\n')
        if size > 0:
            conn = DBUtil.getConnect()
            cur = conn.cursor()
            cur.copy_from(f, table, columns=cols,sep='\t', null='\\N', size=size)
            conn.commit()
        f.close()
        return

    @staticmethod
    def _insert_beauty(executesql:str, datalist:list):
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        try:
            ex.execute_values(cur, executesql, datalist)
        except Exception as e:
            conn.rollback()
            print("single insert")
            for data in datalist:
                value_data = DBUtil.__beauty_sql(data)
                sql = executesql % "("+value_data+")"
                try:
                    cur.execute(sql)
                except Exception as err:
                    print("execute error :", sql, err)
                    conn.rollback()
        conn.commit()

    @staticmethod
    def __beauty_sql(data:tuple):
        pattern = re.compile(r'^[-+]{0,1}\d*(\.\d*)$')
        value_part = []
        for item in data:
            if pattern.match(str(item)):
                value_part.append(str(item))
            else:
                value_part.append("'"+item+"'")
        return ",".join(value_part)