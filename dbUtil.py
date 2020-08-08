import psycopg2 as pg
import config as conf

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
