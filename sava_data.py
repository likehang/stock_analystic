import config as conf
from dbUtil import DBUtil
from stock import Stock
import pandas as pd


class SaveStock():
    @staticmethod
    def get_stock_insert_sql(table_name:str)->str:
        if table_name.endswith("_data"):
            table_name = table_name.replace("_data","")
        return "INSERT INTO {0}.{1} VALUES %s ".format(conf.STOCK_SCHEMA, table_name)

    @staticmethod
    def save_as_csv(file, pdata):
        data_list = []
        while pdata.next():
            data_list.append(pdata.get_row_data())
        result = pd.DataFrame(data_list, columns=pdata.fields)
        result.to_csv(file, encoding='utf-8', index=False)
        return None
