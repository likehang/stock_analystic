import config as conf
from dbUtil import DBUtil
from stock import Stock
import pandas as pd


def get_stock_insert_sql(table_name:str)->str:
    if table_name.endswith("_data"):
        table_name = table_name.replace("_data","")
    return "INSERT INTO {0}.{1} VALUES %s ".format(conf.STOCK_SCHEMA, table_name)

def save_as_csv(file, pdatas):
    data_list = []
    if len(pdatas) > 0 and len(pdatas[0].get_data()) > 0:
        cols = pdatas[0].fields
        for pdata in pdatas:
            while pdata.next():
                data_list.append(pdata.get_row_data())
        result = pd.DataFrame(data_list, columns=cols)
        result.to_csv(file, encoding='utf-8', index=False)
    return None
