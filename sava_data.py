import config as conf
from dbUtil import DBUtil
from stock import Stock
import pandas as pd
from io import StringIO
from psycopg2 import extras as ex

class SaveStock():
    function_to_table = {
        "dividend_data":"dividend",
        "history_k_data":"history_k",
        "stock_industry":"stock_industry",
        "all_stock":"all_stocks",
        "sz50_stocks":"sz50_stocks",
        "hs300_stocks":"hs300_stocks",
        "zz500_stocks":"zz500_stocks",
        "trade_dates":"trade_dates",
        "adjust_factor":"adjust_factor",
        "quarter_profit_data":"quarter_profit",
        "quarter_growth_data":"quarter_growth",
        "query_balance_data":"query_balance",
        "quarter_dupont_data":"quarter_dupont",
        "quarter_cash_flow_data":"quarter_cash_flow",
        "quarter_operation_data":"quarter_operation",
        "quarter_performance_express_report":"quarter_performance_express_report",
        "quarter_forcast_report":"quarter_forcast_report",
        "normal_deposit_rate_data":"normal_deposit_rate",
        "normal_loan_rate_data":"normal_loan_rate",
        "normal_required_reserve_ratio_data":"normal_required_reserve_ratio",
        "normal_money_supply_data_month":"normal_money_supply_month",
        "normal_money_supply_data_year":"normal_money_supply_year",
        "normal_shibor_data":"normal_shibor"
    }
    function_to_sql = {
        # "dividend_data":"INSERT INTO data.dividend VALUES %s ",
        # "history_k_data":"INSERT INTO data.history_k VALUES %s ",
        #  "stock_industry":"INSERT INTO data.stock_industry VALUES %s ",
        #  "all_stock":"INSERT INTO data.all_stock VALUES %s ",
        #  "sz50_stocks":"INSERT INTO data.sz50_stocks VALUES %s ",
        #  "hs300_stocks":"INSERT INTO data.hs300_stocks VALUES %s ",
        #  "zz500_stocks":"INSERT INTO data.zz500_stocks VALUES %s ",
        #  "trade_dates":"INSERT INTO data.trade_dates VALUES %s ",
        #  "adjust_factor":"INSERT INTO data.adjust_factor VALUES %s ",
        "quarter_profit_data":"INSERT INTO data.quarter_profit VALUES %s ",
        "quarter_growth_data":"INSERT INTO data.quarter_growth VALUES %s ",
        "query_balance_data":"INSERT INTO data.query_balance VALUES %s ",
        "quarter_dupont_data":"INSERT INTO data.quarter_dupont VALUES %s ",
        "quarter_cash_flow_data":"INSERT INTO data.quarter_cash_flow VALUES %s ",
        "quarter_operation_data":"INSERT INTO data.quarter_operation VALUES %s ",
        "quarter_performance_express_report":"INSERT INTO data.quarter_performance_express_report VALUES %s ",
        "quarter_forcast_report":"INSERT INTO data.quarter_forcast_report VALUES %s ",
        # "normal_deposit_rate_data":"INSERT INTO data.normal_deposit_rate VALUES %s ",
        # "normal_loan_rate_data":"INSERT INTO data.normal_loan_rate VALUES %s ",
        # "normal_required_reserve_ratio_data":"INSERT INTO data.normal_required_reserve_ratio VALUES %s ",
        # "normal_money_supply_data_month":"INSERT INTO data.normal_money_supply_month VALUES %s ",
        # "normal_money_supply_data_year":"INSERT INTO data.normal_money_supply_year VALUES %s ",
        # "normal_shibor_data":"INSERT INTO data.normal_shibor VALUES %s "
    }

    @staticmethod
    def save_as_csv(file, pdata):
        data_list = []
        while pdata.next():
            data_list.append(pdata.get_row_data())
        result = pd.DataFrame(data_list, columns=pdata.fields)
        result.to_csv(file, encoding='utf-8', index=False)
        return None

    @staticmethod
    def save_as_db(executesql, pdatas):
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
    def io_save_db(table, cols, pdatas):
        f = StringIO()
        f.seek(0)
        size = 0
        for pdata in pdatas:
            size += len(pdata)
            while pdata.next():
                f.write('\t'.join(pdata.get_row_data())+'\n')
        conn = DBUtil.getConnect()
        cur = conn.cursor()
        cur.copy_from(f, table, columns=cols,sep='\t', null='\\N', size=size)
        conn.commit()
        return