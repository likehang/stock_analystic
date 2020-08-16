
from dbUtil import DBUtil
from sava_data import save_as_csv, get_stock_insert_sql
from stock import Stock
from stock_task import run_task_gevent, run_task_process

import datetime

def init_schedule():
    conn = DBUtil.getConnect()
    cur = conn.cursor()
    exeSql = "INSERT INTO system.stock_schedule(execute_table,cron,execute_flag,logic_time,truth_time) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (pk_id) DO UPDATE SET execute_table=excluded.execute_table,cron=excluded.cron,execute_flag=excluded.execute_flag,logic_time=excluded.logic_time,truth_time=excluded.truth_time;"
    content = [
        ["dividend_data", "1w"],
        ["history_k_data", "1d"],
        ["stock_industry", "1d"],
        ["all_stock", "1d"],
        ["sz50_stocks", "1m"],
        ["hs300_stocks", "1m"],
        ["zz500_stocks", "1m"],
        ["trade_dates", "1d"],
        ["adjust_factor", "1d"],
        ["quarter_profit_data", "1m"],
        ["quarter_growth_data", "1m"],
        ["query_balance_data", "1d"],
        ["quarter_dupont_data", "1m"],
        ["quarter_cash_flow_data", "1m"],
        ["quarter_operation_data", "1m"],
        ["quarter_performance_express_report", "1m"],
        ["quarter_forcast_report", "1m"],
        ["normal_deposit_rate_data", "1m"],
        ["normal_loan_rate_data", "1m"],
        ["normal_required_reserve_ratio_data", "1m"],
        ["normal_money_supply_data_month", "1m"],
        ["normal_money_supply_data_year", "1m"],
        ["normal_shibor_data", "1m"]
    ]
    for ld in content:
        ld.append("未执行")
        ld.append("now()")
        ld.append(None)
        cur.execute(exeSql, ld)
    conn.commit()

def init_stock_data():
    now_date = datetime.datetime.now().strftime("%Y-%m-%d")
    st = Stock()
    ss = Stock.get_macroscopic_list()
    # ss = st.get_all_functions()
    codes = []
    conn = DBUtil.getConnect()
    cur = conn.cursor()
    sql = "select code from data.all_stock"
    cur.execute(sql)
    for row in cur.fetchall():
        codes.append(row[0])
    for key in ss:
        start = datetime.datetime.now()
        print("Do ",key)
        # insert_sql = get_stock_insert_sql(key)
        fun = st.get_fun(key)
        kwargs = []
        if key in Stock.get_divided_list():
            for code in codes:
                for year in range(2010,int(now_date[0:4])):
                    kwargs.append({"code":code,"year":str(year),"yearType":"report"})
        elif key in Stock.get_history_list():
            for code in codes:
                kwargs.append({
                    "code":code,
                    "frequency":"d",
                    "start_date":None,
                    "end_date":now_date,
                    "adjust_flag":"3"
                })
        elif key in Stock.get_sector_list():
            if key == "stock_industry":
                for code in codes:
                    kwargs.append({"code":code})
            else:
                kwargs.append({})
        elif key in Stock.get_metadata_list():
            if key == "all_stock":
                kwargs.append({"date":now_date})
            elif key == "trade_dates":
                kwargs.append({"start_date":"2005-01-01", "end_date":now_date})
            elif key == "stock_basic":
                for code in codes:
                    kwargs.append({"code":code})
            elif key == "adjust_factor":
                for code in codes:
                    kwargs.append({"code":code, "start_date":"2005-01-01", "end_date":now_date})
        elif key in Stock.get_evaluation_list():
            for code in codes:
                for year in range(2000,int(now_date[0:4])):
                    for quarter in [1, 2, 3, 4]:
                        kwargs.append({"code":code, "year":year, "quarter":quarter})
        elif key in Stock.get_corporate_list():
            for code in codes:
                kwargs.append({"code":code, "end_date":None, "start_date":"2003-01-01"})
        elif key in Stock.get_macroscopic_list():
            if key == "normal_required_reserve_ratio_data":
                kwargs.append({"start_date":"2005-01-01", "end_date":now_date, "yearType":0})
            else:
                kwargs.append({"start_date":"2005-01-01", "end_date":now_date})
        print("task size:",len(kwargs))
        results = run_task_process(fun=fun, fun_kwargs=kwargs, processes=5, prefun=Stock)
        print(key," start insert", (datetime.datetime.now() - start).seconds)
        start = datetime.datetime.now()
        DBUtil.save_as_db(get_stock_insert_sql(key), results)
        # DBUtil.io_save_db("data", key, results)
        print(key," done", (datetime.datetime.now() - start).seconds)
    print("Done ")

if __name__ == "__main__":
    init_stock_data()