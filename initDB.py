from dbUtil import DBUtil
from sava_data import SaveStock
from stock import Stock

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
    st = Stock()
    ss = SaveStock().function_to_sql
    skeys = ss.keys()
    codes = []
    conn = DBUtil.getConnect()
    cur = conn.cursor()
    sql = "select code from data.all_stock"
    cur.execute(sql)
    for row in cur.fetchall():
        codes.append(row[0])
    for key in skeys:
        print("Do {}",key)
        sql = ss[key]
        datalist = []
        if key == "dividend_data":
            datalist = []
            for code in codes:
                for year in range(2010,2020):
                    datalist.append(st.dividend_data(code,str(year),"report"))
        elif key == "history_k_data":
            for code in codes:
                datalist.append(st.history_k_data(code,"d",None,"2020-08-03","3"))
        elif key == "stock_industry":
            for code in codes:
                datalist.append(st.stock_industry(code))
        elif key == "all_stock":
            datalist.append(st.all_stock("2020-08-03"))
        elif key == "sz50_stocks":
            datalist.append(st.sz50_stocks())
        elif key == "hs300_stocks":
            datalist.append(st.hs300_stocks())
        elif key == "zz500_stocks":
            datalist.append(st.zz500_stocks())
        elif key == "trade_dates":
            datalist.append(st.trade_dates("2005-01-01", "2020-08-03"))
        elif key == "adjust_factor":
            for code in codes:
                datalist.append(st.adjust_factor(code, "2005-01-01", "2020-08-03"))
        elif key == "quarter_profit_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_profit_data(code, year, quarter))
        elif key == "quarter_growth_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_growth_data(code, year, quarter))
        elif key == "query_balance_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_balance_data(code, year, quarter))
        elif key == "quarter_dupont_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_dupont_data(code, year, quarter))
        elif key == "quarter_cash_flow_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_cash_flow_data(code, year, quarter))
        elif key == "quarter_operation_data":
            for code in codes:
                for year in range(2005,2020):
                    for quarter in [1, 2, 3, 4]:
                        datalist.append(st.quarter_operation_data(code, year, quarter))
        elif key == "quarter_performance_express_report":
            for code in codes:
                datalist.append(st.quarter_performance_express_report(code, None, "2006-01-01"))
        elif key == "quarter_forcast_report":
            for code in codes:
                datalist.append(st.quarter_forecast_report(code, None, "2003-01-01"))
        elif key == "normal_deposit_rate_data":
            datalist.append(st.normal_deposit_rate_data("2005-01-01", "2020-08-03"))
        elif key == "normal_loan_rate_data":
            datalist.append(st.normal_loan_rate_data("2005-01-01", "2020-08-03"))
        elif key == "normal_required_reserve_ratio_data":
            datalist.append(st.normal_required_reserve_ratio_data("2005-01-01", "2020-08-03", 0))
        elif key == "normal_money_supply_data_month":
            datalist.append(st.normal_money_supply_data_month("2005-01-01", "2020-08-03"))
        elif key == "normal_money_supply_data_year":
            datalist.append(st.normal_money_supply_data_year("2005-01-01", "2020-08-03"))
        elif key == "normal_shibor_data":
            datalist.append(st.normal_shibor_data("2005-01-01", "2020-08-03"))
        else:
            print("error {}", key)
        SaveStock.save_as_db(sql, datalist)
        print("{} done", key)
    print("Done ")

if __name__ == "__main__":
    init_stock_data()