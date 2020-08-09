import baostock as bs
import pandas as pd
import datetime
import time
import config
from functools import wraps
class Stock():
    @staticmethod
    def get_history_list():
        return ["history_k_data"]

    @staticmethod
    def get_sector_list():
        return ["stock_industry","sz50_stocks","hs300_stocks","zz500_stocks"]

    @staticmethod
    def get_evaluation_list():
        return [
            "quarter_profit_data","quarter_growth_data"
            ,"query_balance_data","quarter_dupont_data"
            ,"quarter_cash_flow_data","quarter_operation_data"
        ]

    @staticmethod
    def get_corporate_list():
        return ["quarter_performance_express_report","quarter_forcast_report"]

    @staticmethod
    def get_metadata_list():
        return ["all_stock","trade_dates","stock_basic"]

    @staticmethod
    def get_macroscopic_list():
        return [
            "normal_deposit_rate_data","normal_loan_rate_data"
            ,"normal_required_reserve_ratio_data","normal_money_supply_data_month"
            ,"normal_money_supply_data_year","normal_shibor_data"
        ]

    def get_result_data(self, flag_name, *args):
        fun = self.__getattribute__(flag_name)
        return fun(self, *args)

    # def bs_ready(self, func):
    #     @wraps(func)
    #     def wrapper(self, *args, **kwargs):
    #         self.getStockInstance()
    #         rs = func(self, *args, **kwargs)
    #         self._test_rs(rs)
    #         return rs
    #     return wrapper

    def __init__(self):
        self._login = bs.login()
        self._lasttime = datetime.datetime.today()

    def __del__(self):
        bs.logout()

    def _test_rs(self, rs):
        if rs.error_code == '0':
            self._lasttime = datetime.datetime.today()
        else:
            raise BaseException("rs is error")

    def getStockInstance(self):
        nowtimestamp = datetime.datetime.today()
        timedelta = nowtimestamp - self._lasttime
        timedelta = timedelta.total_seconds()/ 60.0
        if self._login.error_code == '0' and timedelta <= 1:
            self._lasttime = nowtimestamp
            return True
        else:
            flag = False
            num = 0
            while( not flag):
                num += 1
                self._login = bs.login()
                if self._login.error_code == '0':
                    return True
                time.sleep(config.SLEEP_TIME)
                if( num >= 5):
                    raise BaseException("login error after 5 times")
    """
    除权除息信息：query_dividend_data()
    """
    def dividend_data(self, code, year, yearType='report'):
        self.getStockInstance()
        rs = bs.query_dividend_data(code, year, yearType)
        self._test_rs(rs)
        return rs
    """
    获取历史A股K线数据：query_history_k_data_plus()
    """
    def history_k_data(self, code, frequency, start_date, end_date, adjust_flag='3'):
        frequency = str(frequency).lower()
        if frequency == 'd':
            fields = 'date, code, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST'
        elif frequency in ['w', 'm']:
            fields = 'date, code, open, high, low, close, volume, amount, adjustflag, turn, pctChg'
        else:
            fields='date, code, open, high, low, close, volume, amount, adjustflag'
        self.getStockInstance()
        rs = bs.query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjust_flag)
        self._test_rs(rs)
        return rs
    """
    证券代码查询：query_all_stock()
    """
    def all_stock(self, date=None):
        self.getStockInstance()
        rs = bs.query_all_stock(date)
        self._test_rs(rs)
        return rs
    """
    行业分类：query_stock_industry()
    """
    def stock_industry(self, code):
        self.getStockInstance()
        rs = bs.query_stock_industry(code)
        self._test_rs(rs)
        return rs
    """
    上证50成分股：query_sz50_stocks()
    """
    def sz50_stocks(self, *args, **kwargs):
        self.getStockInstance()
        rs = bs.query_sz50_stocks()
        self._test_rs(rs)
        return rs
    """
    沪深300成分股：query_hs300_stocks()
    """
    def hs300_stocks(self):
        self.getStockInstance()
        rs = bs.query_hs300_stocks()
        self._test_rs(rs)
        return rs
    """
    中证500成分股：query_zz500_stocks()
    """
    def zz500_stocks(self):
        self.getStockInstance()
        rs = bs.query_zz500_stocks()
        self._test_rs(rs)
        return rs
    """
    交易日查询：query_trade_dates()
    """
    def trade_dates(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_trade_dates(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    证券基本资料：query_stock_basic()
    """
    def stock_basic(self, code):
        self.getStockInstance()
        rs = bs.query_stock_basic(code)
        self._test_rs(rs)
        return rs
    """
    复权因子：query_adjust_factor()
    """
    def adjust_factor(self, code, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_adjust_factor(code, start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    季频盈利能力：query_profit_data()
    """
    def quarter_profit_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_profit_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频成长能力：query_growth_data()
    """
    def quarter_growth_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_growth_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频偿债能力：query_balance_data()
    """
    def quarter_balance_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_balance_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频杜邦指数：query_dupont_data()
    """
    def quarter_dupont_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_dupont_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频现金流量：query_cash_flow_data()
    """
    def quarter_cash_flow_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_cash_flow_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频营运能力：query_operation_data()
    """
    def quarter_operation_data(self, code, year, quarter):
        self.getStockInstance()
        rs = bs.query_operation_data(code, year, quarter)
        self._test_rs(rs)
        return rs
    """
    季频公司业绩快报：query_performance_express_report()
    """
    def quarter_performance_express_report(self, code,end_date, start_date='2006-01-01'):
        self.getStockInstance()
        rs = bs.query_performance_express_report(code, start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    季频公司业绩预告：query_forecast_report()
    """
    def quarter_forecast_report(self, code,end_date, start_date='2003-01-01'):
        self.getStockInstance()
        rs = bs.query_forecast_report(code, start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    存款利率：query_deposit_rate_data()
    """
    def normal_deposit_rate_data(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_deposit_rate_data(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    贷款利率：query_loan_rate_data()
    """
    def normal_loan_rate_data(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_loan_rate_data(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    存款准备金率：query_required_reserve_ratio_data()
    """
    def normal_required_reserve_ratio_data(self, start_date, end_date, yearType):
        assert yearType in [0, 1]
        self.getStockInstance()
        rs = bs.query_required_reserve_ratio_data(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    货币供应量：query_money_supply_data_month()
    """
    def normal_money_supply_data_month(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_money_supply_data_month(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    货币供应量(年底余额)：query_money_supply_data_year()
    """
    def normal_money_supply_data_year(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_money_supply_data_year(start_date, end_date)
        self._test_rs(rs)
        return rs
    """
    银行间同业拆放利率：query_shibor_data()
    """
    def normal_shibor_data(self, start_date, end_date):
        self.getStockInstance()
        rs = bs.query_shibor_data(start_date, end_date)
        self._test_rs(rs)
        return rs