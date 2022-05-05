import pandas as pd
from time import sleep
import tushare as ts
from datetime import datetime, timedelta
import numpy as np
ts.set_token('c721c267a710ecdcb3f8da94e4a3c3ed145ae5c0e4cf6a5c983e470b')
pro = ts.pro_api()


def datetime_range(start: str, end: str):
    """
    :param start: str
    :param end: str
    :return: str
    """
    start = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    span = end - start
    for i in range(span.days + 1):
        date = start + timedelta(days=i)
        yield datetime.strftime(date, "%Y%m%d")

def get_trading_date_range(start_date, end_date):
    """
    get date range list from start and end date
    :param start_date: "20210810"
    :param end_date:
    :return: []
    """
    df = pro.trade_cal(start_date=start_date, end_date=end_date)
    return df.loc[df['is_open']==1,"cal_date"].to_list()

def get_stock_eod_single(trade_date):
    """
    return EOD data, with adj_factor and is_trade

        col "is_trade":
            stock in suspend list marked as 1, others as 0.
    """
    try:
        eod_df = pro.daily(trade_date=trade_date)
        adj_df = pro.adj_factor(trade_date=trade_date, fields="ts_code, adj_factor")
        df_m = pd.merge(eod_df, adj_df, on="ts_code")

        sus_df = pro.suspend_d(trade_date=trade_date, suspend_type="S", fields="ts_code, suspend_timing")
        sus_code_lst = sus_df[sus_df['suspend_timing'].isnull()]['ts_code'].to_list()

        df_m['is_trade'] = df_m['ts_code'].apply(lambda x : 0 if x in sus_code_lst else 1)
        return df_m
    except:
        sleep(10)
        print("api access limit, sleep for 10s")
        return get_stock_eod_single(trade_date)

def get_stock_eod(start_date, end_date):

    daily_df = pd.DataFrame()
    for date in datetime_range(start_date, end_date):
        df_one_day = get_stock_eod_single(date)
        if len(df_one_day) > 0:
            print("get eod data", date)
            daily_df = daily_df.append(df_one_day)

    return daily_df

def get_cb_eod_single(trade_date):
    """
    return EOD data, with adj_factor and is_trade

        col "is_trade":
            stock in suspend list marked as 1, others as 0.
    """
    try:
        eod_df = pro.cb_daily(trade_date=trade_date)
        return eod_df
    except:
        sleep(10)
        print("api access limit, sleep for 5s")
        return get_cb_eod_single(trade_date)

def get_cb_eod(start_date, end_date):

    daily_df = pd.DataFrame()
    for date in get_trading_date_range(start_date, end_date):
        df_one_day = get_cb_eod_single(date)
        if len(df_one_day) > 0:
            print("get eod data", date)
            daily_df = daily_df.append(df_one_day)
    return daily_df

def get_ts_stock_lst():
    all_stock = pro.stock_basic()
    st_stock = all_stock[all_stock['name'].str.contains("ST")]
    return st_stock['ts_code'].to_list()

def get_sym_name_map():
    """获取可转债基础信息列表"""
    df = pro.cb_basic(fields="ts_code,bond_short_name")
    return dict(zip(df['ts_code'], df['bond_short_name']))

def get_sym_underlying_map():
    """获取可转债基础信息列表"""
    df = pro.cb_basic(fields="ts_code,stk_code")
    return dict(zip(df['ts_code'], df['stk_code']))

def get_sym_underlying_name_map():
    """获取可转债基础信息列表"""
    df = pro.cb_basic(fields="ts_code,stk_short_name")
    return dict(zip(df['ts_code'], df['stk_short_name']))

if __name__ == '__main__':
    # date  = '2021-07-05'
    # eod_df = pd.DataFrame()
    # d = get_stock_eod(START_DATE, END_DATE)
    d1 = get_cb_eod('20210702', '20210705')
    print(d1)