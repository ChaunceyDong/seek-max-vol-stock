import tushare as ts
import pandas as pd
from time import sleep
from config import *


def get_sym_name_map():
    """获取可转债基础信息列表"""
    df = pro.cb_basic(fields="ts_code,bond_short_name")
    return dict(zip(df['ts_code'], df['bond_short_name']))

def get_daily(start_date, end_date):
    try:
        df2 = pro.cb_daily(start_date=start_date, end_date=end_date)
        return df2
    except:
        sleep(1)
        print("api access limit, sleep for 1s")
        return get_daily(start_date, end_date)


if __name__ == '__main__':
    pro = ts.pro_api()

    sym_name_map = get_sym_name_map()
    daily_df = pd.DataFrame()
    for [s, e] in DATE_LST:
        daily_df = daily_df.append(get_daily(s, e))

    ranking = daily_df.groupby(by='ts_code').mean()["amount"].sort_values(ascending=False)
    # ranking['name'] = ranking
    output = pd.DataFrame(ranking.index)
    output['name'] = output['ts_code'].apply(lambda x: sym_name_map[x])
    output['avg_vol'] = output['ts_code'].apply(lambda x: ranking[x])
    output.to_csv("data/vol_ranking.csv", encoding='utf_8_sig')
