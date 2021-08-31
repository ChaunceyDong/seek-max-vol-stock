import tushare as ts
import pandas as pd
from time import sleep
from config import *
from tushare_data import get_cb_eod, get_sym_name_map, get_sym_underlying_map
import datetime


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
    get_sym_underlying_map = get_sym_underlying_map()
    daily_df = pd.DataFrame()


    max_vol_summary = pd.DataFrame()
    all_eod_df = get_cb_eod(START_DATE, END_DATE)

    vol_df = all_eod_df.pivot_table(index="trade_date", values="vol", columns="ts_code")
    ewm_vol = vol_df.ewm(halflife=5, adjust=False, min_periods=10).mean()
    ranking = ewm_vol.iloc[-1].sort_values(ascending=False)

    output = pd.DataFrame(ranking.index)
    output['name'] = output['ts_code'].apply(lambda x: sym_name_map[x])
    output['ewm_vol'] = output['ts_code'].apply(lambda x: ranking[x])
    output['underlying'] = output['ts_code'].apply(lambda x: get_sym_underlying_map[x])
    output['exchange'] = output['ts_code'].apply(lambda x: get_sym_underlying_map[x])
    file_path = f"data/vol_ranking_{START_DATE}_{END_DATE}-" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    output.to_csv(file_path, encoding='utf_8_sig')
    print("finished, output to", file_path)
