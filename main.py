import tushare as ts
import pandas as pd
from time import sleep
from config import *
from tushare_data import get_cb_eod, get_sym_name_map, get_sym_underlying_map, get_sym_underlying_name_map
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

    get_sym_underlying_name_map = get_sym_underlying_name_map()
    daily_df = pd.DataFrame()


    max_vol_summary = pd.DataFrame()
    all_eod_df = get_cb_eod(START_DATE, END_DATE)

    vol_df = all_eod_df.pivot_table(index="trade_date", values="vol", columns="ts_code")
    close_df = all_eod_df.pivot_table(index="trade_date", values="close", columns="ts_code")
    mean_vol = vol_df.mean()

    ewm_vol = vol_df.ewm(halflife=5, adjust=False).mean()

    # jack add this detailed close file, to identify during which period equity drop and rebounce in excel
    output_2=close_df.T
    output_2['ts_code']=output_2.index
    output_2['name'] = output_2['ts_code'].apply(lambda x: sym_name_map[x])
    # output_2['exchange'] = output_2['ts_code'].str.split('.').apply(lambda x: x[1])
    output_2['underly_code'] = output_2['ts_code'].apply(lambda x: get_sym_underlying_map[x])
    output_2['underly_name'] = output_2['ts_code'].apply(lambda x: get_sym_underlying_name_map[x])
    output_2['mean_vol'] = mean_vol
    output_2 = pd.concat([output_2, ewm_vol.T], axis=1)

    ranking = ewm_vol.iloc[-1].sort_values(ascending=False)
    output = pd.DataFrame(ranking.index)
    output['name'] = output['ts_code'].apply(lambda x: sym_name_map[x])
    output['ewm_vol'] = output['ts_code'].apply(lambda x: ranking[x])
    output['mean_vol'] = output['ts_code'].apply(lambda x: get_sym_underlying_map[x])
    output['underlying'] = output['ts_code'].apply(lambda x: get_sym_underlying_map[x])
    output['exchange'] = output['ts_code'].str.split('.').apply(lambda x: x[1])
    file_path = f"data/vol_ranking_{START_DATE}_{END_DATE}-" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    output.to_csv(file_path, encoding='utf_8_sig')
    print("finished, output to", file_path)

    file_path = f"data/vol_with_close_{START_DATE}_{END_DATE}-" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    output_2.to_csv(file_path, encoding='utf_8_sig')
    print("finished, output to", file_path)
