import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

daily_info_path = 'tushare_data/data/tush_stock_daily/'

index_hist_data_path = 'tushare_data/data/tush_index_hist_data/'

# 实时行情
def save_daily_basic():
    fields = ['ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount']
    df = pro.daily(trade_date=trade_date, fields=fields)

    #save
    df.to_csv(daily_info_path + trade_date + "_stockdaily.csv", index=False)
    df.to_csv(daily_info_path + "stockdaily.csv", index=False)

# 大盘指数行情
def save_index_dailybasic():
    fields = ['ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount']
    df_list = []
    file_list = os.listdir(index_hist_data_path)
    file_list.sort()
    for file in file_list:
        tmp_data = pd.read_csv(os.path.join(index_hist_data_path, file))
        tmp_df = tmp_data.loc[tmp_data.shape[0]-1:tmp_data.shape[0]-1]
        if str(tmp_df['trade_date'].values[0]) == trade_date:
            df_list.append(tmp_df[fields])
    df = pd.concat(df_list,  axis=0, join='outer',ignore_index=True)
    #save
    df.to_csv(daily_info_path + trade_date + "_indexdaily.csv", index=False)
    df.to_csv(daily_info_path + "indexdaily.csv", index=False)


if __name__ == '__main__':
    save_daily_basic()
    save_index_dailybasic()

    # company
    path = daily_info_path + "stockdaily.csv"
    table_name = "tush_companydaily"
    df = pd.read_csv(path)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name)

    # index
    path = daily_info_path + "indexdaily.csv"
    table_name = "tush_indexdaily"
    df = pd.read_csv(path)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name)