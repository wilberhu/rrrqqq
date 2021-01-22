import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

trade_cal_path = 'tushare_data/data/trade_cal/'

def get_trade_cal():
    fields = ['exchange', 'cal_date', 'is_open', 'pretrade_date']
    data = pro.trade_cal(fields=fields)
    data.to_csv(trade_cal_path + "trade_cal.csv", index=False)


if __name__ == '__main__':
    get_trade_cal()

    path = trade_cal_path + "trade_cal.csv"
    table_name = "tush_trade_cal"
    df = pd.read_csv(path)
    df = df.fillna('')
    df2sql(df=df, table_name=table_name)