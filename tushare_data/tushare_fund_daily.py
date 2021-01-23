import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

fund_daily_path = "tushare_data/data/tush_fund_daily/"

# 场内基金日线行情，类似股票日行情
def save_fund_daily():
    fields = ['ts_code', 'trade_date', 'open', 'high', 'low',
              'close', 'pre_close', 'change', 'pct_chg', 'vol',
              'amount']
    df = pro.fund_daily(trade_date=trade_date, fields=fields)
    df.to_csv(fund_daily_path + "fund_daily.csv", index=False)
    df.to_csv(fund_daily_path + trade_date + "_fund_daily.csv", index=False)


if __name__ == '__main__':
    save_fund_daily()

    print("~~~~~~~~~~~")
    df = pd.read_csv(fund_daily_path + "fund_daily.csv")
    df = df.fillna('')

    table_name = "tush_funddaily"
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name)