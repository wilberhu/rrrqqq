import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

fund_nav_path = "tushare_data/data/tush_fund_nav/"

# 公募基金净值数据
def save_fund_nav():
    fields = ['ts_code', 'ann_date', 'end_date', 'unit_nav', 'accum_nav',
              'accum_div', 'net_asset', 'total_netasset', 'adj_nav']
    df = pro.fund_nav(end_date=trade_date, market='E', fields=fields)
    df.to_csv(fund_nav_path + "fund_nav_E.csv", index=False)
    df.to_csv(fund_nav_path + trade_date + "_fund_nav_E.csv", index=False)

    df = pro.fund_nav(end_date=trade_date, market='O', fields=fields)
    df.to_csv(fund_nav_path + "fund_nav_O.csv", index=False)
    df.to_csv(fund_nav_path + trade_date + "_fund_nav_O.csv", index=False)


if __name__ == '__main__':
    save_fund_nav()


    print("~~~~~~~~~~~")
    data1 = pd.read_csv(fund_nav_path + "fund_nav_O.csv")
    data2 = pd.read_csv(fund_nav_path + "fund_nav_E.csv")
    df = pd.concat([data1, data2], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)
    df = df.fillna('')

    table_name = "tush_fundnav"
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name)