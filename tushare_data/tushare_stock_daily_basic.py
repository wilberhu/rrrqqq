import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

daily_info_path = 'tushare_data/data/tush_stock_daily_basic/'

# 实时行情
def save_daily_basic():
    fields = ["ts_code","trade_date","close","turnover_rate","turnover_rate_f",
               "volume_ratio","pe","pe_ttm","pb","ps",
               "ps_ttm","total_share","float_share","free_share","total_mv",
               "circ_mv"]
    df = pro.daily_basic(ts_code='', trade_date=trade_date, fields=fields)

    #save
    df.to_csv(daily_info_path + trade_date + "_company_dailybasic.csv", index=False)
    df.to_csv(daily_info_path + "company_dailybasic.csv", index=False)

# 大盘指数行情
def save_index_dailybasic():
    fields = ["ts_code","trade_date","total_mv","float_mv","total_share",
               "float_share","free_share","turnover_rate","turnover_rate_f","pe",
               "pe_ttm","pb"]
    df = pro.index_dailybasic(trade_date=trade_date, fields=fields)

    #save
    df.to_csv(daily_info_path + trade_date + "_index_dailybasic.csv", index=False)
    df.to_csv(daily_info_path + "index_dailybasic.csv", index=False)

if __name__ == '__main__':
    save_daily_basic()
    save_index_dailybasic()

    # company
    path = daily_info_path + "company_dailybasic.csv"
    table_name = "tush_companydailybasic"
    dtype= {'trade_date': str, "ts_code": str}
    df = pd.read_csv(path, dtype=dtype)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name, dtype=dtype)

    # index
    path = daily_info_path + "index_dailybasic.csv"
    table_name = "tush_indexdailybasic"
    dtype= {'trade_date': str, "ts_code": str}
    df = pd.read_csv(path, dtype=dtype)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name, dtype=dtype)
