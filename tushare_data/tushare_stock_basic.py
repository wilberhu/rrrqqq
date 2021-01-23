import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

stock_basics_path = 'tushare_data/data/tush_stock_basic/'

def get_stock_basic():
    fields = ['ts_code', 'symbol', 'name', 'area', 'industry',
              'fullname', 'enname', 'market', 'exchange', 'curr_type',
              'list_status', 'list_date', 'delist_date', 'is_hs']
    data1 = pro.stock_basic(exchange='', list_status='L', fields=fields)
    data2 = pro.stock_basic(exchange='', list_status='D', fields=fields)
    data3 = pro.stock_basic(exchange='', list_status='P', fields=fields)

    data = pd.concat([data1, data2, data3], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)

    data.to_csv(stock_basics_path + "stock_basic.csv", index=False)
    data.to_csv(stock_basics_path + trade_date + "_stock_basic.csv", index=False)

def get_index_basic():
    fields = ['ts_code', 'name', 'fullname', 'market', 'publisher',
              'index_type', 'category', 'base_date', 'base_point', 'list_date',
              'weight_rule', 'desc', 'exp_date']
    data1 = pro.index_basic(market='SSE', fields=fields)
    data2 = pro.index_basic(market='SZSE', fields=fields)

    data = pd.concat([data1, data2], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)

    data.to_csv(stock_basics_path + "index_basic.csv", index=False)
    data.to_csv(stock_basics_path + trade_date + "_index_basic.csv", index=False)


if __name__ == '__main__':
    get_stock_basic()
    get_index_basic()

    print("~~~~~~~~~~~")
    path = stock_basics_path + "stock_basic.csv"
    table_name = "tush_company"
    dtype = {"base_point": float}
    df = pd.read_csv(path, dtype=dtype)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name, dtype=dtype)


    print("~~~~~~~~~~~")
    path = stock_basics_path + "index_basic.csv"
    table_name = "tush_index"
    dtype = {"base_point": float}
    df = pd.read_csv(path, dtype=dtype)
    df = df.fillna('')
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name, dtype=dtype)



