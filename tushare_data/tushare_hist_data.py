import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

import time

hist_data_path = 'tushare_data/data/tush_hist_data/'
index_hist_data_path = 'tushare_data/data/tush_index_hist_data/'

stock_basics_path = 'tushare_data/data/tush_stock_basic/stock_basic.csv'
index_basics_path = 'tushare_data/data/tush_stock_basic/index_basic.csv'


company_fields = ['ts_code', 'trade_date', 'open', 'high', 'low',
                  'close', 'pre_close', 'change', 'pct_chg', 'vol',
                  'amount']
index_fields = ['ts_code', 'trade_date', 'open', 'high', 'low',
                  'close', 'pre_close', 'change', 'pct_chg', 'vol',
                  'amount']
table_name="tush_hist_data"

def load_companies(path):
    df = pd.read_csv(path, dtype=str)
    return list(df['ts_code'])

def update_hist_data(code):
    file_path = hist_data_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)

        if str(hist_data.iloc[-1]['trade_date']) == trade_date:
            return
        df = pd.DataFrame(pro.daily(ts_code=code, fields=company_fields, start_date=str(hist_data.iloc[-1]['trade_date']))[::-1])
        if df.empty:
            return
        else:
            df2sql(df=df[1:], table_name=table_name, dtype={"amount":float})
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.daily(ts_code=code, fields=company_fields)[::-1])
        if df.empty:
            return
        else:
            df2sql(df=df, table_name=table_name, dtype={"amount":float})
            df.to_csv(file_path, index=False, encoding='UTF-8')

def update_index_hist_data(code):
    file_path = index_hist_data_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)

        if str(hist_data.iloc[-1]['trade_date']) == trade_date:
            return
        df = pd.DataFrame(pro.index_daily(ts_code=code, fields=index_fields, start_date=str(hist_data.iloc[-1]['trade_date']))[::-1])
        if df.empty:
            return
        else:
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.index_daily(ts_code=code, fields=index_fields)[::-1])
        if df.empty:
            return
        else:
            df.to_csv(file_path, index=False, encoding='UTF-8')


def fun_company(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                time.sleep(0.8)
                print(index, " company code: ", code)
                update_hist_data(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_company(items, code)


def fun_index(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                time.sleep(1.2)
                print(index, " index code: ", code)
                update_index_hist_data(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_index(items, code)

def save_hist_data_to_db(items):

    for index, code in enumerate(items):
        print("company code:", code)
        file_path = hist_data_path + code + '.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df2sql(df=df, table_name=table_name, dtype={"amount":float})

if __name__ == '__main__':
    pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')
    companies = load_companies(stock_basics_path)
    fun_company(companies)

    indexs = load_companies(index_basics_path)
    fun_index(indexs)

    # save_hist_data_to_db(companies)
