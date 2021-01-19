import tushare as ts
import pandas as pd
import os
import time
import datetime
import pytz
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection

hist_data_path = 'tushare_data/data/hist_data/'
index_hist_data_path = 'tushare_data/data/index_hist_data/'

stock_basics_path = 'tushare_data/data/stock_basics/stock_basics.csv'
index_basics_path = 'tushare_data/data/stock_basics/index_basic.csv'

# trade_date = '20200430'

time_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
time_h = int(time_now.strftime("%H"))
time_w = int(time_now.strftime("%w"))
if time_h > 17: # 17点之前
    if time_w == 6: # 星期六
        trade_date = (time_now - datetime.timedelta(days=1)).strftime("%Y%m%d")
    elif time_w == 0: # 星期天
        trade_date = (time_now - datetime.timedelta(days=2)).strftime("%Y%m%d")
    else:
        trade_date = (time_now).strftime("%Y%m%d")
else: # 17点之后
    if time_w == 0: # 星期天
        trade_date = (time_now - datetime.timedelta(days=2)).strftime("%Y%m%d")
    elif time_w == 1: # 星期一
        trade_date = (time_now - datetime.timedelta(days=3)).strftime("%Y%m%d")
    else:
        trade_date = (time_now - datetime.timedelta(days=1)).strftime("%Y%m%d")


company_fields = 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
index_fields = 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'


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
            df2sql_hist_data(df[1:])
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.daily(ts_code=code, fields=company_fields)[::-1])
        if df.empty:
            return
        else:
            df2sql_hist_data(df)
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

def df2sql_hist_data(df):
    if df.empty:
        return
    df = df.fillna('null')
    columns = df.columns.tolist()

    sql_list = []

    sql_list.append("replace into hist_data(")
    for i in columns:
        sql_list.append("`" + i + "`")
        sql_list.append(",")
    sql_list.pop()
    sql_list.append(")")

    sql_list.append(" values ")

    for i in df.index:
        row = df.loc[i].values

        sql_list.append("\n(")

        for index, item in enumerate(row):
            if index == columns.index('ts_code') or index == columns.index('trade_date'):
                sql_list.append('"' + str(item) + '"')
            else:
                sql_list.append(str(item))
            sql_list.append(",")
        sql_list.pop()
        sql_list.append(")")
        sql_list.append(",")
    sql_list.pop()
    sql = "".join(sql_list)
    # print(sql)

    #存入数据库
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()


def fun_company(items):
    for index, code in enumerate(items):
        time.sleep(1)
        print("company code:", code)
        update_hist_data(code)

def fun_index(indexs):
    for code in indexs:
        time.sleep(1.2)
        print("index code:", code)
        update_index_hist_data(code)

def save_hist_data_to_db(items):
    for index, code in enumerate(items):
        print("company code:", code)
        file_path = hist_data_path + code + '.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df2sql_hist_data(df)

if __name__ == '__main__':
    pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')
    companies = load_companies(stock_basics_path)
    fun_company(companies)

    indexs = load_companies(index_basics_path)
    fun_index(indexs)

    # save_hist_data_to_db(companies)
