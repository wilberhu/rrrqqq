import tushare as ts
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection
import datetime
import pytz
import math

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

stock_basics_path = 'tushare_data/data/stock_basics/'

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

def get_stock_basics():
    fields = 'ts_code,symbol,name,area,industry,list_date,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
    data1 = pro.stock_basic(exchange='', list_status='L', fields=fields)
    data2 = pro.stock_basic(exchange='', list_status='D', fields=fields)
    data3 = pro.stock_basic(exchange='', list_status='P', fields=fields)

    data = pd.concat([data1, data2, data3], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)

    data.to_csv(stock_basics_path + "stock_basics.csv", index=False)
    data.to_csv(stock_basics_path + trade_date + "_stock_basics.csv", index=False)

def get_index_basic():
    fields = 'ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date'
    data1 = pro.index_basic(market='SSE', fields=fields)
    data2 = pro.index_basic(market='SZSE', fields=fields)

    data = pd.concat([data1, data2], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)

    data.to_csv(stock_basics_path + "index_basic.csv", index=False)
    data.to_csv(stock_basics_path + trade_date + "_index_basic.csv", index=False)


def df2sql_stock():
    # read
    df = pd.read_csv(stock_basics_path + "stock_basics.csv", dtype={"symbol": str})
    df = df.fillna('')
    columns = df.columns.tolist()

    sql_list = []

    sql_list.append("replace into companies_company(")
    for i in columns:
        sql_list.append(i)
        sql_list.append(",")
    sql_list.pop()
    sql_list.append(")")

    sql_list.append(" values ")

    for i in df.index:
        row = df.loc[i].values

        sql_list.append("\n(")

        for index, item in enumerate(row):
            if index == columns.index('list_date') or index == columns.index('delist_date'):
                if item == '':
                    sql_list.append("null")
                else:
                    sql_list.append('"' + str(item) + '"')
            else:
                sql_list.append('"' + str(item) + '"')
            sql_list.append(",")
        sql_list.pop()
        sql_list.append(")")
        sql_list.append(",")
    sql_list.pop()
    sql = "".join(sql_list)
    # print(sql)

    #存入数据库
    cursor = connection.cursor()
    cursor.execute("truncate table companies_company;")
    cursor.execute(sql)
    cursor.close()

    #存入数据库
    # df.to_sql('companies_company', engine, index=True, index_label='id', if_exists='replace')

def df2sql_index():
    # read
    df = pd.read_csv(stock_basics_path + "index_basic.csv", dtype={"symbol": str, "base_point": float})
    df = df.fillna('')
    columns = df.columns.tolist()

    sql_list = []

    sql_list.append("replace into companies_index(")
    for i in columns:
        sql_list.append('`' + i + '`')
        sql_list.append(",")
    sql_list.pop()
    sql_list.append(")")

    sql_list.append(" values ")

    for i in df.index:
        row = df.loc[i].values

        sql_list.append("\n(")

        for index, item in enumerate(row):
            if index == columns.index('base_date') or \
                index == columns.index('list_date') or \
                index == columns.index('exp_date'):
                if item == '':
                    sql_list.append("null")
                else:
                    sql_list.append('"' + str(item) + '"')
            elif index == columns.index('desc'):
                sql_list.append('"' + item.replace('"', '\\"') + '"')
            elif index == columns.index('base_point'):
                if str(item).strip() == "":
                    sql_list.append('"0"')
                else:
                    sql_list.append('"' + str(item) + '"')
            else:
                sql_list.append('"' + str(item) + '"')
            sql_list.append(",")
        sql_list.pop()
        sql_list.append(")")
        sql_list.append(",")
    sql_list.pop()
    sql = "".join(sql_list)
    # print(sql)

    #存入数据库
    cursor = connection.cursor()
    cursor.execute("truncate table companies_index;")
    cursor.execute(sql)
    cursor.close()

if __name__ == '__main__':
    get_stock_basics()
    get_index_basic()
    print("~~~~~~~~~~~")
    df2sql_stock()
    print("~~~~~~~~~~~")
    df2sql_index()
