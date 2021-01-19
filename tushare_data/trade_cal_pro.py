import tushare as ts
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

trade_cal_path = 'tushare_data/data/trade_cal/'

def get_trade_cal():
    fields = ['exchange', 'cal_date', 'is_open', 'pretrade_date']
    data = pro.trade_cal(fields=fields)
    data.to_csv(trade_cal_path + "trade_cal.csv", index=False)


def df2sql_trade_cal():
    # read
    df = pd.read_csv(trade_cal_path + "trade_cal.csv", dtype={"cal_date": str, "pretrade_date": str})
    df = df.fillna('')
    columns = df.columns.tolist()

    sql_list = []

    sql_list.append("replace into trade_cal(")
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
            if index == columns.index('cal_date') or index == columns.index('pretrade_date'):
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
    cursor.execute("truncate table trade_cal;")
    cursor.execute(sql)
    cursor.close()

    #存入数据库
    # df.to_sql('trade_cal', engine, index=True, index_label='id', if_exists='replace')

if __name__ == '__main__':
    get_trade_cal()
    df2sql_trade_cal()