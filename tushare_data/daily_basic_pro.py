import tushare as ts
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection
import datetime
import pytz
import time
pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

daily_info_path = 'tushare_data/data/daily_basic/'

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


# 实时行情
def save_daily_basic():
    fields = 'ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,free_share,total_mv,circ_mv'
    df = pro.daily_basic(ts_code='', trade_date=trade_date, fields=fields)

    #save
    df.to_csv(daily_info_path + trade_date + "_daily_basic.csv", index=False)
    df.to_csv(daily_info_path + "daily_basic.csv", index=False)

# 大盘指数行情
def save_index_dailybasic():
    fields = 'ts_code,trade_date,total_mv,float_mv,total_share,float_share,free_share,turnover_rate,turnover_rate_f,pe,pe_ttm,pb'
    df = pro.index_dailybasic(trade_date=trade_date, fields=fields)

    #save
    df.to_csv(daily_info_path + trade_date + "_index_dailybasic.csv", index=False)
    df.to_csv(daily_info_path + "index_dailybasic.csv", index=False)



def df2sql(path, columns, table_name):
    # read
    df = pd.read_csv(path, dtype={"ts_code": str})
    df = df.fillna(0)


    sql_list = []

    sql_list.append("replace into " + table_name + "(")
    for i in columns:
        # mysql关键字加反引号处理
        sql_list.append("`" + i + "`")
        sql_list.append(",")
    sql_list.pop()
    sql_list.append(")")

    sql_list.append(" values ")

    for i in df.index:
        row = df.loc[i].values

        sql_list.append("\n(")
        for index, item in enumerate(row):
            if index == columns.index('trade_date'):
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
    cursor.execute("truncate table " + table_name + ";")
    cursor.execute(sql)
    cursor.close()


if __name__ == '__main__':
    save_daily_basic()
    save_index_dailybasic()

    # company
    path = daily_info_path + "daily_basic.csv"
    columns = ["ts_code","trade_date","close","turnover_rate","turnover_rate_f",
               "volume_ratio","pe","pe_ttm","pb","ps",
               "ps_ttm","total_share","float_share","free_share","total_mv",
               "circ_mv"]
    table_name = "dailies_companydaily"
    df2sql(path=path, columns=columns, table_name=table_name)

    # index
    path = daily_info_path + "index_dailybasic.csv"
    columns = ["ts_code","trade_date","total_mv","float_mv","total_share",
               "float_share","free_share","turnover_rate","turnover_rate_f","pe",
               "pe_ttm","pb"]
    table_name = "dailies_indexdaily"
    df2sql(path=path, columns=columns, table_name=table_name)