import datetime
import pytz
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection
import pandas as pd

def get_today():
    # return "20210305"
    time_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
    trade_date = time_now.strftime('%Y%m%d')
    columns = ['cal_date', 'pretrade_date', 'is_open']
    sql = "SELECT " + ",".join(columns) + " FROM tush_trade_cal WHERE cal_date=" + trade_date

    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()  # 读取所有
    df = pd.DataFrame(rows, columns=columns)

    time_h = int(time_now.strftime("%H"))
    if time_h < 18 or df.iloc[0]['is_open'] == 0:
        return str(df.iloc[0]['pretrade_date'])
    else:
        return str(df.iloc[0]['cal_date'])


if __name__ == '__main__':
    print(get_today())