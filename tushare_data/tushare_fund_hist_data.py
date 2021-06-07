import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

import time

fund_hist_path = r'tushare_data/data/tush_fund_hist_data/'
fund_nav_path = r'tushare_data/data/tush_fund_nav_data/'

fund_list_path = r'tushare_data/data/tush_fund_list/'
fund_list_tushare_path = 'tushare_data/data/tush_fund_basic/'
fund_list_akshare_path = r'tushare_data/data/aksh_fund_em/'

fund_basic_e_path = r'tushare_data/data/tush_fund_basic/20210122_fund_basic_E.csv'

def load_funds(path):
    df = pd.read_csv(path, dtype=str)
    return list(df['ts_code'])


def get_fund_list():
    # fund_list_tushare - fund_code_list_tushare : 000001 - 000001.OF
    # fund_list_akshare - fund_code_list_akshare : 000001 - 000001.OF
    # fund_list_akshare_add - fund_code_list_akshare_add : 000001 - 000001.OF
    # fund_list_dir - fund_code_list_dir : 000001 - 000001.OF
    # fund_list_dir_add - fund_code_list_dir_add : 000001 - 000001.OF


    fund_code_list_tushare = load_funds(fund_list_tushare_path + "fund_basic_O_L.csv")
    fund_code_list_tushare.extend(load_funds(fund_list_tushare_path + "fund_basic_O_I.csv"))
    fund_code_list_tushare.extend(load_funds(fund_list_tushare_path + "fund_basic_O_D.csv"))
    fund_code_list_tushare.extend(load_funds(fund_list_tushare_path + "fund_basic_E.csv"))
    fund_list_tushare = []
    for item in fund_code_list_tushare:
        fund_list_tushare.append(item.split(".")[0])
    print("fund_code_list_tushare: ", len(fund_code_list_tushare))

    fund_list_akshare = load_funds(fund_list_akshare_path + "fund_em.csv")
    fund_list_akshare_add = list(set(fund_list_akshare).difference(set(fund_list_tushare)))  # fund_list_akshare中有而fund_list_tushare中没有的
    fund_list_akshare_add.sort()
    fund_code_list_akshare_add = []
    for item in fund_list_akshare_add:
        fund_code_list_akshare_add.append(item + ".OF")
    print("fund_code_list_akshare_add: ", len(fund_code_list_akshare_add))

    fund_code_list_tu_n_ak = fund_code_list_tushare + fund_code_list_akshare_add


    fund_file_list = os.listdir(fund_nav_path)
    fund_file_list.sort()
    fund_code_list_dir_add = []
    for item in fund_file_list:
        code = item.strip(".csv")
        if code not in fund_code_list_tu_n_ak:
            fund_code_list_dir_add.append(code)

    return fund_code_list_tu_n_ak + fund_code_list_dir_add


def update_fund_daily(code):
    fields = ['ts_code', 'trade_date', 'open', 'high', 'low',
                      'close', 'pre_close', 'change', 'pct_chg', 'vol',
                      'amount']
    file_path = fund_hist_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)

        if str(hist_data.iloc[-1]['trade_date']) == trade_date:
            return
        df = pd.DataFrame(pro.fund_daily(ts_code=code, fields=fields, start_date=str(hist_data.iloc[-1]['trade_date']))[::-1])
        df = df.drop_duplicates('trade_date')
        if df.empty:
            return
        else:
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.fund_daily(ts_code=code, fields=fields)[::-1])
        df = df.drop_duplicates('trade_date')
        if df.empty:
            return
        else:
            df.to_csv(file_path, index=False, encoding='UTF-8')



def update_fund_nav(code):
    fields = ['ts_code', 'ann_date', 'end_date', 'unit_nav', 'accum_nav',
              'accum_div', 'net_asset', 'total_netasset', 'adj_nav']
    file_path = fund_nav_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)

        if str(hist_data.iloc[-1]['end_date']) == trade_date:
            return
        df = pd.DataFrame(pro.fund_nav(ts_code=code, fields=fields, start_date=str(hist_data.iloc[-1]['end_date']))[::-1])
        df = df.drop_duplicates('end_date')
        if df.empty:
            return
        else:
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.fund_nav(ts_code=code, fields=fields)[::-1])
        df = df.drop_duplicates('end_date')
        if df.empty:
            return
        else:
            df.to_csv(file_path, index=False, encoding='UTF-8')


def fun_fund_daily(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                # 每分钟最多调用250次，内场日线
                time.sleep(0.3)
                print(index, " daily fund code: ", code)
                update_fund_daily(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_fund_daily(items, code)


def fun_fund_nav(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                # 每分钟最多调用80次，基金净值
                time.sleep(0.8)
                print(index, " nav fund code: ", code)
                update_fund_nav(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_fund_nav(items, code)


if __name__ == '__main__':
    fund_list_e = load_funds(fund_list_tushare_path + "fund_basic_E.csv")
    fun_fund_daily(fund_list_e)

    fund_list = get_fund_list()
    fun_fund_nav(fund_list)