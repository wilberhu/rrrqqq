import akshare as ak
import tushare as ts
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")

from tushare_data.get_today import get_today
trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

import time

fund_list_path = r'tushare_data/data/tush_fund_list/'

fund_list_akshare_path = r'tushare_data/data/aksh_fund_em/'
fund_list_tushare_path = 'tushare_data/data/tush_fund_basic/'


def load_funds(path):
    df = pd.read_csv(path, dtype=str)
    return list(df['ts_code'])

# 公募基金数据列表
def save_fund_em():
    fund_em_fund_name_df = ak.fund_em_fund_name()
    fund_em_fund_name_df.rename(columns={"基金代码": "ts_code", "拼音缩写": "phonetic_name", "基金简称": "name", "基金类型": "type", "拼音全称": "phonetic_full_name"}, inplace=True)
    fund_em_fund_name_df.to_csv(fund_list_akshare_path + "fund_em.csv", index=False)
    fund_em_fund_name_df.to_csv(fund_list_akshare_path + trade_date + "_fund_em.csv", index=False)


def update_fund_list():
    fields = ['ts_code', 'name', 'management', 'custodian', 'fund_type',
              'found_date', 'due_date', 'list_date', 'issue_date', 'delist_date',
              'issue_amount', 'm_fee', 'c_fee', 'duration_year', 'p_value',
              'min_amount', 'exp_return', 'benchmark', 'status', 'invest_type',
              'type', 'trustee', 'purc_startdate', 'redm_startdate', 'market']


    fund_list = []
    f = open(fund_list_path + "fund_list.txt")
    for item in f.readlines():
        fund_list.append(item.strip('\n').split(".")[0])
    f.close()


    fund_list_akshare = load_funds(fund_list_akshare_path + "fund_em.csv")

    fund_list_tushare_temp = load_funds(fund_list_tushare_path + "fund_basic_O_L.csv")
    fund_list_tushare_temp.extend(load_funds(fund_list_tushare_path + "fund_basic_O_I.csv"))
    fund_list_tushare_temp.extend(load_funds(fund_list_tushare_path + "fund_basic_O_D.csv"))
    fund_list_tushare_temp.extend(load_funds(fund_list_tushare_path + "fund_basic_E.csv"))

    fund_list_tushare = []
    for item in fund_list_tushare_temp:
        fund_list_tushare.append(item.split(".")[0])

    fund_list_akshare_add = list(set(fund_list_akshare).difference(set(fund_list_tushare)))  # fund_list_akshare中有而fund_list_tushare中没有的
    fund_list_akshare_add.sort()
    print("fund_list_akshare_add: ", len(fund_list_akshare_add))



    fund_list_tushare_full = fund_list_tushare + fund_list_akshare_add
    fund_list_other_add = list(set(fund_list).difference(set(fund_list_tushare_full)))  # fund_list中有而fund_list_tushare_full中没有的
    fund_list_other_add.sort()
    print("fund_list_other_add: ", len(fund_list_other_add))


    fund_list_all = fund_list_tushare_full + fund_list_other_add
    fund_list_all.sort()
    f = open(fund_list_path + "fund_list.txt", "w")
    f.writelines('\n'.join(fund_list_all))
    f.close()

    f = open(fund_list_path + trade_date + "_fund_list.txt", "w")
    f.writelines('\n'.join(fund_list_all))
    f.close()


    df = pd.DataFrame(columns=fields)
    df.to_csv(fund_list_tushare_path + "fund_basic_akshare_add.csv", index=False, encoding='UTF-8')
    df.to_csv(fund_list_tushare_path + trade_date + "_fund_basic_akshare_add.csv", index=False, encoding='UTF-8')
    for item in fund_list_akshare_add:
        print(item)
        # 每分钟最多访问该接口10次
        time.sleep(6)
        df = pro.fund_basic(ts_code=item + ".OF", fields=fields)
        df.to_csv(fund_list_tushare_path + "fund_basic_akshare_add.csv", index=False, header=0, mode='a', encoding='UTF-8')
        df.to_csv(fund_list_tushare_path + trade_date + "_fund_basic_akshare_add.csv", index=False, header=0, mode='a', encoding='UTF-8')



    df = pd.DataFrame(columns=fields)
    df.to_csv(fund_list_tushare_path + "fund_basic_other_add.csv", index=False, encoding='UTF-8')
    df.to_csv(fund_list_tushare_path + trade_date + "_fund_basic_akshare_add.csv", index=False, encoding='UTF-8')
    for item in fund_list_other_add:
        print(item)
        # 每分钟最多访问该接口10次
        time.sleep(6)
        df = pro.fund_basic(ts_code=item + ".OF", fields=fields)
        df.to_csv(fund_list_tushare_path + "fund_basic_other_add.csv", index=False, header=0, mode='a', encoding='UTF-8')
        df.to_csv(fund_list_tushare_path + trade_date + "_fund_basic_other_add.csv", index=False, header=0, mode='a', encoding='UTF-8')


if __name__ == '__main__':
    save_fund_em()
    update_fund_list()