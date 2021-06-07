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

fund_basic_path = "tushare_data/data/tush_fund_basic/"

tushare_fund_fields = ['ts_code', 'name', 'management', 'custodian', 'fund_type',
                       'found_date', 'due_date', 'list_date', 'issue_date', 'delist_date',
                       'issue_amount', 'm_fee', 'c_fee', 'duration_year', 'p_value',
                       'min_amount', 'exp_return', 'benchmark', 'status', 'invest_type',
                       'type', 'trustee', 'purc_startdate', 'redm_startdate', 'market']

def load_funds(path):
    df = pd.read_csv(path, dtype=str)
    return list(df['ts_code'])

# 公募基金数据列表
def save_aksh_fund_em():
    fund_em_fund_name_df = ak.fund_em_fund_name()
    fund_em_fund_name_df.rename(columns={"基金代码": "ts_code", "拼音缩写": "phonetic_name", "基金简称": "name", "基金类型": "type", "拼音全称": "phonetic_full_name"}, inplace=True)
    print("akshare", fund_em_fund_name_df.shape)
    fund_em_fund_name_df.to_csv(fund_list_akshare_path + "fund_em.csv", index=False)
    fund_em_fund_name_df.to_csv(fund_list_akshare_path + trade_date + "_fund_em.csv", index=False)


# 公募基金数据列表
def save_tush_fund_basic():
    fields = ['ts_code', 'name', 'management', 'custodian', 'fund_type',
              'found_date', 'due_date', 'list_date', 'issue_date', 'delist_date',
              'issue_amount', 'm_fee', 'c_fee', 'duration_year', 'p_value',
              'min_amount', 'exp_return', 'benchmark', 'status', 'invest_type',
              'type', 'trustee', 'purc_startdate', 'redm_startdate', 'market']
    df = pro.fund_basic(market='E', fields=fields)
    print("tushare, E", df.shape)
    df.to_csv(fund_basic_path + "fund_basic_E.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_E.csv", index=False)

    df = pro.fund_basic(market='O', status="D", fields=fields)
    print("tushare, O, D", df.shape)
    df.to_csv(fund_basic_path + "fund_basic_O_D.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_D.csv", index=False)

    df = pro.fund_basic(market='O', status="I", fields=fields)
    print("tushare, O, I", df.shape)
    df.to_csv(fund_basic_path + "fund_basic_O_I.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_I.csv", index=False)

    df = pro.fund_basic(market='O', status="L", fields=fields)
    print("tushare, O, L", df.shape)
    df.to_csv(fund_basic_path + "fund_basic_O_L.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_L.csv", index=False)


def get_more_fund_list():


    fund_list = []
    if os.path.exists(fund_list_path + "fund_list.txt"):
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

    return fund_list_akshare_add, fund_list_other_add


def fun_fund(items, startCode=None):
    df = pd.DataFrame(columns=tushare_fund_fields)
    df.to_csv(fund_list_tushare_path + file_name, index=False, encoding='UTF-8')
    df.to_csv(fund_list_tushare_path + trade_date + "_" + file_name, index=False, encoding='UTF-8')

    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                time.sleep(8)
                print(index, " fund code: ", code)
                # 每分钟最多访问该接口10次
                df = pro.fund_basic(ts_code=code + ".OF", fields=tushare_fund_fields)
                df.to_csv(fund_list_tushare_path + file_name, index=False, header=0, mode='a', encoding='UTF-8')
                df.to_csv(fund_list_tushare_path + trade_date + "_" + file_name, index=False, header=0, mode='a', encoding='UTF-8')
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(20)
        fun_fund(items, code)


if __name__ == '__main__':
    save_tush_fund_basic()

    save_aksh_fund_em()
    fund_list_akshare_add, fund_list_other_add = get_more_fund_list()

    file_name = "fund_basic_akshare_add.csv"
    fun_fund(fund_list_akshare_add)

    file_name = "fund_basic_other_add.csv"
    fun_fund(fund_list_other_add)
