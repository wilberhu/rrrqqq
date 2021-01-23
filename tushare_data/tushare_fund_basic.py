import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.get_today import get_today
from tushare_data.df2sql import df2sql, truncate_table

trade_date = get_today()

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

fund_basic_path = "tushare_data/data/tush_fund_basic/"

# 公募基金数据列表
def save_fund_basic():
    fields = ['ts_code', 'name', 'management', 'custodian', 'fund_type',
              'found_date', 'due_date', 'list_date', 'issue_date', 'delist_date',
              'issue_amount', 'm_fee', 'c_fee', 'duration_year', 'p_value',
              'min_amount', 'exp_return', 'benchmark', 'status', 'invest_type',
              'type', 'trustee', 'purc_startdate', 'redm_startdate', 'market']
    df = pro.fund_basic(market='E', fields=fields)
    df.to_csv(fund_basic_path + "fund_basic_E.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_E.csv", index=False)

    df = pro.fund_basic(market='O', status="D", fields=fields)
    df.to_csv(fund_basic_path + "fund_basic_O_D.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_D.csv", index=False)

    df = pro.fund_basic(market='O', status="I", fields=fields)
    df.to_csv(fund_basic_path + "fund_basic_O_I.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_I.csv", index=False)

    df = pro.fund_basic(market='O', status="L", fields=fields)
    df.to_csv(fund_basic_path + "fund_basic_O_L.csv", index=False)
    df.to_csv(fund_basic_path + trade_date + "_fund_basic_O_L.csv", index=False)


if __name__ == '__main__':
    # save_fund_basic()


    print("~~~~~~~~~~~")
    data1 = pd.read_csv(fund_basic_path + "fund_basic_O_L.csv")
    data2 = pd.read_csv(fund_basic_path + "fund_basic_O_I.csv")
    data3 = pd.read_csv(fund_basic_path + "fund_basic_O_D.csv")
    data4 = pd.read_csv(fund_basic_path + "fund_basic_E.csv")
    df = pd.concat([data1, data2, data3, data4], axis=0, join='outer', ignore_index=True,
           keys=None, levels=None, names=None, verify_integrity=False, copy=True)
    df = df.fillna('')

    table_name = "tush_fundbasic"
    truncate_table(table_name)
    df2sql(df=df, table_name=table_name)