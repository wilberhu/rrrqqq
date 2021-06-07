import tushare as ts
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from tushare_data.tushare_fund_hist_data import get_fund_list

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

import time

fund_portfolio_path = 'tushare_data/data/tush_fund_portfolio/'


# 持股数据获取
def update_fund_portfolio(code):
    fields = ['ts_code', 'ann_date', 'end_date', 'symbol', 'mkv',
                'amount', 'stk_mkv_ratio', 'stk_float_ratio']
    file_path = fund_portfolio_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)

        trade_date = str(hist_data.iloc[-1]['end_date'])

        df = pd.DataFrame(pro.fund_portfolio(ts_code=code, fields=fields, start_date=trade_date)[::-1])
        if df.empty:
            return
        else:
            df = df[~df['end_date'].isin([trade_date])]
            # 通过~取反，选取不包含trade_date的行
            df.to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.fund_portfolio(ts_code=code, fields=fields)[::-1])
        if df.empty:
            return
        else:
            df.to_csv(file_path, index=False, encoding='UTF-8')


def fun_fund_portfolio(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                time.sleep(1)
                print(index, " fund code: ", code)
                update_fund_portfolio(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_fund_portfolio(items, code)


if __name__ == '__main__':

    fund_list = get_fund_list()
    fund_list.sort()
    fun_fund_portfolio(fund_list)

