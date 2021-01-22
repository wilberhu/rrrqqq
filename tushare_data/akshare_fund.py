import akshare as ak
import tushare as ts

import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")

pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')

fund_em_path = "tushare_data/data/fund_em/"

# 大盘指数行情
def save_fund_em():
    fund_em_fund_name_df = ak.fund_em_fund_name()
    fund_em_fund_name_df.to_csv(fund_em_path + "fund_em.csv", index=False)


# 大盘指数行情
def save_fund_basic():
    df = pro.fund_basic(market='E')
    df.to_csv(fund_em_path + "fund_basic_E.csv", index=False)

    df = pro.fund_basic(market='O', status="D")
    df.to_csv(fund_em_path + "fund_basic_O_D.csv", index=False)

    df = pro.fund_basic(market='O', status="I")
    df.to_csv(fund_em_path + "fund_basic_O_I.csv", index=False)

    df = pro.fund_basic(market='O', status="L")
    df.to_csv(fund_em_path + "fund_basic_O_L.csv", index=False)


if __name__ == '__main__':
    save_fund_em()
    save_fund_basic()