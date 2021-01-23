import akshare as ak

import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")

fund_em_path = "tushare_data/data/aksh_fund_em/"

# 公募基金数据列表
def save_fund_em():
    fund_em_fund_name_df = ak.fund_em_fund_name()
    fund_em_fund_name_df.to_csv(fund_em_path + "fund_em.csv", index=False)

if __name__ == '__main__':
    save_fund_em()