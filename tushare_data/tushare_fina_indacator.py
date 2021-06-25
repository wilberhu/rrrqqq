import tushare as ts
import pandas as pd
import os
import sys
import time

sys.path.insert(0, os.path.abspath('.'))
from tushare_data.df2sql import truncate_table
import sqlalchemy as sa
from sqlalchemy import create_engine

connect_info = 'mysql+pymysql://root:87654321@localhost:3306/stock_api?charset=utf8'
engine = create_engine(connect_info) #use sqlalchemy to build link-engine

#indicators路径
indicator_path=r'tushare_data/data/tush_fina_indicator/'
stock_basics_path = 'tushare_data/data/tush_stock_basic/stock_basic.csv'

table_name='tush_fina_indicators'

fields = ['ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps',
          'total_revenue_ps', 'revenue_ps', 'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps',
          'extra_item', 'profit_dedt', 'gross_margin', 'current_ratio', 'quick_ratio',
          'cash_ratio', 'invturn_days', 'arturn_days', 'inv_turn', 'ar_turn',
          'ca_turn', 'fa_turn', 'assets_turn', 'op_income', 'valuechange_income',
          'interst_income', 'daa', 'ebit', 'ebitda', 'fcff',
          'fcfe', 'current_exint', 'noncurrent_exint', 'interestdebt', 'netdebt',
          'tangible_asset', 'working_capital', 'networking_capital', 'invest_capital', 'retained_earnings',
          'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps',
          'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 'grossprofit_margin',
          'cogs_of_sales', 'expense_of_sales', 'profit_to_gr', 'saleexp_to_gr', 'adminexp_of_gr',
          'finaexp_of_gr', 'impai_ttm', 'gc_of_gr', 'op_of_gr', 'ebit_of_gr',
          'roe', 'roe_waa', 'roe_dt', 'roa', 'npta',
          'roic', 'roe_yearly', 'roa2_yearly', 'roe_avg', 'opincome_of_ebt',
          'investincome_of_ebt', 'n_op_profit_of_ebt', 'tax_to_ebt', 'dtprofit_to_profit', 'salescash_to_or',
          'ocf_to_or', 'ocf_to_opincome', 'capitalized_to_da', 'debt_to_assets', 'assets_to_eqt',
          'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets', 'tbassets_to_totalassets', 'int_to_talcap',
          'eqt_to_talcapital', 'currentdebt_to_debt', 'longdeb_to_debt', 'ocf_to_shortdebt', 'debt_to_eqt',
          'eqt_to_debt', 'eqt_to_interestdebt', 'tangibleasset_to_debt', 'tangasset_to_intdebt',
          'tangibleasset_to_netdebt',
          'ocf_to_debt', 'ocf_to_interestdebt', 'ocf_to_netdebt', 'ebit_to_interest', 'longdebt_to_workingcapital',
          'ebitda_to_debt', 'turn_days', 'roa_yearly', 'roa_dp', 'fixed_assets',
          'profit_prefin_exp', 'non_op_profit', 'op_to_ebt', 'nop_to_ebt', 'ocf_to_profit',
          'cash_to_liqdebt', 'cash_to_liqdebt_withinterest', 'op_to_liqdebt', 'op_to_debt', 'roic_yearly',
          'total_fa_trun', 'profit_to_op', 'q_opincome', 'q_investincome', 'q_dtprofit',
          'q_eps', 'q_netprofit_margin', 'q_gsprofit_margin', 'q_exp_to_sales', 'q_profit_to_gr',
          'q_saleexp_to_gr', 'q_adminexp_to_gr', 'q_finaexp_to_gr', 'q_impair_to_gr_ttm', 'q_gc_to_gr',
          'q_op_to_gr', 'q_roe', 'q_dt_roe', 'q_npta', 'q_opincome_to_ebt',
          'q_investincome_to_ebt', 'q_dtprofit_to_profit', 'q_salescash_to_or', 'q_ocf_to_sales', 'q_ocf_to_or',
          'basic_eps_yoy', 'dt_eps_yoy', 'cfps_yoy', 'op_yoy', 'ebt_yoy',
          'netprofit_yoy', 'dt_netprofit_yoy', 'ocf_yoy', 'roe_yoy', 'bps_yoy',
          'assets_yoy', 'eqt_yoy', 'tr_yoy', 'or_yoy', 'q_gr_yoy',
          'q_gr_qoq', 'q_sales_yoy', 'q_sales_qoq', 'q_op_yoy', 'q_op_qoq',
          'q_profit_yoy', 'q_profit_qoq', 'q_netprofit_yoy', 'q_netprofit_qoq', 'equity_yoy',
          'rd_exp', 'update_flag']


def load_companies(path):
    df = pd.read_csv(path, dtype=str)
    return list(df['ts_code'])


def save_to_db(df):
    df.to_sql(table_name, engine, index=False, if_exists='replace')


def delete_from_db(df):
    if df.shape[0] == 0:
        return
    meta = sa.MetaData()

    # Map the Inventory table in your database to a SQLAlchemy object
    fina_indicators = sa.Table(table_name, meta, autoload=True, autoload_with=engine)

    # Build the WHERE clause of your DELETE statement from rows in the dataframe.
    # Equivalence in T-SQL
    #      WHERE (ts_code = ... AND end_date = ...) OR (ts_code = ... AND end_date = ...) OR (ts_code = ... AND end_date = ...)
    cond = df.apply(lambda row: sa.and_(fina_indicators.c['ts_code'] == row['ts_code'],
                                        fina_indicators.c['end_date'] == row['end_date']), axis=1)
    cond = sa.or_(*cond)

    # Define and execute the DELETE
    delete = fina_indicators.delete().where(cond)
    with engine.connect() as conn:
        conn.execute(delete)


def update_fina_indicators(code):
    file_path = indicator_path + code + '.csv'
    if os.path.exists(file_path):
        hist_data = pd.read_csv(file_path)
        df = pd.DataFrame(
            pro.fina_indicator(ts_code=code, fields=fields, start_date=str(hist_data.iloc[-1]['end_date']))[::-1])
        df = df.drop_duplicates('end_date')
        if df.empty:
            return
        else:
            delete_from_db(df[1:])
            save_to_db(df[1:])
            df[1:].to_csv(file_path, index=False, header=0, mode='a', encoding='UTF-8')
    else:
        df = pd.DataFrame(pro.fina_indicator(ts_code=code, fields=fields)[::-1])
        df = df.drop_duplicates('end_date')
        if df.empty:
            return
        else:
            delete_from_db(df)
            save_to_db(df)
            df.to_csv(file_path, index=False, encoding='UTF-8')


def fun_company(items, startCode=None):
    try:
        flag = False
        for index, code in enumerate(items):
            if code == startCode or startCode is None:
                flag = True
            if flag:
                time.sleep(2)
                print(index, " company code: ", code)
                update_fina_indicators(code)
    except:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Error")
        time.sleep(5)
        fun_company(items, code)


def save_fina_indicator_to_db(items, startCode=None):
    # truncate_table(table_name='tush_fina_indicators')
    flag = False
    for index, code in enumerate(items):
        if code == startCode or startCode is None:
            flag = True
        if flag:
            print("company code:", code)
            file_path = indicator_path + code + '.csv'
            if os.path.exists(file_path):
                time.sleep(1)
                df = pd.read_csv(file_path)
                save_to_db(df)

if __name__ == '__main__':
    pro = ts.pro_api(token='e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b')
    companies = load_companies(stock_basics_path)
    fun_company(companies)

    # save_fina_indicator_to_db(companies)