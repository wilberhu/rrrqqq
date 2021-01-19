python tushare_data/stock_basics_to_sql_pro.py
python tushare_data/daily_basic_pro.py
python tushare_data/hist_data_update_pro.py
python tushare_data/today_pro.py
# python tushare_data/trade_cal_pro.py # 每年运行
# python tushare_data/fina_indacators_update_pro.py # 每季度运行


rm -fr rqalpha/bundle/*
rqalpha download-bundle -d rqalpha
