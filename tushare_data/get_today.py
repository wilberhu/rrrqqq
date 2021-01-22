import datetime
import pytz

def get_today():
    # trade_date = '20200430'

    time_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
    time_h = int(time_now.strftime("%H"))
    time_w = int(time_now.strftime("%w"))
    if time_h > 17:  # 17点之前
        if time_w == 6:  # 星期六
            trade_date = (time_now - datetime.timedelta(days=1)).strftime("%Y%m%d")
        elif time_w == 0:  # 星期天
            trade_date = (time_now - datetime.timedelta(days=2)).strftime("%Y%m%d")
        else:
            trade_date = (time_now).strftime("%Y%m%d")
    else:  # 17点之后
        if time_w == 0:  # 星期天
            trade_date = (time_now - datetime.timedelta(days=2)).strftime("%Y%m%d")
        elif time_w == 1:  # 星期一
            trade_date = (time_now - datetime.timedelta(days=3)).strftime("%Y%m%d")
        else:
            trade_date = (time_now - datetime.timedelta(days=1)).strftime("%Y%m%d")
    return trade_date