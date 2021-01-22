import os
import sys
sys.path.insert(0, os.path.abspath('.'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_api.settings")
from django.db import connection

def df2sql(df, table_name, dtype=None):
    if dtype is None:
        dtype = {}
    if df.empty:
        return
    df = df.fillna('null')
    columns = df.columns.tolist()

    sql_list = []

    sql_list.append("replace into " + table_name + "(")
    for i in columns:
        sql_list.append("`" + i + "`")
        sql_list.append(",")
    sql_list.pop()
    sql_list.append(")")

    sql_list.append(" values ")

    for i in df.index:
        row = df.loc[i].values

        sql_list.append("\n(")

        for index, item in enumerate(row):
            if columns[index] in dtype and (dtype[columns[index]] == float or dtype[columns[index]] == int):
                if str(item).strip() == "":
                    sql_list.append("null")
                else:
                    sql_list.append(str(item))
            else:
                if item == '':
                    sql_list.append("null")
                else:
                    sql_list.append('"' + str(item).replace('"', '\\"')  + '"')
            sql_list.append(",")
        sql_list.pop()
        sql_list.append(")")
        sql_list.append(",")
    sql_list.pop()
    sql = "".join(sql_list)
    # print(sql)

    #存入数据库
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()


if __name__ == '__main__':
    dtype = {'ts_code': str}
    print(dtype['ts_code'] == str)
    print(dtype['ts_code'] == int)
    print(dtype['ts_code'] == float)