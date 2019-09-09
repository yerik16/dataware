# -*- encoding: utf8 -*-
import psycopg2
from odps import ODPS
import datetime
import pandas as pd
import numpy as np
import re
import sys


# # odps
# ACCESS_ID = ''
# ACCESS_KEY = ''
# END_POINT = ''
# o = ODPS(access_id=ACCESS_ID, secret_access_key=ACCESS_KEY, project='', endpoint=END_POINT)

# pg
PG_HOST = ''
PG_PORT = 1111
PG_USER = ''
PG_PASSWD = ''
PG_DB = ''

pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWD, database=PG_DB)
pg_cur = pg_conn.cursor()


# MONGO_HOST = ''
# MONGO_PORT =
# MONGO_USER = ''
# MONGO_PASSWD = ''

# client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
# db = client.third_party_data
# db.authenticate(MONGO_USER, MONGO_PASSWD)





def main():
    dic_list = []

    dataframe = pd.read_excel('.xlsx',sheet_name='Sheet1')


    # dataframe = pd.DataFrame(dic_list)  # 转换成dataframe
    # dataframe[''] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # df.where 满足条件保持不变，否则用 None 代替
    dataframe = dataframe.where(pd.notnull(dataframe), None)  # convert NaN to None

    c_str = ''
    d_str = ','.join(map(lambda x: '%s', dataframe.columns))
    sql = "insert into xxx.xxx ({}) VALUES ({})".format(c_str, d_str)

    try:
        # 这里的写入需要保持幂等性
        pg_cur.execute("truncate table xxx.xxx")
        pg_cur.executemany(sql, dataframe.values)
        pg_conn.commit()
    except Exception as e:
        print(e)
    finally:
        pg_cur.close()
        pg_conn.close()

    # todo：写入 odps


if __name__ == '__main__':
    main()



