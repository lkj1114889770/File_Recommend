from django.db import models
import pymysql

# Create your models here.


def getData(table, number, dataType='tuple'):
    if dataType == 'dict':
        conn = pymysql.connect(host='10.110.43.140', port=3306, user='lkj', passwd='0818lkj', db='db_movies', use_unicode=True,
                           charset='utf8',  cursorclass=pymysql.cursors.DictCursor)
    else:
        conn = pymysql.connect(host='10.110.43.140', port=3306, user='lkj', passwd='0818lkj', db='db_movies',
                               use_unicode=True, charset='utf8')
    cur = conn.cursor()
    cur.execute("select * from %s limit 0, %d" % (table, number))
    datas = cur.fetchall()
    cur.close()
    conn.close()
    return datas
