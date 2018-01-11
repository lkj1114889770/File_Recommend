import csv
import pymysql
import numpy as np


def savedata():
    conn = pymysql.connect(host='10.110.43.140', port=3306, user='###', passwd='#####', use_unicode=True, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES utf8;')
    cur.execute('SET CHARACTER SET utf8;')
    cur.execute('SET character_set_connection=utf8;')
    #将数据存进数据库中
    csv_reader = csv.reader(open('./datas/movies_metadata.csv', encoding='utf-8'))
    rows = [row for row in csv_reader]
    datas = []
    for i in range(len(rows) - 1):
        datas.append(rows[i + 1])
    # print(len(datas[-1]))
    try:
        # cur.execute("create database if not exists db_movies")
        cur.execute("use db_movies")
        cur.execute("drop table if exists tb_movies")
        sql = "create table tb_movies (%s VARCHAR(100))" % ('movie_' + rows[0][0])
        cur.execute(sql)
        conn.commit()
    except:
        conn.rollback()
        return 1
    for attr in rows[0]:
        if attr == 'adult':
            continue
        size = 512
        if attr == 'belongs_to_collection' or attr == 'production_companies' or attr == 'genres'\
                or attr == '' or attr == 'overview' or attr == 'production_countries' or attr == 'spoken_languages': size = 2048
        attr = 'movie_' + attr
        try:
            sql = "alter table tb_movies add %s VARCHAR(%d) character set utf8" % (attr, size)
            cur.execute(sql)
            conn.commit()
        except:
            conn.rollback()
            return 2
        # print(sql)
        count = 0
    for data in datas:
        count = count + 1
        if len(data) < 24:
            continue
        sql = """insert into `tb_movies` values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""\
              % (repr(data[0]), repr(data[1]), repr(data[2]), repr(data[3]), repr(data[4]), repr(data[5]), repr(data[6]), repr(data[7]),\
                 repr(data[8]), repr(data[9]), repr(data[10]), repr(data[11]), repr(data[12]), repr(data[13]), repr(data[14]), repr(data[15]),\
                 repr(data[16]), repr(data[17]), repr(data[18]), repr(data[19]), repr(data[20]), repr(data[21]), repr(data[22]), repr(data[23]))
        # print(sql)
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(str(e))
            conn.rollback()
            return count
    cur.close()
    conn.close()
    return 0


def loaddata(database):
    conn = pymysql.connect(host='10.110.43.140', port=3306, user='lkj', passwd='0818lkj', db=database, use_unicode=True, charset='utf8')
    cur = conn.cursor()
    sql = "select * from `tb_movies`"
    cur.execute(sql)
    movies = cur.fetchall()
    cur.close()
    conn.close()
    return movies


def saveTop(database, path, type):
    conn = pymysql.connect(host='10.110.43.140', port=3306, user='lkj', passwd='0818lkj', use_unicode=True,
                           charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES utf8;')
    cur.execute('SET CHARACTER SET utf8;')
    cur.execute('SET character_set_connection=utf8;')
    # 将数据存进数据库中
    csv_reader = csv.reader(open(path, encoding='utf-8'))
    rows = [row for row in csv_reader]
    datas = []
    for i in range(len(rows) - 1):
        datas.append(rows[i + 1])
    try:
        cur.execute("drop database if exists %s" % database)
        cur.execute("create database %s" % database)
        cur.execute("use %s" % database)
        cur.execute("drop table if exists tb_movies")
        sql = "create table tb_movies (%s VARCHAR(100))" % ('movie_' + rows[0][0])
        cur.execute(sql)
        conn.commit()
    except:
        conn.rollback()
        return 1
    for attr in rows[0]:
        size = 100
        if attr == rows[0][0]: continue
        try:
            sql = "alter table tb_movies add %s VARCHAR(%d) character set utf8" % (attr, size)
            cur.execute(sql)
            conn.commit()
        except:
            conn.rollback()
            return 2
    count = 0
    if type == 'TOP':
        for data in datas:
            count = count + 1
            sql = "insert into `tb_movies` values (%s, %s, %s, %s, %s, %s, %s)"\
                  % (repr(data[0]), repr(data[1]), repr(data[2]), repr(data[3]), repr(data[4]), repr(data[5]), repr(data[6]))
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print(str(e))
                conn.rollback()
                return count
    elif type == 'rec':
        for data in datas:
            count = count + 1
            sql = "insert into `tb_movies` values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"\
                  % (repr(data[0]), repr(data[1]), repr(data[2]), repr(data[3]), repr(data[4]), repr(data[5]), repr(data[6]),\
                     repr(data[7]), repr(data[8]), repr(data[9]), repr(data[10]))
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print(str(e))
                conn.rollback()
                return count
    cur.close()
    conn.close()
    return 0


if __name__ == '__main__':
    # print(savedata())
    movies = loaddata('db_movies')
    print(movies[0])