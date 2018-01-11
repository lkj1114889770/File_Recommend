# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 18:30:59 2018

@author: lkj
"""

import pandas as pd
from ast import literal_eval
import warnings; warnings.simplefilter('ignore')
import numpy as np
import pymysql

#从数据库中读取电影数据
#连接数据库
conn = pymysql.connect(host='10.110.43.140',port= 3306,user = '###',passwd='####',db='sys') #db：库名
#创建游标
cur = conn.cursor()
#从数据库中读取
md=pd.read_sql('SELECT * FROM db_movies.tb_movies;',conn)
cur.close()
conn.close()
###替换列名等操作，和后面的代码兼容
md.columns=['adult', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'id',
       'imdb_id', 'original_language', 'original_title', 'overview',
       'popularity', 'poster_path', 'production_companies',
       'production_countries', 'release_date', 'revenue', 'runtime',
       'spoken_languages', 'status', 'tagline', 'title', 'video',
       'vote_average', 'vote_count']

md['vote_count']=md['vote_count'].astype('int')
md['vote_average']=md['vote_average'].astype('float')
md = md.drop([19730, 29503, 35587])  #这三行id的格式不对，删除先
md['id'] = md['id'].astype('int')


md['genres'] = md['genres'].fillna('[]').apply(literal_eval).apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else [])#将电影类型解析出来
md['year'] = pd.to_datetime(md['release_date'], errors='coerce').apply(lambda x: str(x).split('-')[0] if x != np.nan else np.nan)
vote_counts = md[md['vote_count'].notnull()]['vote_count'].astype('int')
vote_averages = md[md['vote_average'].notnull()]['vote_average'].astype('int')
C = vote_averages.mean()  #所有电影的平均得分
m = vote_counts.quantile(0.95)  #投票数在0.95中位数之前才计入TOP
#满足投票数要求的电影
qualified = md[(md['vote_count'] >= m) & (md['vote_count'].notnull()) & (md['vote_average'].notnull())][['title', 'year', 'vote_count', 'vote_average', 'popularity', 'genres']]
qualified['vote_count'] = qualified['vote_count'].astype('int')
qualified['vote_average'] = qualified['vote_average'].astype('int')

#贝叶斯统计算法来计算电影得分
def weighted_rating(x):
    v = x['vote_count']
    R = x['vote_average']
    return (v/(v+m) * R) + (m/(m+v) * C)
#所有电影的TOP250
qualified['wr'] = qualified.apply(weighted_rating, axis=1)
qualified = qualified.sort_values('wr', ascending=False).head(250)
qualified.to_csv('TOP250_movie.csv',index=False)

########################
#下面对特定类型的电影TOP进行分析
s = md.apply(lambda x: pd.Series(x['genres']),axis=1).stack().reset_index(level=1, drop=True)
s.name = 'genre'
gen_md = md.drop('genres', axis=1).join(s)   #合并

#选取特定类型电影的TOP250函数
def build_chart(genre, percentile=0.85):
    df = gen_md[gen_md['genre'] == genre]
    vote_counts = df[df['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = df[df['vote_average'].notnull()]['vote_average'].astype('int')
    C = vote_averages.mean()
    m = vote_counts.quantile(percentile)
    
    qualified = df[(df['vote_count'] >= m) & (df['vote_count'].notnull()) & (df['vote_average'].notnull())][['title', 'year', 'vote_count', 'vote_average', 'popularity']]
    qualified['vote_count'] = qualified['vote_count'].astype('int')
    qualified['vote_average'] = qualified['vote_average'].astype('int')
    
    qualified['wr'] = qualified.apply(lambda x: (x['vote_count']/(x['vote_count']+m) * x['vote_average']) + (m/(m+x['vote_count']) * C), axis=1)
    if len(qualified)>250:
        qualified = qualified.sort_values('wr', ascending=False).head(250)
    return qualified

'''
只有这些电影类型超过250部
Drama                                    20265
Comedy                                   13182
Thriller                                  7624
Romance                                   6735
Action                                    6596
Horror                                    4673
Crime                                     4307
Documentary                               3932
Adventure                                 3496
Science Fiction                           3049
Family                                    2770
Mystery                                   2467
Fantasy                                   2313
Animation                                 1935
Foreign                                   1622
Music                                     1598
History                                   1398
War                                       1323
Western                                   1042
TV Movie                                   767

'''
###以超过3000电影数量为主要类型，再进行TOP250排序
movie_types=['Drama','Comedy','Thriller','Romance','Action','Horror','Crime','Documentary','Science Fiction']
for movie_type in movie_types:
    movie=build_chart(movie_type)
    movie.to_csv('TOP250_movie_'+movie_type+'.csv',index=False)