# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 19:48:16 2018

@author: lkj
"""

import pandas as pd
import numpy as np
from surprise import Reader, Dataset, SVD, evaluate
from collections import defaultdict
import warnings; warnings.simplefilter('ignore')


reader = Reader()
ratings = pd.read_csv('ratings_small.csv')

#从DataFrame导入数据
data = Dataset.load_from_df(ratings[['userId', 'movieId', 'rating']], reader)
data.split(n_folds=5)
trainset = data.build_full_trainset()
#SVD算法
algo = SVD()
evaluate(algo, data, measures=['RMSE', 'MAE'])

#训练模型
algo.train(trainset)
#对用户未评价的电影生成测试集
testset = trainset.build_anti_testset()
predictions = algo.test(testset)  #预测测试集结果


def get_top_n(predictions, n=10):
    '''对预测结果中的每个用户，返回n部电影，默认n=10
    返回值一个字典，包括：
    keys 为原始的userId，以及对应的values为一个元组
        [(raw item id, rating estimation), ...].
    '''

    # 预测结果取出，对应每个userId.
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, est))
    # 排序取出前n个
    for uid, user_ratings in top_n.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        top_n[uid] = user_ratings[:n]
    return top_n

top_n = get_top_n(predictions, n=10)
rec_result=np.zeros((671,11))  #定义二维矩阵来存放结果
i=0
for uid, user_ratings in top_n.items():
    rec_result[i,0]=uid
    rec_result[i,1:]=[iid for (iid, _) in user_ratings]
    i=i+1
rec_result=rec_result.astype('int')

#转变成DataFrame
rec_result=pd.DataFrame(rec_result,columns=['userId','rec1','rec2','rec3','rec4','rec5',
                                          'rec6','rec7','rec8','rec9','rec10'])
    
####################
##下面开始从推荐电影的id到实际电影名字的映射
md=pd.read_csv('movies_metadata.csv')
ratings = pd.read_csv('ratings_small.csv')
links_small = pd.read_csv('links_small.csv')
links_small=links_small.dropna()
links_small['tmdbId'] = links_small['tmdbId'].astype('int')
smd = md[md['id'].isin(links_small)]


#从id到movie的映射函数
def id2movie(idd):
    #print(idd)
    link=links_small[links_small.movieId==idd].tmdbId
    if len(link)==0:
        return ''
    a=smd[smd.id==int(link)]['title']
    if len(a)==0:
        b=md[md.id==int(links_small[links_small.movieId==idd].tmdbId)]['title']
        if len(b)==0:
            return ''
        else:
            return b.values[0]
    else:
        return a.values[0]

for i in range(1,11):
    rec_result['rec'+str(i)]=rec_result['rec'+str(i)].apply(id2movie)
rec_result.to_csv('rec_movie.csv',index=False)

