# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 16:16:02 2018

@author: Kyle Chen
"""

"""
把function跟處理部分分開比較好
"""

import sys
print(sys.path)
sys.path.append('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33_crawl')   #不知道為什麼不能直接import，就指定路徑去找
import CrawlForFinancial
import pandas as pd
import threading
import time

#讀取company list
companyList = pd.read_excel('C://Users/User/Desktop/CrawlForFinancial/33_crawl/10-K Company.xlsx')

#做一個cik和ticker的對照表(以備不時之需)
tickerToCompanyName = {str(companyList['Ticker'][i]): companyList['Company_Name'][i] for i in range(len(companyList))}

#取得CIK作為搜尋的關鍵字
ticker = list(tickerToCompanyName.keys())

#篩選條件
filterCondition = '10-K'

#篩選年份
years = set()
for i in range(2002, 2018):
    years.add(str(i))

#根據Ticker取得搜尋結果
searchResultsWithTicker = CrawlForFinancial.getSearchResultsWithTicker(ticker)

#根據篩選條件取得進階搜尋結果
searchAndFilterResults = CrawlForFinancial.getFilterResults(searchResultsWithTicker, filterCondition)

#根據篩選年份取得所要的document links
documents = {}
threads_d = []
lock_d = threading.Lock()

nonType = []
#把進階搜尋結果的網址放到threads裡
for fr in searchAndFilterResults:
    threads_d.append(CrawlForFinancial.MyDocumentLinks(fr, documents, years, nonType, lock_d))

"""
注意巔峰時段(晚上6點以後)
發包thread任務都請休息>0.2秒
不然會被鎖網路ip
"""
#執行所有thread任務
for td in threads_d:
    td.start()
    time.sleep(0.2)

#threads有快有慢，join()會等到全部的threads都執行完才繼續主程式
for td in threads_d:
    td.join()

#刪除少於8年的公司, 代表年份有缺   
keys = []
for d in documents:
    if len(documents.get(d)) < 8:
        keys.append(d)
for k in keys:
    documents.pop(k)

"""
#到目前為止, 把document links存在txt
os.getcwd() #先確認一下工作目錄在哪, 檔案會存在那
f = open('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33friend_crawl\\documentLinks.txt', 'w')
for d in documents:
    f.write(d + '\n')
    for dd in documents.get(d):
        f.write(dd + ' ' + documents.get(d).get(dd) + '\n')
f.close()

#下次可以不用執行前面, 直接從documentLinks取CIK, 年份, 網址
f = open('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33friend_crawl\\documentLinks.txt', 'r')
count = 1
documents = {}
for line in f.readlines():
    if (count-1)%9 == 0:
        cik=line[:-1]
        documents[cik] = {}
    else:
        documents[cik][line[:-1].split(' ')[0]] = line[:-1].split(' ')[1]
    count+=1
f.close()
"""

#把財報的html files網址抓下來
files = {}
threads_f = []
lock_f = threading.Lock()
filterType = 'DEF 14A'
for d in documents:
    threads_f.append(CrawlForFinancial.MyFileLinks(d, documents.get(d), files, filterType, lock_f))

#執行所有thread任務
for tf in threads_f:
    tf.start()
    time.sleep(0.2)
    
#threads有快有慢，join()會等到全部的threads都執行完才繼續主程式
for tf in threads_f:
    tf.join()

#刪除少於8年的公司, 代表年份有缺(確保一下)
keys = []
for f in files:
    if len(files.get(f)) < 8:
        keys.append(f)
for k in keys:
    files.pop(k)

"""
#到目前為止, 把file links存在txt
os.getcwd() #先確認一下工作目錄在哪, 檔案會存在那
f = open('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33friend_crawl\\FileLinks.txt', 'w')
for file in files:
    f.write(file + '\n')
    for ff in files.get(file):
        f.write(ff + ' ' + files.get(file).get(ff) + '\n')
f.close()

#下次可以不用執行前面, 直接從documentLinks取CIK, 年份, 網址
f = open('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33friend_crawl\\FileLinks.txt', 'r')
count = 1
files = {}
for line in f.readlines():
    if (count-1)%9 == 0:
        cik=line[:-1]
        files[cik] = {}
    else:
        files[cik][line[:-1].split(' ')[0]] = line[:-1].split(' ')[1]
    count+=1
f.close()
"""

#把財報的part抓下來
parts = {}
threads_p = []
lock_p = threading.Lock()
title = {'compensation discussion and analysis', 'compensation discussion & analysis'}

for f in files:
    threads_p.append(CrawlForFinancial.MyPartsOnline(f, files.get(f), parts, title, lock_p))
    
for tp in threads_p:
    tp.start()
    time.sleep(0.2)
    
for tp in threads_p:
    tp.join()