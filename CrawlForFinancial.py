# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 19:47:47 2018

@author: Kyle Chen
"""
import re
import requests
from bs4 import BeautifulSoup
import threading

"""
functions和class定義部分
"""
#依據CIK搜尋並取得連結
def getSearchResultsWithTicker(ticker):
        #Search results的網址前段, "?"後面接Filter Conditions
        searchUrlHead = 'https://www.sec.gov/cgi-bin/browse-edgar?'
        
        #依照CIK搜尋的結果
        searchResultsWithTicker = []
        
        for c in ticker:
            searchResultsWithTicker.append(searchUrlHead + 'CIK=' + c + '&owner=exclude&action=getcompany&Find=Search')
        
        #return 根據CIK搜尋的連結
        return searchResultsWithTicker

#進一步依據Filter Conditions去篩選結果並取得連結
def getFilterResults(searchResultsWithTicker, filterCondition):
    searchAndFilterResults = []

    for sr in searchResultsWithTicker:
        searchAndFilterResults.append(sr + '&type=' + filterCondition)
    
    #return 根據Filter搜尋的連結
    return searchAndFilterResults

#取得需求年份的Documents(multi threads)
class MyDocumentLinks(threading.Thread):
    def __init__(self, url, documents, years, nonType, lock):
        threading.Thread.__init__(self)
        self.url = url
        self.documents = documents
        self.years = years
        self.lock = lock
        self.nonType = nonType
        
    def run(self):
        #threads並不會共用function body內的local variables
        documentUrlHead = 'https://www.sec.gov'
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text)
        try:
            soupTable = soup.find('table', {'class': 'tableFile2'}) #連結所在的table
        except AttributeError:
            self.nonType.append(self.url)
            
        soupTr = soupTable.find_all('tr')   #把table的所有row找出來    
        tempDocumentLinks = {}
            
        for tr in soupTr:   #遍歷所有row
            soupTd = tr.find_all('td')  #把每個row的所有欄位找出來, 每個row有5個欄位: Filings, Format, Description, Filing Date, 	File/Film Number            
            count = 1
            
            for td in soupTd:   #遍歷每個欄位
                if count%4 == 0:    #如果是第4個欄位(即Filing Date)
                    if td.text[: 4] in self.years and td.text[: 4] not in tempDocumentLinks:   #篩選我們要的年份, 且重複的年份選擇靠後的
                        tempDocumentLinks[td.text[: 4]] = documentUrlHead + tr.find('a')['href']
                    
                count += 1
                
        regex = re.compile(r'[0-9]+')
        self.lock.acquire()  #只有在寫入documents時需要lock
        self.documents[regex.search(self.url).group(0)] = tempDocumentLinks
        self.lock.release()

#取得財報的html(with multi threads)
class MyFileLinks(threading.Thread):
    def __init__(self, cik, documentLinks, files, filterType, lock):
        threading.Thread.__init__(self)
        self.cik = cik
        self.documentLinks = documentLinks
        self.files = files
        self.filterType = filterType
        self.lock = lock
		
    def run(self):
        fileLinkHead = 'https://www.sec.gov'
        tempFileLinks = {}
		
        for dl in self.documentLinks:
            response = requests.get(self.documentLinks.get(dl))
            soup = BeautifulSoup(response.text)
            soupTable = soup.find('table', { 'class' : 'tableFile', 'summary' : 'Document Format Files' })
            soupTr = soupTable.find_all('tr')
            
            for tr in soupTr:
                soupTd = tr.find_all('td')
                count = 1
                for td in soupTd:
                    if count%4 == 0 and td.text == self.filterType:
                        tempFileLinks[dl] = fileLinkHead + tr.find('a')['href']
                        break                        
                    count += 1
                else:
                    continue
                break
		
        self.lock.acquire()
        self.files[self.cik] = tempFileLinks
        self.lock.release()
        
"""
先取得html的原始碼就好, 做太多處理ram不夠
線上: 符合title就抓取源碼
線下: 到另一個function去分part
"""
class MyPartsOnline(threading.Thread):
    def __init__(self, cik, fileLinks, parts, title, lock):
        threading.Thread.__init__(self)
        self.cik = cik
        self.fileLinks = fileLinks
        self.parts = parts
        self.title = title
        self.lock = lock
        
    def run(self):        
        for fl in self.fileLinks:
            response = requests.get(self.fileLinks.get(fl))
            html = response.text
            if html.lower() in self.title:  #title可能有很多塚表示法, 看看是否符合任何一種
                #符合就直接存起來
                self.lock.acquire()
                f = open('C:\\Users\\User\\Desktop\\CrawlForFinancial\\33friend_crawl\\DataHtml\\' + self.cik + '_' + fl + '.txt', 'w')
                f.write(html)
                f.close()
                self.lock.release()
            else:
                #要回傳一個東西讓我知道這間公司沒有
                break