""" 
단위테스트 실행
경로 : D:\DevProject\stock-lab
workon stocklab
 python -m unittest tests.test_web_crawling
"""

import unittest
# from common.agent.ebest import EBest
from common.agent.naver_news_crawler import NaverNewsCrawler
import inspect
import time
from common.db_handler.mongodb_handler import MongoDBHandler
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
import urllib.request
from urllib.parse import quote
import pandas as pd

class TestWebCrawling(unittest.TestCase):
    def setUp(self):
        print("TestWebCrawling setUp 시작")
        self.crawler = NaverNewsCrawler()
        # self.ebest = EBest("DEMO")
        # self.ebest.login()
        # self.mongodb = MongoDBHandler()
        
    def test_WebCrawling_from_NAVER(self):

        # info_main = input("="*50+"\n"+"입력 형식에 맞게 입력해주세요."+"\n"+" 시작하시려면 Enter를 눌러주세요."+"\n"+"="*50)
        
        # maxpage = input("최대 크롤링할 페이지 수 입력하시오: ")  
        # query = input("검색어 입력: ")  
        # sort = input("뉴스 검색 방식 입력(관련도순=0  최신순=1  오래된순=2): ")    #관련도순=0  최신순=1  오래된순=2
        # s_date = input("시작날짜 입력(2019.01.04):")  #2019.01.04
        # e_date = input("끝날짜 입력(2019.01.05):")        
        
        self.crawler.crawler(1000, "삼성전자","1","2021.05.01","2021.05.01")
        # self.crawler.crawler(1, query,sort,s_date,e_date)
    # def test_WebCrawling_from_NAVER(self):

    #     news_df = pd.DataFrame(columns=("Title", "Link", "Press", "Datetime", "Article"))
    #     url_query = quote("인공지능")

    #     url = "https://search.naver.com/search.naver?where=news&sm=tab_jum&query=%EC%9D%B8%EA%B3%B5%EC%A7%80%EB%8A%A5"
    #     idx = 0
    #     "https://search.naver.com/search.naver?where=news&sm=tab_pge&query="+url_query+"&sort=0&photo=0&field=0&pd=0&ds=&de=&cluster_rank=49&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:r,p:all,a:all&start="+page_id

    #     search_url = urllib.request.urlopen(url).read()
    #     soup = BeautifulSoup(search_url, 'html.parser')
    #     links = soup.find_all('a', {'class':'info'})

    #     for link in links:
    #         new_url = link.find('a').get('href')

    #         if(new_url == '#'):
    #             continue
    #         else:
    #             news_link = urllib.request.urlopen(new_url).read()
    #             news_html = BeautifulSoup(new_link, 'html.parser')
    #             title = news_html.find('h3', {'id':'articleTitle'}).get_text()
    #             datetime = news_html.find('span', {'class':'t11'}).get_text()
    #             article = news_html.find('div', {'id':'articleBodyContents'}).get_text()

    #             news_df.loc[idx] = [title, new_url, press, datetime, article]
    #             idx += 1
    #             print("#", end="")


    #     next = soup.find('a')
    #     print("start")

    def test_WebCrawling_from_DART(self):
        print("start")


 

    def tearDown(self):
        print("TestWebCrawling Tear Down 종료")
        # self.ebest.logout()

        
if __name__ == "__main__":
    unittest.main()        

