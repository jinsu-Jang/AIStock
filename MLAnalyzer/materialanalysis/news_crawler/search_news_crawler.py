from bs4 import BeautifulSoup
from datetime import datetime
import requests
import urllib.request
from urllib.parse import quote
import configparser
import pandas as pd
import re

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''
< naver 뉴스 검색시 리스트 크롤링하는 프로그램 > _select사용
- 크롤링 해오는 것 : 링크,제목,신문사,날짜,내용요약본
- 날짜,내용요약본  -> 정제 작업 필요
- 리스트 -> 딕셔너리 -> df -> 엑셀로 저장 
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''

class NaverNewsCrawler():
    #각 크롤링 결과 저장하기 위한 리스트 선언 
    # title_text=[]
    # link_text=[]
    # source_text=[]
    # date_text=[]
    # contents_text=[]
    # result={}

    #엑셀로 저장하기 위한 변수
    
    # now = datetime.now() #파일이름 현 시간으로 저장하기

    def __init__(self):
        config = configparser.RawConfigParser()
        config.read('conf/config.ini')
        self.title_text=[]
        self.link_text=[]
        self.press_text=[]
        self.info_text=[]
        self.date_text=[]
        self.contents_text=[]
        self.article_text=[]
        self.result={}      
        self.RESULT_PATH ='D:/crawling_result/Naver_News/'  #결과 저장할 경로
        self.now = datetime.now()  #파일이름 현 시간으로 저장하기

    #날짜 정제화 함수
    def date_cleansing(self, test):
        try:
            #지난 뉴스
            #머니투데이  10면1단  2018.11.05.  네이버뉴스   보내기  
            pattern = '\d+.(\d+).(\d+).'  #정규표현식 
        
            r = re.compile(pattern)
            match = r.search(test).group(0)  # 2018.11.05.
            date_text.append(match)
            
        except AttributeError:
            #최근 뉴스
            #이데일리  1시간 전  네이버뉴스   보내기  
            pattern = '\w* (\d\w*)'     #정규표현식 
            
            r = re.compile(pattern)
            match = r.search(test).group(1)
            #print(match)
            date_text.append(match)

    #내용 정제화 함수 
    def contents_cleansing(self, contents):
        first_cleansing_contents = re.sub('<dl>.*?</a> </div> </dd> <dd>', '', 
                                        str(contents)).strip()  #앞에 필요없는 부분 제거
        second_cleansing_contents = re.sub('<ul class="relation_lst">.*?</dd>', '', 
                                        first_cleansing_contents).strip()#뒤에 필요없는 부분 제거 (새끼 기사)
        third_cleansing_contents = re.sub('<.+?>', '', second_cleansing_contents).strip()
        contents_text.append(third_cleansing_contents)
        #print(contents_text)

    def get_article(self, url):
        text = ""
        if url is None:
            return ""

        try:    
            print("news url", url)
            news_link = urllib.request.urlopen(url).read()  
            soup = BeautifulSoup(news_link, 'html.parser')    
        except:
            return ""
              
        # 연합뉴스
        if ('://yna' in url) | ('app.yonhapnews' in url): 
            main_article = soup.find('div', {'class':'story-news article'})
            if main_article == None:
                main_article = soup.find('div', {'class' : 'article-txt'})
                if main_article == None:
                    main_article = soup.find('article')

            if main_article != None:        
                text = main_article.text
            else:
                text = ""
            
        # MBC 
        elif '//imnews.imbc' in url: 
            text = soup.find('div', {'itemprop' : 'articleBody'}).text
            
        # 매일경제(미라클), req.encoding = None 설정 필요
        elif 'mirakle.mk' in url:
            text = soup.find('div', {'class' : 'view_txt'}).text
            
        # 매일경제, req.encoding = None 설정 필요
        elif 'mk.co' in url:
            text = soup.find('div', {'class' : 'art_txt'}).text
            
        # SBS
        elif 'news.sbs' in url:
            text = soup.find('div', {'itemprop' : 'articleBody'}).text
        
        # KBS
        elif 'news.kbs' in url:
            text = soup.find('div', {'id' : 'cont_newstext'}).text
            
        # JTBC
        elif 'news.jtbc' in url:
            text = soup.find('div', {'class' : 'article_content'}).text
            
        # 그 외
        else:
            text = ""
            
        return text.replace('\n','').replace('\r','').replace('<br>','').replace('\t','')       

    def crawler(self, maxpage,query,sort,s_date,e_date):
        url_query = quote(query)
        s_from = s_date.replace(".","")
        e_to = e_date.replace(".","")
        page = 1  
        maxpage_t =(int(maxpage)-1)*10+1   # 11= 2페이지 21=3페이지 31=4페이지  ...81=9페이지 , 91=10페이지, 101=11페이지
        
        while page <= maxpage_t:
            print("page", page, maxpage_t)
            url = "https://search.naver.com/search.naver?where=news&query=" + url_query + "&sort="+sort+"&ds=" + s_date + "&de=" + e_date + "&nso=so%3Ar%2Cp%3Afrom" + s_from + "to" + e_to + "%2Ca%3A&start=" + str(page)
            print("search url", url)
            # response = requests.get(url)
            # html = response.text
            search_url = urllib.request.urlopen(url).read()

            #뷰티풀소프의 인자값 지정
            soup = BeautifulSoup(search_url, 'html.parser')

            table = soup.find('ul',{'class' : 'list_news'})
            li_list = table.find_all('li', {'id': re.compile('sp_nws.*')})
            area_list = [li.find('div', {'class' : 'news_area'}) for li in li_list]
            # title_list = [area.find('a', {'class' : 'news_tit'}) for area in area_list]
            # contents_list = [area.find('div', {'class' : 'news_dsc'}) for area in area_list]
            # press_list = [area.find('a', {'class' : 'info press'}) for area in area_list]
            # info_list = [area.find('a', {'class' : 'info'}) for area in area_list]

            for area in area_list:
                self.title_text.append(area.find('a', {'class' : 'news_tit'}).get('title'))          #제목
                self.link_text.append(area.find('a', {'class' : 'news_tit'}).get('href'))            #링크주소
                self.article_text.append(self.get_article(area.find('a', {'class' : 'news_tit'}).get('href'))) #신문 본문
                self.press_text.append(area.find('a', {'class' : 'info press'}).get_text())          #언론사
                self.info_text.append(area.find('a', {'class' : 'info'}).get_text())                 #네이버신문 
                self.contents_text.append(area.find('div', {'class' : 'news_dsc'}).get_text())       #네이버신문 요약 

            # for title in title_list:
            #     self.title_text.append(title.get('title'))     #제목
            #     self.link_text.append(title.get('href'))       #링크주소
            #     self.article_text.append("")
            #     # self.article_text.append(self.get_article(title.get('href')))


            # for press in press_list:
            #     self.press_text.append(press.get_text())       #언론사

            # for info in info_list:
            #     self.info_text.append(info.get_text())         #네이버신문

            # for contents in contents_list:
            #     self.contents_text.append(contents.get_text())     #네이버신문 요약  

        #         if(new_url == '#'):
        #             continue
        #         else:
        #             news_link = urllib.request.urlopen(new_url).read()
        #             news_html = BeautifulSoup(new_link, 'html.parser')
        #             news_title = news_html.find('h3', {'id':'articleTitle'}).get_text()
        #             news_datetime = news_html.find('span', {'class':'t11'}).get_text()
        #             news_article = news_html.find('div', {'id':'articleBodyContents'}).get_text()

        #             news_df.loc[idx] = [news_title, news_url, press, news_datetime, news_article]
        #             idx += 1
        #             print("#", end="")

        #     #<a>태그에서 제목과 링크주소 추출
        #     atags = soup.select('._sp_each_title')
        #     print("atags text", atags)
        #     for atag in atags:
        #         self.title_text.append(atag.text)     #제목
        #         self.link_text.append(atag['href'])   #링크주소
                
        #     print("title text", self.title_text)
        #     #신문사 추출
        #     source_lists = soup.select('._sp_each_source')
        #     for source_list in source_lists:
        #         source_text.append(source_list.text)    #신문사
            
        #     #날짜 추출 
        #     date_lists = soup.select('.txt_inline')
        #     for date_list in date_lists:
        #         test=date_list.text   
        #         date_cleansing(test)  #날짜 정제 함수사용 
            
        #     #본문요약본
        #     contents_lists = soup.select('ul.type01 dl')
        #     for contents_list in contents_lists:
        #         #print('==='*40)
        #         #print(contents_list)
        #         contents_cleansing(contents_list) #본문요약 정제화
            
            
            # 모든 리스트 딕셔너리형태로 저장
            self.result= {"title":self.title_text ,  "link":self.link_text, "press" : self.press_text, "info" : self.info_text, "contents" : self.contents_text, "article" : self.article_text }  
            # self.result= {"date" : self.date_text , "title":self.title_text ,  "source" : self.source_text ,"contents": self.contents_text ,"link":self.link_text }  
        #     # result= {"date" : date_text , "title":title_text ,  "source" : source_text ,"contents": contents_text ,"link":link_text }  
        #     print(page)
            
            df = pd.DataFrame(self.result)  #df로 변환
            page += 10
        
        
        # 새로 만들 파일이름 지정
        outputFileName = 'Naver News %s-%s-%s  %s시 %s분 %s초 merging.xlsx' % (self.now.year, self.now.month, self.now.day, self.now.hour, self.now.minute, self.now.second)
        df.to_excel(self.RESULT_PATH+outputFileName,sheet_name='sheet1')        


if __name__ == "__main__":
    crawler = NaverNewsCrawler()
    crawler.crawler(1, "인공지능","1","2021.01.04","2021.04.30")