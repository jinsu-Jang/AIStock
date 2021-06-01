from datetime import datetime, timedelta
import time
from common.agent.logger import Logger

class WorkDate(object):
    def __init__(self):
         self.log = Logger()

    def find_working_day(self, workdate, offset = 0):
        yyyy = workdate[0:4]
        mm = workdate[4:6]
        dd = workdate[6:8]
                
        pre_date = datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, 0) - timedelta(offset)

        while True :
            yyyy = str(pre_date)[0:4]
            mm = str(pre_date)[5:7]
            dd = str(pre_date)[8:10]    
            
            koreanholidays = getattr(KoreanHolidays, 'y'+ yyyy, None)
            if koreanholidays is None:
                self.log.error("KoreanHolidays 클래스 %s년도 휴일 정의"% yyyy)
                return None
            if ( pre_date.weekday() >= 5 ) or (yyyy+mm+dd in koreanholidays) :   
                pre_date = pre_date - timedelta(max(1, (pre_date.weekday() + 6) % 7 - 3 ))

            else : break

        return yyyy+mm+dd

    def get_trading_day_list(self, workdate, offsets):
        if workdate is None:
            workdate = datetime.today().strftime("%Y%m%d")
        trading_days = []
        for offset in offsets:
            trading_day = self.find_working_day(workdate, offset)

            trading_days.append(trading_day)

        return trading_days

    def get_trading_day_list1(self, workdate, days):
        if workdate is None:
            workdate = datetime.today().strftime("%Y%m%d")
        trading_days = []

        for i in range(days):
            trading_day = self.find_working_day(workdate, i)

            if trading_day not in trading_days:
                trading_days.append(trading_day)


        return trading_days        

class KoreanHolidays :
    # 휴장일 정보 https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp
    y2021 = {
        '20210101', #신정
        '20210211', #설날
        '20210212', #설날
        '20210301', #삼일절
        '20210505', #어린이날
        '20210519', #석가탄신일
        '20210920', #추석
        '20210921', #추석
        '20210922', #추석
        '20211231', #연말휴장일
    }
    y2020 = {
        '20200101', #신정
        '20200124', #설날
        '20200127', #설날(대체휴일)
        '20200415', #21대 국회의원선거
        '20200430', #석가탄신일
        '20200501', #근로자의날
        '20200505', #어린이날
        '20200817', #임시공휴일
        '20200930', #추석
        '20201001', #추석
        '20201002', #추석
        '20201009', #한글날
        '20201225', #성탄절
        '20201231', #연말휴장일
    }
    y2019 = {
        '20190101',	#신정
        '20190204',	#설날
        '20190205',	#설날
        '20190206',	#설날
        '20190301',	#삼일절
        '20190501',	#근로자의날
        '20190506',	#어린이날(대체휴일)
        '20190606',	#현충일
        '20190815',	#광복절
        '20190912',	#추석
        '20190913',	#추석
        '20191003',	#개천절
        '20191009',	#한글날
        '20191225',	#성탄절
        '20191231',	#연말휴장일
    }
    y2018 = {
        '20180101', #신정
        '20180215', #설날
        '20180216', #설날
        '20180301', #삼일절
        '20180501', #근로자의날
        '20180507', #어린이날(대체휴일)
        '20180522', #석가탄신일
        '20180606', #현충일
        '20180613', #지방선거
        '20180815', #광복절
        '20180924', #추석
        '20180925', #추석
        '20180926', #추석(대체휴일)
        '20181003', #개천절
        '20181009', #한글날
        '20181225', #성탄절
        '20181231', #연말휴장일
    }
    y2017 = {
        '20170127', #설날
        '20170130', #설날(대체휴일)
        '20170301', #삼일절
        '20170501', #근로자의날
        '20170503', #석가탄신일
        '20170505', #어린이날
        '20170509', #대통령 선거일
        '20170606', #현충일
        '20170815', #광복절
        '20171002', #임시 공휴일
        '20171003', #개천절
        '20171004', #추석
        '20171005', #추석
        '20171006', #추석(대체휴일)
        '20171009', #한글날
        '20171225', #성탄절
        '20171229', #연말휴장일
    }  
  

if __name__ == '__main__':

    wd = WorkDate()
    print(wd.find_working_day('20210523', 0))
    print(wd.get_trading_day_list('20210527', [1, 22, 23, 24, 60, 400, 700, 800, 2000]))
#     print(wd.get_trading_day_list1('20210527', 30))

