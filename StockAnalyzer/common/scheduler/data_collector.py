from datetime import datetime, timedelta
from re import S
import re
import time
from common.agent.ebest import EBest
from common.agent.logger import Logger
from common.db_handler.mongodb_handler import MongoDBHandler
# import matplotlib.pyplot as plt
import pandas as pd


class DataCollector(object):
    def __init__(self):
        self.starttime = time.time()  # 시작 시간 저장  print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
        self.mongodb = MongoDBHandler()
        self.db_name = "stock1"
        self.log = Logger()
        self.ebest = EBest("DEMO")
        self.ebest.login() 
        self.ebest.change_field_lang('E')
        self.ins_date = datetime.today().strftime("%Y%m%d")
        self.ins_time = datetime.today().strftime("%H%M%S")
        
    def __del__(self):
        # self.ebest.logout()
        print("classs__del__")

    def clear_data_base(self):
        self.mongodb.delete_items({}, self.db_name, "stock_code")
        self.mongodb.delete_items({}, self.db_name, "code_info")
        self.mongodb.delete_items({}, self.db_name, "daily_price")
        self.mongodb.delete_items({}, self.db_name, "Naver_news")

    def calc_avg_volume(self, shcode, start_date, end_date):
        tot_volume = 0

        cond = {'shcode': shcode, 'date': {'$gte': start_date, '$lte':end_date}}
        price_list = list(self.mongodb.find_items(cond, self.db_name, "daily_price")) 

        if len(price_list):
            for i, price in enumerate(price_list):
                if int(price['jdiff_vol']) :
                    print(price['jdiff_vol'], price['date'])
                    tot_volume = tot_volume + int(price['jdiff_vol'])

            avg_volume = tot_volume / i
        
        print(i, avg_volume, tot_volume)
        return avg_volume 


    def collect_daily_stock_data_from_ebest(self, init=False):

        print( self.calc_avg_volume('035420', '20210501', '20210520'))

        # return

        if init:
            self.get_stock_code_from_ebest()

        code_list = list(self.mongodb.find_items({"bu12gubun" : "01"}, self.db_name, "stock_code"))

        # 1. update code_info 
        #    - 주식종목 기본 + 추가 정보(전일거래량, 시가, 종가 등)
        #    - 거래량이 만주 이하인 종목 제외 
        result_ext_all = []
        i = 0
        if len(code_list) > 0 : 
            self.log.info("t8407 주식현재가 시작")
            str_codes = ""

            for code in code_list : 
                str_codes = str_codes + code["shcode"]
                i = i + 1
                if len(str_codes) >= 300 or len(code_list) == i:
                    result_ext = self.ebest.get_current_price_by_shcodes(str_codes)
                    result_ext_all.extend(result_ext)  
                    self.log.info("result_ext_all 건수[%d]" % len(result_ext_all))                              
                    str_codes = ""

            # 현재일자 검색과 daily_price 업데이트 최종업데이트 일자
            fromday = (datetime.today() - timedelta(days=30)).strftime("%Y%m%d")
            today = datetime.today().strftime("%Y%m%d")
            result_sc = self.ebest.get_stock_chart_by_code('035420', "2", fromday, today)  
            
            tradingday = ''                 # 최종거래일
            befoneday = self.ins_date       # 한달 전일
            daily_prices = []
            for sc in result_sc:
                if sc['date'] > tradingday:
                    tradingday = sc['date']
                if sc['date'] < befoneday:
                    befoneday = sc['date']                    

            self.log.info("트레이딩 일자 : {} 건".format(tradingday))

            cond = {'shcode': '035420', 'date': tradingday}
            results = list(self.mongodb.find_items(cond, self.db_name, "daily_price")) 
            if len(results) > 0 :
                flag_daily_price = False
            else:
                flag_daily_price = True
            
            # 코드정보와 주식현재가 추가, 일별 주가정보(daily_price) 업데이트
            for code in code_list :
                for extend in result_ext_all :
                    if code["shcode"] == extend["shcode"]:
                        code.update(extend)
                        # 한달평균 거래량 계산
                        ret_avg_volume = self.calc_avg_volume(code["shcode"], befoneday, tradingday)
                        if flag_daily_price:
                            daily_price = {'date':tradingday, 'time':"", 'open':extend['open'], 'high':extend['high'], 'low':extend['low'], 
                                            "close":extend['bidho'], "jdiff_vol":extend['volume'], "value":extend['value'], 
                                            "shcode": extend['shcode'], "hname":extend['hname'], "insdate":self.ins_date, "instime":self.ins_time}
                            daily_prices.append(daily_price)

            if flag_daily_price:
                self.mongodb.delete_items({'date':tradingday}, self.db_name, "daily_price")
                self.mongodb.insert_items(daily_prices, self.db_name, "daily_price")

            self.log.info("종목코드 + 주식 현재가 반영 : {} 건".format(len(code_list)))
            
            self.mongodb.delete_items({}, self.db_name, "code_info")
            self.mongodb.insert_items(code_list, self.db_name, "code_info")

            self.insert_exec_job_list('EB', '종목코드 + 현재가 반영 작업', 'code_info', str(len(code_list))+" 건이 정상처리 되었습니다.")

        pass
    def get_stock_code_from_ebest(self):
        """
        :주식 종목 코드 가져오기
        """
        # stocks  = self.mongodb.find_items({}, self.db_name, "stock_code")
        results = list(self.ebest.get_code_list("ALL"))

        # !!@@나중에 업그레이드 하자, db에 없는 종목만 업데이트
        # for i, result in results:
        #     for stock in stocks:
        #         if stock['shcode'] == result['shcode']:
        #             del
        self.mongodb.delete_items({}, self.db_name, "stock_code")         
        self.mongodb.insert_items(results, self.db_name, "stock_code")

        self.insert_exec_job_list('EB', '종목코드 가져오기', 'stock_code', str(len(results))+" 건이 정상처리 되었습니다.")

    def get_code_info_from_ebest(self):
        """
        주식 종목에 현재 가격, 거래량 정보 추가 
        """
        result_cod = self.ebest.get_code_list("ALL")
        self.log.info("get_code_list%d" % len(result_cod))
        result_ext_all = []
        i = 0
        if len(result_cod) > 0 : 
            self.log.info("t8407 주식현재가 시작")
            str_codes = ""
            for code in result_cod : 
                str_codes = str_codes + code["shcode"]
                i = i + 1
                if len(str_codes) >= 300 or len(result_cod) == i:
                    result_ext = self.ebest.get_current_price_by_shcodes(str_codes)
                    result_ext_all.extend(result_ext)  
                    self.log.info("result_ext_all 건수[%d]" % len(result_ext_all))                              
                    str_codes = ""
            # 코드정보와 주식현재가 병합
            for code in result_cod :
                for extend in result_ext_all :
                    if code["shcode"] == extend["shcode"] :
                        code.update(extend)

            self.log.info("종목코드 + 주식 현재가 반영 : {} 건".format(len(result_cod)))
            
            self.mongodb.delete_items({}, self.db_name, "code_info")
            self.mongodb.insert_items(result_cod, self.db_name, "code_info")

            self.insert_exec_job_list('EB', '종목코드 + 현재가 반영 작업', 'code_info', str(len(result_cod))+" 건이 정상처리 되었습니다.")

    def search_increase_vol_by_code(self):
        """ 거래량 증가 종목 검색
        : 한달 평균 거래량 보다 5배 증가한 종목 수집(check_volume)
        """    
        # 증권그룹 '01' 
        code_list = list(self.mongodb.find_items({"bu12gubun" : "01"}, self.db_name, "code_info"))
        self.log.info("CODE_LIST[%d]" % len(code_list))


        today = datetime.today().strftime("%Y%m%d")
        fromday = (datetime.today() - timedelta(days=30)).strftime("%Y%m%d")
        
        inc_codes = []
        vol_codes = []
        loop_cnt = 0
        for code in code_list :
            if int(code["volume"]) < 10000: 
                continue
            loop_cnt = loop_cnt + 1
            if loop_cnt % 100 == 0 and len(inc_codes) > 0 :
                print(str(loop_cnt % 100) + "//" + str(len(inc_codes)))
                print(code["shcode"]+ "진행율 : " + str((loop_cnt / len(code_list) * 100)))
                self.mongodb.insert_items(inc_codes, self.db_name, "check_volume")  
                inc_codes.clear()

            results = self.ebest.get_stock_chart_by_code(code["shcode"], "2", fromday, today)
            time.sleep(1)
            if len(results) > 0:
                # 평균 거래량 계산
                tot_volume = 0
                i_count = 0
                for result in results:
                    if  int(result['jdiff_vol']) != 0 :
                        tot_volume = tot_volume + int(result['jdiff_vol'])
                        i_count= i_count + 1

                if  i_count == 0 or tot_volume == 0:
                    continue

                avg_volume = tot_volume / i_count
                inc_rate = int(result['jdiff_vol']) / avg_volume
                inc_code = {"shcode": code["shcode"], "hname":code["hname"], "sdate": today, "avg_volume": int(avg_volume),  "volume":int(result['jdiff_vol']), "inc_rate" : inc_rate}
                vol_codes.append(inc_code)
                print("체크 종목 :" + code["shcode"] + "  거래량 [" + result['jdiff_vol'] + "] 평균 [" + str(avg_volume) + "]  비율[" + str(inc_rate) + "]")
                # 거래량이 5배 이상이면 종목 추가
                if inc_rate > 5 :
                    inc_codes.append(inc_code)
                    print("추가된 종목 :" + code["shcode"] + " 건수 : " + str(len(inc_codes))) 

        if len(inc_codes) > 0 :
            self.mongodb.insert_items(inc_codes, self.db_name, "check_volume")  
            
        if len(vol_codes) > 0 :
            self.mongodb.insert_items(vol_codes, self.db_name, "volume")  


        self.insert_exec_job_list('TA', '거래량 급증가 종목 검색', 'check_volume', str(len(vol_codes))+" 건이 정상처리 되었습니다.")
        
    def insert_exec_job_list(self, jobtype, jobname, tablelist, logmsg):
        """ 작업로그 기록
        : jobtype -> 'EB' : EBEST, 'FA': 재무분석작업, 'TA' : 기술적 분석 작업, 
        :            'ML' : 머신러닝 예측작업, 'MA' : 재료분석 작업
        """
        jobs = []
        today = datetime.today().strftime("%Y%m%d")
        totime = datetime.today().strftime("%H%M%S")
        job = {"jobtype": jobtype, "jobdate": today, "jobtime": totime, "jobname": jobname, "tablelist": tablelist,  "logmsg": logmsg}
        jobs.append(job)
        self.mongodb.insert_items(jobs, self.db_name, "job_list")  
        print(jobs)

    @staticmethod
    def ins_daily_price_from_ebest(self, start_date, code_list):
        daily_price = []
        today = datetime.today().strftime("%Y%m%d")
        totime = datetime.today().strftime("%H%M%S")

        for code in code_list:
            results = self.ebest.get_stock_chart_by_code(code['shcode'], "2", start_date, today)  

            for result in results:
                result['shcode'] = code['shcode']
                result['hname']  = code['hname']
                result['insdate'] = today
                result['instime'] = totime

                daily_price.append(result)

            if len(daily_price) > 0:
                self.mongodb.insert_items(daily_price, self.db_name, "daily_price") 
                daily_price.clear()

        self.insert_exec_job_list('EB', '일일 주식거래정보(Candle Chart형)', 'daily_price', str(len(code_list))+" 건이 정상처리 되었습니다.")


    def insert_daily_price_from_ebest(self, init=None, start_date=None, shcode=None):
        ####
        # 일일 주식 거래정보(candle chart형) 
        # init : 1 -> 모든 종목 초기화 후 재생성, 2 -> 입력받은 코드만 조건대로 시작일 부터 갱신
        ####  
        today = datetime.today().strftime("%Y%m%d")
        totime = datetime.today().strftime("%H%M%S")

        if init == 1:
            #데이터 삭제    
            self.mongodb.delete_items({}, self.db_name, "daily_price") 

            code_list = list(self.mongodb.find_items({"bu12gubun" : "01"}, self.db_name, "stock_code"))
            if len(code_list):
                self.ins_daily_price_from_ebest(self, start_date, code_list)
            else:
                print("No Data!!")     

        elif init == 2:
            if shcode:
                cond = {'shcode': shcode, 'date': {'$gte': start_date, '$lte':today}}
            else: 
                cond = {'date': {'$gte': start_date, '$lte':today}}
            self.mongodb.delete_items(cond, self.db_name, "daily_price") 

            if shcode:
                cond = {'shcode': shcode}
            else: 
                cond = {"bu12gubun" : "01"}
            print("조건", cond)
            code_list = list(self.mongodb.find_items(cond, self.db_name, "stock_code"))

            if len(code_list):
                self.ins_daily_price_from_ebest(self, start_date, code_list) 
            else:
                print("No Data !!")


        else:
            print("init error No Data !!")
 
    def search_bollingerBand_by_code(self, search_date, order_type):
        """볼린저 밴드 매수, 매도 시그널이 있는 종목 검색
        : search_date : 검색 시작일자, order_type : 1 -> 매수, 2 -> 매도, 0 -> ALL
        """
        today = datetime.today().strftime("%Y%m%d")
        totime = datetime.today().strftime("%H%M%S")
        fromday = (datetime.today() - timedelta(days=90)).strftime("%Y%m%d")
        
        # 기존 작업 삭제
        cond = {'insdate': today}
        self.mongodb.delete_items(cond, self.db_name, "a1_bollband") 

        # 증권그룹 '01' 종목리스트 조회
        code_list = list(self.mongodb.find_items({"bu12gubun" : "01"}, self.db_name, "stock_code"))
        print("CODE_LIST", len(code_list))



        c = 0  # count
        ins_list = []
        for code in code_list:
            cond = {'shcode': code['shcode'], 'date': {'$gte': fromday, '$lte':today}}
            results = list(self.mongodb.find_items(cond, self.db_name, "daily_price"))

            if not len(results) : continue
            # JSON => dataframe으로 변환
            df = pd.DataFrame.from_dict(results, orient='columns')


            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['volume'] = df['jdiff_vol'].astype(float)

            df['MA20'] = df['close'].rolling(window=20).mean() 
            df['stddev'] = df['close'].rolling(window=20).std() 
            df['upper'] = df['MA20'] + (df['stddev'] * 2)
            df['lower'] = df['MA20'] - (df['stddev'] * 2)
            df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])
            # 고가, 저가, 종가의 합을 3으로 나눠서 중심가격 구함.
            df['TP'] = (df['high'] + df['low'] + df['close']) / 3
            df['PMF'] = 0
            df['NMF'] = 0
            # range함수는 마지막 값을 포함하지 않으므로 0부터 종가개수 -2까지 반복
            for i in range(len(df.close)-1):
                if df.TP.values[i] < df.TP.values[i+1]:
                    # 긍정적 현금흐름 : 중심가격이 전날보다 상승한 날들의 현금흐름의 합
                    df.PMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
                    df.NMF.values[i+1] = 0
                else:
                    # 긍정적 현금흐름 : 중심가격이 전날보다 하락한 날들의 현금흐름의 합
                    df.NMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
                    df.PMF.values[i+1] = 0
            df['MFR'] = (df.PMF.rolling(window=10).sum() /
                df.NMF.rolling(window=10).sum())
            df['MFI10'] = 100 - 100 / (1 + df['MFR'])
            df = df[19:]

            
            # 상승 하락유무 체크
            df_up = df.loc[(df.date.values > search_date) & (df.PB.values > 0.8) & (df.MFI10.values > 80), ["date", "PB", "MFI10"]]
            df_dw = df.loc[(df.date.values > search_date) & (df.PB.values < 0.2) & (df.MFI10.values < 20), ["date", "PB", "MFI10"]]

            if not df_up.empty or not df_dw.empty:
                signal = 'UP'
                if not df_dw.empty:
                    signal = 'DW'
                # 주간, 월간, 3개월 상승율 계산
                ret_1w = round((df.close.values[len(df.close)-1] / df.close.values[len(df.close)-1-5]  - 1) * 100, 2)   # 1주간 수익율
                ret_4w = round((df.close.values[len(df.close)-1] / df.close.values[len(df.close)-1-20]  - 1) * 100, 2)  # 20일 한달 수익율
                ret_8w = round((df.close.values[len(df.close)-1] / df.close.values[len(df.close)-1-40]  - 1) * 100, 2)  # 40일 두달 수익율
                ret_12w = round((df.close.values[len(df.close)-1] / df.close.values[0]  - 1) * 100, 2)                  # 60일 두달 수익율

                ins_code = {"shcode": code["shcode"], "hname":code["hname"], "signal": signal, "search_date": search_date, "insdate": today, "instime" : totime,
                            "ret_1w": ret_1w, "ret_4w": ret_1w, "ret_8w": ret_8w, "ret_12w": ret_12w }
                ins_list.append(ins_code)

              


            # for i in range(len(df.close)):
            #     if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:       # ①
            #         print("매수신호")
            #     elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:     # ③
            #         print("매도신호")

            c += 1
            # print('\ncount :{} \nins_list : {}'.format(c, ins_list))
            # if ins_list:
            #     return
            if (c % 100) == 0:    # 100건 처리하고 등록
                if ins_list :
                    self.mongodb.insert_items(ins_list, self.db_name, "a1_bollband")  
                    print(ins_list)
                    ins_list.clear()

                print('\n전체 {} 건 / {} 건 처리 중....'.format(len(code_list), c))

        self.insert_exec_job_list('TA', '볼린저 밴드 매수 시그날 종목 검색', 'a1_bollband', str(len(code_list))+" 건이 정상처리 되었습니다.")

    def search_momentum(self):
        """ 모멘텀 종목 검색
        : 
        """        
        today = datetime.today().strftime("%Y%m%d")
        totime = datetime.today().strftime("%H%M%S")
        fromday = (datetime.today() - timedelta(days=90)).strftime("%Y%m%d")
        
        # 기존 작업 삭제
        cond = {'insdate': today}
        self.mongodb.delete_items(cond, self.db_name, "a2_momentum") 

        # 네이버 조회를 통해 모멘텀 계산할 일자 조회
        price_list = list(self.mongodb.find_items({"shcode" : "035420"}, self.db_name, "daily_price")) 

        if not price_list:
            print("Not found price Data !!"); return

        df = pd.DataFrame.from_dict(price_list, orient='columns')
        df_dates = df.iloc[[len(df.close)-1, len(df.close)-21, len(df.close)-61, len(df.close)-121, len(df.close)-241], ]
        
        if len(df_dates) != 5:
            print("Not correct dates !!"); return

        # 증권그룹 '01' 종목리스트 조회
        code_list = list(self.mongodb.find_items({"bu12gubun" : "01"}, self.db_name, "stock_code"))
        print("CODE_LIST", len(code_list))

        c = 0  # count
        ins_list = []
        # 종목별 상승율 계산
        for code in code_list:
            cond = {"shcode" : code['shcode'], 'date': {'$in': df_dates.date.values.tolist()}}
            price_list = list(self.mongodb.find_items(cond, self.db_name, "daily_price")) 

            df = pd.DataFrame.from_dict(price_list, orient='columns')
            df.date.sort_values()
            df['close'] = df['close'].astype(float)
            ret_1mon = round(( df.close.values[4] / df.close.values[3] - 1 ) * 100, 2)
            ret_3mon = round(( df.close.values[4] / df.close.values[2] - 1 ) * 100, 2)
            ret_6mon = round(( df.close.values[4] / df.close.values[1] - 1 ) * 100, 2)
            ret_12mon = round(( df.close.values[4] / df.close.values[0] - 1 ) * 100, 2)

            if ret_1mon > 0 and ret_3mon > 0:
                ins_code = {"shcode": code["shcode"], "hname":code["hname"], "insdate": today, "instime" : totime,
                            "ret_1mon": ret_1mon, "ret_3mon": ret_3mon, "ret_6mon": ret_6mon, "ret_12mon": ret_12mon }
                ins_list.append(ins_code)

            c += 1
            # print('\ncount :{} \nins_list : {}'.format(c, ins_list))

            if (c % 100) == 0:    # 100건 처리하고 등록
                if ins_list :
                    self.mongodb.insert_items(ins_list, self.db_name, "a2_momentum")  
                    print(ins_list)
                    ins_list.clear()

                print('\n전체 {} 건 / {} 건 처리 중....'.format(len(code_list), c))

        self.insert_exec_job_list('TA', '모멘텀 상승 종목 검색', 'a2_momentum', str(len(code_list))+" 건이 정상처리 되었습니다.")

    def collect_stock_info(self):
        code_list = self.mongodb.find_items({}, self.db_name, "code_info")
        target_code = set([item["단축코드"] for item in code_list])
        today = datetime.today().strftime("%Y%m%d")
        
        collect_list = self.mongodb.find_items({"날짜":today}, self.db_name, "price_info").distinct("code")
        for col in collect_list:
            target_code.remove(col)

        for code in target_code:
            result_price = self.ebest.get_stock_price_by_code(code, "1")
            time.sleep(1)
            if len(result_price) > 0:
                self.mongodb.insert_items(result_price, self.db_name, "price_info")

if __name__ == '__main__':
    dc = DataCollector()
    # dc.get_stock_code_from_ebest()
    # dc.get_code_info_from_ebest()
    # dc.search_increase_vol_by_code()
    # dc.insert_daily_price_from_ebest(1, '20190101', '')   # 초기 실행
    # dc.insert_daily_price_from_ebest(2, '20210101', '000020')
    # dc.insert_daily_price_from_ebest(2, '20210512', '')   # 매일실행(속도 문제)
    dc.collect_daily_stock_data_from_ebest(False)
    # dc.search_bollingerBand_by_code('20210517', 1)   # 날짜는 3일 전으로 셋업
    # dc.search_momentum()
    # collect_stock_info()
    # 매일 데이터 축적
    # 1. 일일 거래 데이터 업데이트
    # 2. 거래량 증가 종목 찾기
    # 3. 볼린저 밴드 생성
    # 4. 모멘텀 데이터 생성
    # 5. 일일 뉴스 집계
    # 6. 핵심 단어 추출

    print("program main end")