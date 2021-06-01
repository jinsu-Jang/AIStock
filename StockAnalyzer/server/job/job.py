import os
import sys
sys.path.append( os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from datetime import datetime, timedelta
import time
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource, fields, marshal_with
from common.agent.ebest import EBest
from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()
# ebest = EBest("DEMO")
# ebest.login()

class Job(Resource):
    def get(self, id=None):
        if id:
            print("get method id :"+str(id))
            # 종목코드 가져오기
            if id == 1 :
                ebest = EBest("DEMO")
                ebest.login()
                result_cod = ebest.get_code_list("ALL")
                print("get_code_list", len(result_cod))
                result_ext_all = []
                i = 0
                if len(result_cod) > 0 : 
                    print("t8407 주식현재가 시작")
                    str_codes = ""
                    for code in result_cod : 
                        str_codes = str_codes + code["단축코드"]
                        i = i + 1
                        if len(str_codes) >= 300 or len(result_cod) == i:
                            result_ext = ebest.get_current_price_by_shcodes(str_codes)
                            result_ext_all.extend(result_ext)  
                            print("result_ext_all 건수", len(result_ext_all))                              
                            str_codes = ""
                    # 코드정보와 주식현재가 병합
                    for code in result_cod :
                        for extend in result_ext_all :
                            if code["단축코드"] == extend["종목코드"] :
                                code.update(extend)

                    print("result_cod 건수", len(result_cod)) 
                    
                    mongodb = MongoDBHandler()
                    mongodb.delete_items({}, "stock", "m_code_info")
                    mongodb.insert_items(result_cod, "stock", "m_code_info")
                # collect_code_list()
                # collect_stock_info()
                return {"errcode" : 0, "errmsg": str(len(result_cod))+" 건이 정상처리 되었습니다."}
            # 종목가격정보 가져오기
            elif id == 2 :     
                ebest = EBest("DEMO")
                ebest.login()                
                code_list = mongodb.find_items({}, "stock", "m_code_info")
                target_code = set([item["단축코드"] for item in code_list])
                today = datetime.today().strftime("%Y%m%d")
                collect_list = mongodb.find_items({"날짜":today}, "stock", "price_info").distinct("code")
                for col in collect_list:
                    target_code.remove(col)

                for code in target_code:
                    result_price = ebest.get_stock_price_by_code(code, "1")
                    time.sleep(1)
                    if len(result_price) > 0:
                        mongodb.insert_items(result_price, "stock", "price_info")   

                return {"errcode" : 0, "errmsg": str(len(result))+" 건이 정상처리 되었습니다."}

            # 모든 종목정보 가져오기
            elif id == 3 :      
                ebest = EBest("DEMO")
                mongodb = MongoDBHandler()
                ebest.login()
                code_list = mongodb.find_items({}, "stock", "m_code_info")
                target_code = set([item["단축코드"] for item in code_list])
                today = datetime.today().strftime("%Y%m%d")
                print(today)
                collect_list = mongodb.find_items({"날짜":today}, "stock", "price_info").distinct("code")
                for col in collect_list:
                    target_code.remove(col)
                for code in target_code:
                    time.sleep(1)
                    print("code:", code)
                    result_price = ebest.get_stock_price_by_code(code, "1")
                    if len(result_price) > 0:
                        print(result_price)
                        mongodb.insert_items(result_price, "stock", "price_info")

                    result_credit = ebest.get_credit_trend_by_code(code, today)
                    if len(result_credit) > 0:
                        mongodb.insert_items(result_credit, "stock", "credit_info")

                    result_short = ebest.get_short_trend_by_code(code, 
                                                                sdate=today, edate=today)
                    if len(result_short) > 0:
                        mongodb.insert_items(result_short, "stock", "short_info")

                    result_agent = ebest.get_agent_trend_by_code(code, 
                                                                fromdt=today, todt=today)
                    if len(result_agent) > 0:
                        mongodb.insert_items(result_agent, "stock", "agent_info")  

            # 일자별 주가정보에서 평균 거래량 가져오기(거래량 증가 종목 찾기)
            elif id == 4 : 
                ebest = EBest("DEMO")
                ebest.login()    
                mongodb = MongoDBHandler()               
                code_list = list(mongodb.find_items({}, "stock", "m_code_info"))
                print(len(code_list))

                target_code = set([item["단축코드"] for item in code_list])
                # print(target_code)
                today = datetime.today().strftime("%Y%m%d")
                fromday = (datetime.today() - timedelta(days=30)).strftime("%Y%m%d")
                
                inc_codes = []
                vol_codes = []
                loop_cnt = 0
                for code in code_list :
                    if int(code["누적거래량"]) < 10000: 
                        continue
                    loop_cnt = loop_cnt + 1
                    if loop_cnt % 100 == 0 and len(inc_codes) > 0 :
                        print(str(loop_cnt % 100) + "//" + str(len(inc_codes)))
                        print(code["단축코드"]+ "진행율 : " + str((loop_cnt / len(code_list) * 100)))
                        mongodb.insert_items(inc_codes, "stock", "check_volume")  
                        inc_codes.clear()

                    results = ebest.get_stock_chart_by_code(code["단축코드"], "2", fromday, today)
                    time.sleep(1)
                    if len(results) > 0:
                        # 평균 거래량 계산
                        tot_volume = 0
                        i_count = 0
                        for result in results:
                            if  int(result['거래량']) != 0 :
                                tot_volume = tot_volume + int(result['거래량'])
                                i_count= i_count + 1

                        if  i_count == 0 or tot_volume == 0:
                            continue

                        avg_volume = tot_volume / i_count
                        inc_rate = int(result['거래량']) / avg_volume
                        inc_code = {"code": code["단축코드"], "종목명": code["종목명"], "sdate": today, "avg_volume": int(avg_volume),  "volume":int(result['거래량'])}
                        vol_codes.append(inc_code)
                        print("체크 종목 :" + code["종목명"] + "  거래량 [" + result['거래량'] + "] 평균 [" + str(avg_volume) + "]  비율[" + str(inc_rate) + "]")
                        # 거래량이 5배 이상이면 종목 추가
                        if inc_rate > 5 :
                            inc_codes.append(inc_code)
                            print("추가된 종목 :" + code["종목명"] + " 건수 : " + str(len(inc_codes))) 

                if len(inc_codes) > 0 :
                    mongodb.insert_items(inc_codes, "stock", "check_volume")  
                    
                if len(vol_codes) > 0 :
                    mongodb.insert_items(vol_codes, "stock", "volume")  

                return {"errcode" : 0, "errmsg": str(len(vol_codes))+" 건이 정상처리 되었습니다."}                
            elif id == 5 : 
                print("Error Id : " + str(id))
            else:
                print("Error Id : " + str(id))
        else:
            print("list of users")
            # collect_code_list()
            # collect_stock_info()

            return "list of users"

    def post(self):
        try:
            print("post")
            collect_code_list()
            collect_stock_info() 
            return { "errcode": 0, "errmsg": "정상처리" }, 200 
            
        except Exception as e:
            return {'error': str(e)}            

         

