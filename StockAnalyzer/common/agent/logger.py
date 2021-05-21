import logging
import configparser
import logging.config
from datetime import datetime
import os
import json
##### 시스템 로그 처리
# 개발 : DEBUG,   파일로그 : DEBUG 이상 , CONSOLE : WARNING 이상 로깅 
# 운영 : WARNING, 파일로그 : INFO 이상 , CONSOLE : WARNING 이상 로깅 
#####
class Logger(object):
    def __init__(self):
        # config = json.load(open('conf/logger.json'))
        # logging.config.dictConfig(config)

        config = configparser.RawConfigParser()
        config.read('conf/config.ini')
        self.mode = config["SYSTEM"]["mode"]
        self.logpath = config["PATH"]["SA_logpath"]
        if self.mode is None:
            raise Exception("Need to system mode key")

        self.logger = logging.getLogger()
        #1 logger instance를 만든다.
        self.logger = logging.getLogger()

        #2 logger의 level을 가장 낮은 수준인 DEBUG로 설정해둔다.
        if self.mode == 'dev':
            self.logger.setLevel(logging.DEBUG)
        else: self.logger.setLevel(logging.INFO)

        #3 formatter 지정
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        #4 폴더 생성
        log_path = self.logpath + "%s" %  datetime.today().strftime("%Y%m")
        if os.path.exists(log_path) is not True:
            os.mkdir(log_path)

        #4 handler instance 생성
        self.console = logging.StreamHandler()
        file_name = self.logpath + "%s\logging_%s.log" % (datetime.today().strftime("%Y%m"), datetime.today().strftime("%Y%m%d"))
        self.file_handler = logging.FileHandler(filename=file_name)
        # file_name = self.logpath + "%s\error_%s.log" % (datetime.today().strftime("%Y%m"), datetime.today().strftime("%Y%m%d"))
        # self.efile_handler = logging.FileHandler(filename=file_name)
        
        #5 handler 별로 다른 level 설정
        self.console.setLevel(logging.INFO)
        self.file_handler.setLevel(logging.DEBUG)
        # self.efile_handler.setLevel(logging.WARNING)

        #6 handler 출력 format 지정
        self.console.setFormatter(self.formatter)
        self.file_handler.setFormatter(self.formatter)
        # self.efile_handler.setFormatter(self.formatter)

        #7 logger에 handler 추가
        self.logger.addHandler(self.console)
        self.logger.addHandler(self.file_handler)
        # self.logger.addHandler(self.efile_handler)

    def warning(self, msg):
        self.logger.warning(msg)
        
    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def error(self, msg):
        self.logger.error(msg)

# if __name__ == '__main__':
#     log = Logger()
#     log.warning("warning")
#     log.error("error~~~")
#     log.info("info~~~")
#     log.debug("debug~~~")

