from datetime import datetime
from konlpy.tag import Komoran
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from difflib import SequenceMatcher
from db_handler.mongodb_handler import MongoDBHandler

# komoran = Komoran()

# print(komoran.nouns("삼성증권은 올해 1분기 매출액이 전년 동기 대비 21.65% 늘어난 3조2993억원을 기록했다고 7일 밝혔다. 세전이익은 4027억원으로 1708%, 당기순이익은 2890억원으로 1776% 급증했다. 분기 기준으로 세전이익과 당기순익 모두 사상 최대 실적 기록이다."\
#     "리테일 부문은 우호적 시장환경과 시의적절한 영업활동에 힘입어 양호한 성과를 시현했다는 평가다. 순수탁수수료는 2408억원으로 사상 최대 실적을 경신했다. 국내주식은 전년 동기 대비 92%, 해외주식은 148% 증가했다."\
#     "1억원 이상 개인 고객은 20만명을 돌파했다. 리테일 고객 예탁자산은 1분기에만 10조원 순유입되며 280조원을 기록했다. 본사영업부문도 전 부문에서 호실적을 내며 전사 실적 개선에 기여했다."\
#     "삼성증권 관계자는 투자은행(IB) 부문은 주식자본시장(ECM), 구조화금융의 가파른 성장을 바탕으로 전년 동기 대비 55% 증가했다며 운용부문도 금리 변동성 확대에 선제적으로 대응하며 양호한 실적을 달성했다고 설명했다."))

class KeywordAnalyzer(object):
    def __init__(self):
        self.mongodb = MongoDBHandler()
        self.komoran = Komoran()
        self.rdate   = datetime.today().strftime("%Y%m%d")
        self.articles = [] 
        self.font_path = '../DownloadLib/NanumFont_TTF_ALL/NanumGothic.ttf'
        self.code_list = []

        

    def set_read_cond_article(self, rdate=None):
        if rdate is None:
            self.rdate =  datetime.today().strftime("%Y%m%d")
        else:
            self.rdate = rdate

    def read_article_from_mongodb(self):
        self.articles = list(self.mongodb.find_items({'category_name': '경제','text_company' : '한국경제', 'time': {"$gte":'2021.05.07'}}, "stock", "Naver_news"))
        print("article len:", len(self.articles))
        for article in self.articles:
            print(article["text_sentence"])
            # nouns = self.komoran.nouns(article["text_sentence"])
        # print(nouns)

    def make_word_cloud_image(self, words=None):
        word_cloud = WordCloud(
            font_path=self.font_path,
            width=800,
            height=800,
            background_color="white",
        )        
        # 추출된 단어 빈도수 목록을 이용해 워드 클라우드 객체를 초기화 합니다.
        word_cloud = word_cloud.generate_from_frequencies(words)
        # 워드 클라우드를 이미지로 그립니다.
        fig = plt.figure(figsize=(10, 10))
        plt.imshow(word_cloud)
        plt.axis("off")
        # 만들어진 이미지 객체를 파일 형태로 저장합니다.
        fig.savefig("./{0}.png".format("file_name12121"))

    def keyword_analyzing(self):
        # for article in self.articles:
        #     nouns = self.komoran.nouns(article['text_sentence'])
        nouns = self.komoran.nouns("삼성증권은 올해 1분기 매출액이 전년 동기 대비 21.65% 늘어난 3조2993억원을 기록했다고 7일 밝혔다. 세전이익은 4027억원으로 1708%, 당기순이익은 2890억원으로 1776% 급증했다. 분기 기준으로 세전이익과 당기순익 모두 사상 최대 실적 기록이다."\
            "리테일 부문은 우호적 시장환경과 시의적절한 영업활동에 힘입어 양호한 성과를 시현했다는 평가다. 순수탁수수료는 2408억원으로 사상 최대 실적을 경신했다. 국내주식은 전년 동기 대비 92%, 해외주식은 148% 증가했다."\
            "1억원 이상 개인 고객은 20만명을 돌파했다. 리테일 고객 예탁자산은 1분기에만 10조원 순유입되며 280조원을 기록했다. 본사영업부문도 전 부문에서 호실적을 내며 전사 실적 개선에 기여했다."\
            "삼성증권 관계자는 투자은행(IB) 부문은 주식자본시장(ECM), 구조화금융의 가파른 성장을 바탕으로 전년 동기 대비 55% 증가했다며 운용부문도 금리 변동성 확대에 선제적으로 대응하며 양호한 실적을 달성했다고 설명했다.")

        processed = [n for n in nouns if len(n) >= 2]
        count = Counter(processed)

        print(count)

        wordcounts = {}
        faverCode = []       # 관심종목
        faverCodeScore = {}  # 관심종목 점수
        wordScores = {'최대':5, '양호':2, '증가': 2, '급증':4, '돌파':2, '확대':2, '개선':2, '달성': 2,
                   '최소':-5, '저조':-2, '감소': -2, '급락':-4, '돌파':-2, '축소':-2, '미달': -2, }
        score = 0
        self.code_list = list(self.mongodb.find_items({"증권그룹" : "01"}, "stock", "code_info"))
        # 출현 빈도가 높은 max_count 개의 명사만을 추출합니다.
        for n, c in count.most_common(30):
            wordcounts[n] = c
            if n in wordScores:
                score += wordScores[n] * c
            for code in self.code_list:
                strCode = str(code["종목명"])
                # 종목명의 비교하여 80% 이상 일치하면 관심종목에 추가
                ratio = SequenceMatcher(None, n, strCode).ratio()
                if ratio > 0.8:
                    faverCode.append(strCode)

        if len(faverCode) == 1:
            faverCodeScore.extend({faverCode: score})
        else: 
            print("faver Code", faverCode, score)
            
        self.make_word_cloud_image(wordcounts)
        print(wordcounts)
        # 추출된 단어가 하나도 없는 경우 '내용이 없습니다.'를 화면에 보여줍니다.
        # if len(result) == 0:
        #     result["내용이 없습니다."] = 1
        # return result

    
    def start(self):
        # MultiProcess 크롤링 시작

        print("dd")
        # for category_name in self.selected_categories:
        #     proc = Process(target=self.keyword_analyzing, args=(category_name,))
        #     proc.start()

if __name__ == "__main__":
    keyanalyzer = KeywordAnalyzer()
    today =  datetime.today().strftime("%Y%m%d")
    keyanalyzer.set_read_cond_article(today)
    # keyanalyzer.read_article_from_mongodb()
    keyanalyzer.keyword_analyzing()
    # keyanalyzer.start()
    
