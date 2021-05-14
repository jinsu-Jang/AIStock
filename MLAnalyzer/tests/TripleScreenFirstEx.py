import pandas as pd
import mplfinance as mpf


from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()

results = list(mongodb.find_items({'shcode':'035420'}, "stock", "daily_price"))

# JSON => dataframe으로 변환
df = pd.DataFrame.from_dict(results, orient='columns')

df.close = df.close.astype(float)
df.open = df.open.astype(float)
df.high = df.high.astype(float)
df.low = df.low.astype(float)
df['volume'] = df['jdiff_vol'].astype(float)

df.index = pd.to_datetime(df.date)
df = df[['open', 'high', 'low', 'close', 'volume']] 

ema60 = df.close.ewm(span=60).mean()   # ① 종가의 12주 지수 이동평균
ema130 = df.close.ewm(span=130).mean() # ② 종가의 26주 지수 이동평균
macd = ema60 - ema130                  # ③ MACD선
signal = macd.ewm(span=45).mean()      # ④ 신호선(MACD의 9주 지수 이동평균)
macdhist = macd - signal               # ⑤ MACD 히스토그램

apds = [mpf.make_addplot(ema130, color='c'),
    mpf.make_addplot(macdhist, type='bar', panel=1, color='m'),
    mpf.make_addplot(macd, panel=1, color='b'),
    mpf.make_addplot(signal, panel=1, color='g'),
    ]
mc = mpf.make_marketcolors(up='r', down='b', inherit=True) 
stl = mpf.make_mpf_style(marketcolors=mc) 
mpf.plot(df, title='Triple Screen Trading - First Screen (NCSOFT)', type='candle',
    addplot=apds, figsize=(9,7), panel_ratios=(1,1), style=stl)