"""
볼린저 밴드를 이용한 반전매매 기법

매수 : 주가가 하단밴드 부근에서 W형 패턴을 나타내고 강세지표가 확증할 때 매수
   (%b가 0.05보다 작고 II%가 0보다 크면 매수)
매도 : 주가가 상단밴드 부근에서 일련의 주가 태그가 일어나며, 약세 지표가 확증할 때 매도
   (%b가 0.95보다 크고 II%가 0보다 작으면 매도)

"""

import matplotlib.pyplot as plt
import pandas as pd
from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()

results = list(mongodb.find_items({'shcode':'035420'}, "stock", "daily_price"))

# dataframe으로 변환
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

# 일중강도 II를 구함
df['II'] = (2*df['close']-df['high']-df['low'])\
    /(df['high']-df['low'])*df['volume']  # ①
# 일중강도율
df['IIP21'] = df['II'].rolling(window=21).sum()\
    /df['volume'].rolling(window=21).sum()*100  # ②
df = df.dropna()

plt.figure(figsize=(9, 9))
plt.subplot(3, 1, 1)
plt.title('SK Hynix Bollinger Band(20 day, 2 std) - Reversals')
plt.plot(df.index, df['close'], 'b', label='Close')
plt.plot(df.index, df['upper'], 'r--', label ='Upper band')
plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
plt.plot(df.index, df['lower'], 'c--', label ='Lower band')
plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')

plt.legend(loc='best')
plt.subplot(3, 1, 2)
plt.plot(df.index, df['PB'], 'b', label='%b')
plt.grid(True)
plt.legend(loc='best')

plt.subplot(3, 1, 3)  # ③
plt.bar(df.index, df['IIP21'], color='g', label='II% 21day')  # ④
plt.grid(True)
plt.legend(loc='best')
plt.show()