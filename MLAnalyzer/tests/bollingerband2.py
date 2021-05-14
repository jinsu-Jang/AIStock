"""
볼린저 밴드 추세 추종매매 기법
MFI 80을 상회하면 강력한 매수신호, MFI가 20을 하회하면 강력한 매도신호
"""

import matplotlib.pyplot as plt
from pandas.io.json import json_normalize
import pandas as pd
from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()

results = list(mongodb.find_items({'shcode':'035420'}, "stock", "daily_price"))

# dataframe으로 변환
df = pd.DataFrame.from_dict(results, orient='columns')
# df = pd.DataFrame.from_dict(json_normalize(results), orient='columns') #중첩된 열이 있을 경우

df['close'] = df['close'].astype(float)
df['MA20'] = df['close'].rolling(window=20).mean()
df['stddev'] = df['close'].rolling(window=20).std()
df['upper'] = df['MA20'] + (df['stddev'] * 2)
df['lower'] = df['MA20'] - (df['stddev'] * 2)
df['PB'] = (df['close'] - df['upper']) / (df['close'] - df['lower'])

df = df[19: ]
print(df['종가'])

plt.figure(figsize=(9,8))
plt.subplot(2,1,1)
plt.plot(df.index, df['close'], color='#0000ff', label='Close')

plt.plot(df.index, df['upper'], 'r--', label='Upper band')
plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
plt.plot(df.index, df['lower'], 'c--', label='Lower band')
plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')

plt.legend(loc='best')
plt.title('Naver Bollinger Band(20day)')

plt.subplot(2,1,2)
plt.plot(df.index, df['PB'], color='b', label='%B')

plt.grid(True)
plt.legend(loc='best')
plt.show()
