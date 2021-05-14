"""
볼린저 밴드 추세 추종매매 기법
%b가 0.8보다 크고 MFI 80을 상회하면 강력한 매수신호, 
%b가 0.2보다 작고 MFI가 20을 하회하면 강력한 매도신호
"""
import matplotlib.pyplot as plt
import pandas as pd
from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()

results = list(mongodb.find_items({'shcode':'035420'}, "stock", "daily_price"))

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

print(df)
# 상승 하락유무 체크
for i in range(len(df.close)):
    if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:       # ①
        print("매수신호")
    elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:     # ③
        print("매도신호")

plt.figure(figsize=(9, 8))
plt.subplot(2, 1, 1)
plt.title('NAVER Bollinger Band(20 day, 2 std) - Trend Following')
plt.plot(df.index, df['close'], color='#0000ff', label='Close')
plt.plot(df.index, df['upper'], 'r--', label ='Upper band')
plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
plt.plot(df.index, df['lower'], 'c--', label ='Lower band')
plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')
for i in range(len(df.close)):
    if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:       # ①
        plt.plot(df.index.values[i], df.close.values[i], 'r^')  # ②
    elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:     # ③
        plt.plot(df.index.values[i], df.close.values[i], 'bv')  # ④
plt.legend(loc='best')

plt.subplot(2, 1, 2)
plt.plot(df.index, df['PB'] * 100, 'b', label='%B x 100')       # ⑤ 
plt.plot(df.index, df['MFI10'], 'g--', label='MFI(10 day)')     # ⑥
plt.yticks([-20, 0, 20, 40, 60, 80, 100, 120])                  # ⑦
for i in range(len(df.close)):
    if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:
        plt.plot(df.index.values[i], 0, 'r^')
    elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:
        plt.plot(df.index.values[i], 0, 'bv')
plt.grid(True)
plt.legend(loc='best')
plt.show();   