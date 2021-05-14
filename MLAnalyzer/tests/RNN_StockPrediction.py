from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from common.db_handler.mongodb_handler import MongoDBHandler

mongodb = MongoDBHandler()
results = list(mongodb.find_items({'shcode':'035420'}, "stock", "daily_price"))

# JSON => dataframe으로 변환
df = pd.DataFrame.from_dict(results, orient='columns')

df.close = df.close.astype(float)
df.open = df.open.astype(float)
df['high'] = df['high'].astype(float)
df['low'] = df['low'].astype(float)
df['volume'] = df['jdiff_vol'].astype(float)

raw_df = df

# mk = Analyzer.MarketDB()
# raw_df = mk.get_daily_price('삼성전자', '2018-05-04', '2020-01-22')

window_size = 10   # 
data_size = 5

def MinMaxScaler(data):
    """최솟값과 최댓값을 이용하여 0 ~ 1 값으로 변환
       숫자 단위가 클수록 계산에 소요되는 시간이 늘어나므로
    """
    numerator = data - np.min(data, 0)
    denominator = np.max(data, 0) - np.min(data, 0)
    # 0으로 나누기 에러가 발생하지 않도록 매우 작은 값(1e-7)을 더해서 나눔
    return numerator / (denominator + 1e-7)

dfx = raw_df[['open','high','low','volume', 'close']]
dfx = MinMaxScaler(dfx)
dfy = dfx[['close']]

x = dfx.values.tolist()
y = dfy.values.tolist()

data_x = []
data_y = []
for i in range(len(y) - window_size):
    _x = x[i : i + window_size] # 다음 날 종가(i+windows_size)는 포함되지 않음
    _y = y[i + window_size]     # 다음 날 종가
    data_x.append(_x)
    data_y.append(_y)
print(_x, "->", _y)

train_size = int(len(data_y) * 0.7)
print("train_size :", train_size)
train_x = np.array(data_x[0 : train_size])
train_y = np.array(data_y[0 : train_size])

test_size = len(data_y) - train_size
test_x = np.array(data_x[train_size : len(data_x)])
test_y = np.array(data_y[train_size : len(data_y)])

# 모델 생성
model = Sequential()
model.add(LSTM(units=10, activation='relu', return_sequences=True, input_shape=(window_size, data_size)))
model.add(Dropout(0.1))  # 과적합 방지
model.add(LSTM(units=10, activation='relu'))
model.add(Dropout(0.1))
model.add(Dense(units=1)) # 유닛이 하나인 출력층을 추가
model.summary()

model.compile(optimizer='adam', loss='mean_squared_error')  # 최적화 도구는 adam, 손실함수는 평균 제곱오차
model.fit(train_x, train_y, epochs=60, batch_size=30)  # epochs는 전체 데이터셋에 대한 학습횟수, batch_size는 한번에 제공되는 훈련데이터 개수
pred_y = model.predict(test_x)

# Visualising the results
plt.figure()
plt.plot(test_y, color='red', label='real SEC stock price')
plt.plot(pred_y, color='blue', label='predicted SEC stock price')
plt.title('SEC stock price prediction')
plt.xlabel('time')
plt.ylabel('stock price')
plt.legend()
plt.show()

# raw_df.close[-1] : dfy.close[-1] = x : pred_y[-1]
# 내일의 종가 출력
# print("raw_df.close[-1]", raw_df.close[-1])
print("pred_y[-1]", pred_y[-1])
print("pred_y[-1]", pred_y)
print("=================================================")
print("dfy.close[-1]", dfy.close)
print("dfy[-1]", dfy)
# print("Tomorrow's SEC price :", raw_df.close[-1] * pred_y[-1] / dfy.close[-1], 'KRW')