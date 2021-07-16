import pandas as pd
import numpy as np

lst_A = ['a','b','c', 'a', 'a', 'b', 'c', 'c']
lst_B = [10,15,20,15,30,40,35,]

df = pd.DataFrame([ x for x in zip(lst_A,lst_B)], columns=['Data','Score'])

# print(df.groupby('Data').count())
print(df)
# print(df.groupby('Data').count())
# print(df.groupby('Data').agg(['count','max','min','sum','mean']))

# df.groupby('Data').count()
# df.groupby('Data').max()
# df.groupby('Data').agg(['count','max','min','sum','mean'])

# df['Rank']=df.groupby('Data')['Score'].rank(ascending=False)

# print(df)

# df_rank = df[df['Rank'] < 3]

# print(df_rank)

##############  데이터 프레임의 타입 변경 astype() ########################

# print(df.dtypes)

# #df = df.astype('float') #모든 컬럼 변경
# df = df.astype({'Score': 'float'}) #특정 컬럼 변경
# print(df.dtypes)
# print(df)

# # 전체 열 이름 입력하기
# df.columns = ['col1', 'col2']
# print(df)
# # 선택하여 열 이름 변경하기
# df.rename(columns={'col1':'Data', 'col2':'Score'}, inplace=True)
# print(df)

########################nan or inf 숫자로 바꾸기 #######################

var = np.array([None, 7.0], dtype=np.float16)
print(var)

np.nan_to_num(var, copy=False)
print(var)

