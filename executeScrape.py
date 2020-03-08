import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import sklearn
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.linear_model import LinearRegression
import numpy as np
import matplotlib.pyplot as plt
engine = create_engine('postgresql+psycopg2://attrey:tusr3f@127.0.0.1/postgres')


Industry = Postgres().postgresQueryDf(f'''select distinct "col_1" from public."dailydata_v2";''')
Industry[0][1]


for i in range(1,10):
    print(i)




df = Postgres().postgresQueryDf(f'''select * from public."DailyData_v2" where "col_1" = '{Industry[0][8]}');''')
df.columns = Postgres().getColumnNames('DailyData')




tickerList = list(set(df['ticker']))
counter = len(tickerList)

ticker = 'AMD'
weeks = 4*12

df2 = df[df['ticker'] == f'{ticker}']
df2 = df2.sort_values(by = ['date'], ascending = False)

        #change number of 30 day periods here
        #input for range cycle, number of days
TotalDays = weeks * 5
q = list(range(TotalDays//10, TotalDays, TotalDays//10))
#change number of 30 day periods here
growthPercent = []

for num in q:
    lst = df2[0:num]['close'].tolist()
    growthRate = ((lst[1] / lst[-1]) - 1) * 100
    growthPercent.append(growthRate)

df3 = pd.DataFrame(growthPercent).transpose()
df3.columns = [f'Period{i + 1}' for i,v in enumerate(q)]
df3['ticker'] = f'{ticker}'
df3['industry'] = Industry
df2 = df2[0:num]


#Linear Regression Model
Y = np.array(df3.iloc[:,1:-1])
Y = np.array(Y).reshape(-1,1)

X = [i for i in range(1,len(df3.iloc[:,1:-1].columns) + 1)] #creates index value for number of periods in Y
X = np.array(X).reshape(-1,1)

Predicted = LinearRegression().fit(X, Y).predict(X)

df3['R2'] = r2_score(Y, Predicted)
df3['MSE'] = mean_squared_error(Y, Predicted)
df3['coefficient'] = LinearRegression().fit(X,Y).coef_
df3['currentPrice'] = df2['close'].head(1).values[0]
df3['averagePrice'] = df2['close'][:TotalDays].mean()
df3['minPrice'] = df2['close'][:TotalDays].min()
df3['maxPrice'] = df2['close'][:TotalDays].max()
df3['totalGrowth'] = ((df2['close'].values[1] / df2['close'].values[-1]) - 1) * 100
df3['variance'] = np.var(np.array(df2['close']))


periodStatistics = np.array(df3.transpose()[:-10])
df3['totalPeriods'] = len(periodStatistics)
df3['pGreaterThan0'] = len(periodStatistics[periodStatistics > 0])
df3['pGreaterThan10'] = len(periodStatistics[periodStatistics > 10])
df3['pGreaterThan25'] = len(periodStatistics[periodStatistics > 25])
df3['pGreaterThan35'] = len(periodStatistics[periodStatistics > 35])
df3['pGreaterThan45'] = len(periodStatistics[periodStatistics > 45])
df3['pGreaterThan55'] = len(periodStatistics[periodStatistics > 55])

df4 = df3.transpose()['ticker':].transpose()
#df4.to_sql(f'{tableName}', engine, if_exists='append')

