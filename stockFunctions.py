import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import sklearn
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.linear_model import LinearRegression
import numpy as np
import matplotlib.pyplot as plt
engine = create_engine('postgresql+psycopg2://attrey:tusr3f@127.0.0.1/postgres')
########################PostGres Functions##############################

class Postgres(object):
        
    def postgresQueryDf(self, query):
        try:
            conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
            cur = conn.cursor()
            postgres_script = f"{query}"
            cur.execute(postgres_script)
            return pd.DataFrame(cur.fetchall())
        
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating table", error)

    
    def postgresQueryList(self, query):
        try:
            conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
            cur = conn.cursor()
            postgres_script = f"{query}"
            cur.execute(postgres_script)
            return cur.fetchall()
        
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating table", error)


    #get column names from table
    def getColumnNames(self, tableName, columnNames = []):
        conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
        cur = conn.cursor()
        cur.execute(f'''Select * FROM public."{tableName}" LIMIT 0;''')
        colnames = [desc[0] for desc in cur.description]
        return colnames


    def getTickerList(self, tableName): #returns ticker list from specified table from postgres
        '''Pull entire ticker list from postgres table of choice'''

        t = self.postgresQueryList(f'select ticker from public."{tableName}";')
        tickerList = []
        for item in t:
            tickerList.append(list(item)[0])
        #Cleaning up list
        lst = []
        
        for s in tickerList:
            if s.find('.') == -1:
                lst.append(s)
            else:
                lst.append(s[:s.find('.')])

        return lst



class GrowthFeatures(object):

    #Calculate the growth rate by periods 
    def growthRateByPeriod(self, tableName, weeks):

        for item in range(1,10):

            df = Postgres().postgresQueryDf(f'''select * from public."dailydata_v2" where "col_1" = {item};''')
            df.columns = Postgres().getColumnNames('dailydata_v2')

            tickerList = list(set(df['ticker']))
            counter = len(tickerList)

            for ticker in tickerList:
                try:
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
                    df4.to_sql(f'{tableName}', engine, if_exists='append')
                    print(ticker, counter)
                    counter -= 1

                except Exception as error:
                    print(ticker, error)
                    counter -= 1
                    pass

