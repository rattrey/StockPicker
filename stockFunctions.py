import pandas as pd
from sqlalchemy import create_engine
import psycopg2
########################GROWTH FUNCTIONS###############################
class Growth(object):

    def growthList(self, lst, stepVal):
        '''Takes a list and calculates the change between every stepVal and generates a new list'''
        growthList = []
        x = lst[::stepVal]

        for previous, current in zip(x, x[1:]):
            y = ((current / previous) - 1) * 100
            growthList.append(y)
        
        return growthList


    def posByPeriod(self, lst, intervals, tickerName= ' ', colName='Pos'):
        '''Takes a growthList and generates the average of the positive values in growthList by an interval period (intervals) for a given ticker'''
        z = []
        colNames = []

        for i in range(5,100,intervals):
            x = self.growthList(lst, i)
            z.append((len([i for i in x if i > 0])))
            colNames.append(f'{colName}{i}')

        df = pd.DataFrame(z).transpose()
        df.columns = colNames
        df['ticker'] = f'{tickerName}'
        return df


    def negByPeriod(self, lst, intervals, tickerName=' ', colName='Neg'):
        '''Takes a growthList and generates the average of the negative values in growthList by an interval period (intervals) for a given ticker'''
        z = []
        colNames = []

        for i in range(5,100,intervals):
            x = self.growthList(lst, i)
            z.append((len([i for i in x if i < 0])))
            colNames.append(f'{colName}{i}')

        df = pd.DataFrame(z).transpose()
        df.columns = colNames
        df['ticker'] = f'{tickerName}'
        return df


    def avgGrowthByPeriod(self, lst, intervals, tickerName):
        '''Takes a growthList and generates the average values in growthList by an interval period (intervals) for a given ticker'''
        z = []
        colNames = []

        for i in range(5,100, 10):
            x = sum(self.growthList(lst, i)) / len(self.growthList(lst,i))
            z.append(x)
            colNames.append(f'AvgPd{i}')
        
        df = pd.DataFrame(z).transpose()
        df.columns = colNames
        df['ticker'] = f'{tickerName}'
        return df


########################PostGres Functions###############################


class Postgres(object):
        
    def postgresQuery(self, query):
        try:
            conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
            cur = conn.cursor()
            postgres_script = f"{query}"
            cur.execute(postgres_script)
            return pd.DataFrame(cur.fetchall())
        
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating table", error)


    #get column names from table
    def getColumnNames(self, tableName, columnNames = []):
        conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
        cur = conn.cursor()
        cur.execute(f'''Select * FROM public."{tableName}" LIMIT 0;''')
        colnames = [desc[0] for desc in cur.description]
        return colnames
