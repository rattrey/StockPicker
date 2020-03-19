import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import psycopg2
import datetime
from datetime import datetime
import alpha_vantage
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.sectorperformance import SectorPerformances
import pandas as pd
from sqlalchemy import create_engine
import sys
from datetime import date
import re
from stockFunctions import Postgres
from urllib.request import urlopen
import json
from stockFunctions import Postgres
###############Sector Data Pull#################

class StockScraper(object):

    engine = create_engine('postgresql+psycopg2://postgres:tusr3f@127.0.0.1/postgres')

    def __init__(self):
        pass

    #finds nth value of a string object - this will eventually need to go into generic functions file
    def find_nth(self, haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start + len(needle))
            n -= 1
        return start

 
    def scrapeTickerList(self, tableName, n = 10): #scrape list of all tickers list from gurufocus website
        """Scrape all tickers from website www.gurufocus.com n = the number of items displayed on page - n can be 10, 30, 100"""
        string = 'abcdefghijklmnopqrstuvwxyz'
        try:

            for letter in string.upper():
                url = requests.get(f'https://www.gurufocus.com/stock_list.php?m_country%5B%5D=USAS&m_country[]=USA&m_country[]=CAN&a={letter}&p=0&n={n}')
                soup = BeautifulSoup(url.text, "html.parser")

                z = soup.find('div', attrs = {'class':'page_links'}).find_all('a',  href=True)[10].contents #extract last page contents from a tag
                lastPage = int(re.findall(r'\d+', str(z))[0]) #extract value of list and 

                for x in range(0, lastPage - 1):
                    url2 = requests.get(f'https://www.gurufocus.com/stock_list.php?m_country%5B%5D=USAS&m_country[]=USA&m_country[]=CAN&a={letter}&p={x}&n={n}')
                    soup2 = BeautifulSoup(url2.text, 'html.parser')

                    lst = []
                    lst2 = []
                    for i in range(0, n*7, 7):
                        j = soup2.find_all('a', attrs={'class':'nav'})[i].contents[0]
                        lst.append(j)

                    for l in range(1,11):
                        k = soup2.find('table', id = 'R1').find_all('tr')[l].find_all('a')[1].contents[0]
                        lst2.append(k)
                    
                    df = pd.DataFrame({'ticker':lst, 'Name':lst2})
                    df.to_sql(f'{tableName}', self.engine, if_exists = 'append')

                print(letter)
        
        except Exception as error:
            print(error)                
        
        print(f'Stock List Upload to {tableName} is Complete')


    def scrapeSectorData(self, tableName): #scrapes sector data from alphaVantage
        '''pull sector data and impute into postgres tableName'''

        sp = SectorPerformances('Q2H4NWX3SZPHCUG8', output_format='pandas')
        df = pd.DataFrame(sp.get_sector()[0])
        df.columns = ['real_time_performance', 'real_time_performance2', 'day_performance_1', 'day_performance_5', 'day_performance_30', 'day_performance_90', 'performance_ytd', 'year_performance_1', 'year_performance_5', 'year_performance_10']
        df['date'] = date.today()
        df.to_sql(f'{tableName}', self.engine, if_exists='append')
        print('sector data pulled successfully')



    #premiumKey = ['Q2H4NWX3SZPHCUG8']
    #apiKey = ['F5SF7AJGHC9K0FMQ', 'XBPW67167NS5O3VM', 'DHCLP2EWLYEKXS5M', 'ZR4K4D5LCGX3H0GA', 'RJ1MLP7AXU22WE07' 'XL344TV8YGUMBEAG', 'O1NBEIK2KP29NUVH', '0D5HKL8COYR611B7', 'W9H0I01Q5B1L776Q']
    def scrapeDailyData(self, tableName):
        '''Scrape alpha vantage daily stock data and store data into postgres table of choice'''

        tickerListMstr = Postgres().getTickerList('TickerList')
        tickerListMstr.sort()
        counter = len(tickerListMstr)

        ts = TimeSeries('Q2H4NWX3SZPHCUG8', output_format='pandas')

        for ticker in tickerListMstr:
            try:
                stock = ts.get_daily_adjusted(symbol=f'{ticker}', outputsize='compact')
                #All other metrics
                df = pd.DataFrame(stock[0])
                df['ticker'] = [f'{ticker}' for i in df.transpose()]
                df.columns = ['open', 'high', 'low', 'close', 'adjusted_close','volume', 'dividend_amount', 'split_coefficient', 'ticker']
                df.to_sql(f'{tableName}', self.engine, if_exists='append')

                counter -= 1
                print(counter, ticker)
                time.sleep(2)

            except Exception:
                counter -= 1
                print (counter, f'{ticker} not found on alphaVantage')
                pass
        
        print('Complete')


    def scrapeIndustryList(self, tableName):
        fullList = Postgres().getTickerList('tickerList')
        fullList.sort()
        counter = len(fullList)

        for ticker in fullList:
            try:
                industryList = []
                companyList = []

                url_data = requests.get(f'https://www.gurufocus.com/stock/{ticker}/summary')
                soup = BeautifulSoup(url_data.text, 'html.parser')

                x = str(soup)

                if x.rfind('Basic Materials') > 1:
                    y = x[x.rfind('Basic Materials'):x.rfind('Basic Materials')+len('Basic Materials')]
                elif x.rfind('Consumer Cyclical') > 1:
                    y = x[x.rfind('Consumer Cyclical'):x.rfind('Consumer Cyclical')+len('Consumer Cyclical')]
                elif x.rfind('Real Estate') > 1:
                    y = x[x.rfind('Real Estate'):x.rfind('Real Estate')+len('Real Estate')]
                elif x.rfind('Consumer Defensive') > 1:
                    y = x[x.rfind('Consumer Defensive'):x.rfind('Consumer Defensive')+len('Consumer Defensive')]
                elif x.rfind('Healthcare') > 1:
                    y = x[x.rfind('Healthcare'):x.rfind('Healthcare')+len('Healthcare')]
                elif x.rfind('Utilities') > 1:
                    y = x[x.rfind('Utilities'):x.rfind('Utilities')+len('Utilities')]
                elif x.rfind('Communication Services') > 1:
                    y = x[x.rfind('Communication Services'):x.rfind('Communication Services')+len('Communication Services')]
                elif x.rfind('Energy') > 1:
                    y = x[x.rfind('Energy'):x.rfind('Energy')+len('Energy')]
                elif x.rfind('Industrials') > 1:
                    y = x[x.rfind('Industrials'):x.rfind('Industrials')+len('Industrials')]
                elif x.rfind('Financial Services') > 1:
                    y = x[x.rfind('Financial Services'):x.rfind('Financial Services')+len('Financial Services')]
                elif x.rfind('Technology') > 1:
                    y = x[x.rfind('Technology'):x.rfind('Technology')+len('Technology')]

                companyList.append(ticker)
                industryList.append(y)
                counter -= 1
                print(counter, ticker, y)

                zippedList = zip(companyList, industryList)
                df = pd.DataFrame(zippedList, columns = ['ticker' , 'Industry']) 
                df.to_sql(f'{tableName}', self.engine, if_exists='append')
            except Exception:
                pass


    def scrapeMarketBeat(self, tableName):

        tickerListMstr = Postgres().getTickerList('tickerList')
        tickerListMstr.sort()
        counter = len(tickerListMstr)

        for ticker in tickerListMstr[:]:
            try:
                url_data = requests.get(f'https://www.marketbeat.com/stocks/NYSE/{ticker}/', timeout = 5)
                soup = BeautifulSoup(url_data.text, 'html.parser')


                #Determine the most recent date this was updated
                Date = soup.find('div', attrs={'class':'price-updated'}).contents[0]
                Date = Date[self.find_nth(Date,' ', 2) + 1 : self.find_nth(Date, ' ', 3)]
                ######This code pulls the following in order: Volume, Average Volume, Market Capitalization, P/E Ratio, Dividend Yield, and Beta#####
                companyProfile = []
                for num in range(0,6):
                    v = soup.find('div', attrs={'class':'col-lg-12 col-xl-9 price-data-section'}).find_all('strong')[num].contents
                    companyProfile.append(re.sub(r'[^\d.]', '', str(v)))

                #####This code pulls the following in order######
                #Industry Information: stock exchange, industry, sub-industry, sector, current symbol, previous symbol, CUSIP, CIK, web, phone
                #Debt: debt to equity ratio, current ratio, quick ratio#
                #Price to Earnings: trailing P/E Ratio, Forward P/E Ratio, P/E Growth#
                #Sales & Book Value: Annual Sales, Price/Sales, Cash Flow, Price / Cash Flow, Book Value, Price / Book#
                #Profitability: EPS (Most recent fiscal year), net income, net margins, return on equity, return on assets#
                #Miscellaneous: Employees, outstanding shares, market cap, next earnings Date (Estimated), Optionable#
                financeInfo = []
                for num in range(0,len(soup.find('div', attrs={'class':'row price-data-section'}).find_all('strong'))):
                    v = soup.find('div', attrs={'class':'row price-data-section'}).find_all('strong')[num].contents
                    financeInfo.append(re.sub(r'[^\d.()-]', '', str(v)))

                #Determinds the question number that contains the competitor information
                competitorList = []

                for num in range(1,len(soup.find_all(itemprop="name text"))):
                    if str(soup.find_all(itemprop="name text")[num].contents)[2:17] == 'Who are some of':
                        questionNumber = soup.find_all(itemprop="name text")[num].get('id')       
                        answerNumber = 'answer' + questionNumber[-1:]  
                #passess the above answerNumber to extract the competitor list
                        try:
                            for num in range(1,50,2):
                                competitorName = soup.find(id = f'{answerNumber}').contents[num].contents[0]
                                competitorList.append(competitorName)
                        except Exception:
                            pass

                #Community Rating#
                communityRating = []
                for c in range(1,12):    
                    if c in [2,5,8,11]:
                        v = soup.find('table').find_all('td')[c].contents[0]
                        communityRating.append(re.sub(r'[^\d.]', '', v))

                mstrList = ([Date] + [ticker] + companyProfile + financeInfo[9:-1] + communityRating)
                mstrList.append(len(competitorList))

                df = pd.DataFrame([mstrList],columns=['Date','ticker', 'volume','avgVolume', 'mktCap', 'peRatio', 'dividendYield', 'Beta', 'phone', 'debtEquityRatio', 'currentRatio', 'quickRatio', 'trailingPERatio', 'forwardPERatio', 'PEGrowth', 'annualSales', 'priceSalesRatio', 'cashFlow', 'priceCashFlowRatio', \
                    'bookValue', 'priceBookRatio', 'eps_mostRecentFY', 'netIncome', 'netMargin', 'returnOnEquity', 'returnOnAssets', 'employees', 'outstandingShares', 'mktCap2', 'nextEarningsDate','communityRating', 'outperformVotes', 'underperformVotes', 'total_votes', 'numberofCompetitors'])

                df.to_sql(f'{tableName}', self.engine, if_exists='append')     
                counter -= 1
                print (counter, f'{ticker}')
            
            except Exception:
                counter -= 1
                print (counter, f'{ticker} not found on marketBeat')
                pass




class DataPull(object):

    def pullSingleStock(self, ticker):
        ts = TimeSeries('Q2H4NWX3SZPHCUG8', output_format='pandas')
        stock = ts.get_daily_adjusted(symbol=f'{ticker}', outputsize='full')
        #All other metrics

        df = pd.DataFrame(stock[0])
        df['ticker'] = [f'{ticker}' for i in df.transpose()]
        df.columns = ['open', 'high', 'low', 'close', 'adjusted_close','volume', 'dividend_amount', 'split_coefficient', 'ticker']
        return df



class FinancialScrape(object):
    engine = create_engine('postgresql+psycopg2://postgres:tusr3f@127.0.0.1/postgres')

    def __init__(self):
        pass


    def get_jsonparsed_data(self, url):
    #'''Receive the content of url, parse it as JSON and return the object. Parameters url : str Returns dict '''
        response = urlopen(url)
        data = response.read().decode("utf-8")
        y = json.loads(data)
        x = y['financials']
        df = pd.DataFrame.from_dict(x)
        return df

    

    def financialScrape(self, statementTypeSite, tableName, tickerListTable):
        """
        Scrape financial statements from "https://financialmodelingprep.com/api/v3/financials/income-statement/AAPL?period=quarter"
        Parameters
        ----------
        statementTypeSite : str - used to determine the proper url for which to pull and can be one of the following:
        'balance-sheet', 'income-statement', or 'cash-flow'
        tableName: str - table name for database upload
        
        """
        #get ticker list from financialscrape website allowing to gather a larger repository
        tickerList = Postgres().getTickerList(f'{tickerListTable}')
        counter = len(tickerList)

        for ticker in tickerList:
            try:
                url = (f"https://financialmodelingprep.com/api/v3/financials/{statementTypeSite}-statement/{ticker}?period=quarter")

                df2 = self.get_jsonparsed_data(url)
                df2.columns = [re.sub(r'\W+','', i) for i in df2.columns]
                df2['ticker'] = f'{ticker}'
                df2.to_sql(f'{tableName}', self.engine, if_exists='append')
                counter -= 1
                print(ticker, counter)
            except Exception:
                counter -= 1
                print(f'{ticker} {counter} not found')
                pass

