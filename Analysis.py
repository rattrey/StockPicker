from stockFunctions import Postgres
import plotly.express as px
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans
from scipy.stats import pearsonr
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import math
import seaborn as sns

correlationData = Postgres().postgresQueryDf('''select * from "Regression_12Month" where "ticker" IN ('MSFT', 'AAPL', 'SQ', 'SHOP', 'TDOC', 'AMZN', 'AXP', 'ADBE', 'AMT', 'BL', 'WORK', 'GLPG');''')
correlationData.columns = Postgres().getColumnNames('Regression_12Month')
correlationData.head()


correlationData1 = correlationData.transpose()[1:-8]
correlationData1.columns = correlationData.transpose().loc['ticker', :].values.tolist()
correlationData1 = correlationData1.reset_index(drop  = True)

corrMatrix = DataFrame(correlationData1.astype(float)).corr()
sns.heatmap(corrMatrix, annot = True)
plt.show()











