import numpy as np
from scraperRo import DataPull
import matplotlib.pyplot as plt
from stockFunctions import GrowthFeatures
import pandas as pd

def growth(lst, stepVal):
    growthList = []
    x = lst[::stepVal]

    for previous, current in zip(x[1:], x):
        y = ((current / previous) - 1) * 100
        growthList.append(y)

    return growthList




df = DataPull().pullSingleStock('MSFT')

stock_price = df[60:365]
stock_price


stock_growth = np.array(growth(stock_price['close'], 1))
stock_growth = np.append(stock_growth, 0)
stock_price['growth'] = stock_growth


x = pd.DataFrame(stock_price).reset_index().sort_values(by=['close'])
bin_labels = [1,2,3,4,5,6,7]
x['bins'] = pd.qcut(x['growth'], q = 7, labels = bin_labels)


stock_growth = np.array(x['bins'])


plt.figure(figsize=(12.5, 3.5))
n_count_data = len(stock_growth)
plt.bar(np.arange(0, len(stock_growth)), stock_growth, color="#348ABD")
plt.show()



#Distribution
# Function to draw random gamma variate
rgamma = np.random.gamma

def rcategorical(probs, n=None):
    # Function to draw random categorical variate
    return np.array(probs).cumsum().searchsorted(np.random.sample(n))

#In order to generate probabilities for the conditional posterior of tau, we need the kernal of the gamma density
dgamma = lambda lam, a, b: lam**(a-1) * np.exp(-b*lam)
alpha, beta = 1., 10

# Specify number of iterations & Initialize trace of samples
n_iterations = 1000
lambda1, lambda2, tau = np.zeros((3, n_iterations+1))

lambda1[0] = 6
lambda2[0] = 2
tau[0] = 50


# Sample from conditionals
for i in range(n_iterations):
    # Sample early mean
    lambda1[i+1] = rgamma(stock_growth[:int(tau[i])].sum() + alpha, 1./(tau[i] + beta))
    # Sample late mean
    lambda2[i+1] = rgamma(stock_growth[int(tau[i]):].sum() + alpha, 1./(n_count_data - tau[i] + beta))
    # Sample changepoint: first calculate probabilities (conditional)
    p = np.array([dgamma(lambda1[i+1], stock_growth[:t].sum() + alpha, t + beta)*
             dgamma(lambda2[i+1], 
                    stock_growth[t:].sum() + alpha, 
                    n_count_data - t + beta)
             for t in range(n_count_data)])
    # ... then draw sample
    tau[i+1] = rcategorical(p/p.sum())


param_dict = {r'$\lambda_1$':lambda1, r'$\lambda_2$':lambda2, r'$\tau$':tau}
for p in param_dict:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].plot(param_dict[p][100:])
    axes[0].set_ylabel(p, fontsize=20, rotation=0)
    axes[1].hist(param_dict[p][int(n_iterations/2):])


plt.show()


x[x['bins'] == 3]['growth'].sum() + x[x['bins'] == 4]['growth'].sum()

