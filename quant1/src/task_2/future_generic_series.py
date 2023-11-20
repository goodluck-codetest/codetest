"""
Futures contracts have a limited lifespan that will influence 
the outcome of your trades and exit strategy. 
The two most important expiration terms are expiration and rollover. 
Please construct a continuous future series according to 
https://ibkrguides.com/tws/usersguidebook/technicalanalytics/continuous.htm

You have 2 csv files which contain future reference 
and daily market data, please implement below requirements

Data quality check
Build generic index future series with suffix “c1” 
which points to the futures with nearest-to-expiry. 
Similarly, c2 means 2nd nearest to expiry. 
please construct generic series for IF future: IFc1, IFc2, IFc3.

Build generic commodity future series with suffix “v1” 
which points to the most active futures, 
“active” can be either measured by volume or 
open-interest or composite rules. 
please construct generic series for P future: Pv1, Pv2, Pv3

Note: if one future has been rolled from near to far, 
it can’t be rolled back again. 
E.g., Pv1 (P2101) on day1, then rolls to Pv1(P2102) on day2. 
It’s not allowed to roll back Pv1(P2101) on day 3.
Apply price adjustment according to ratio defined in reference page
Please visualize the rolling path and adjusted price
"""

import pandas as pd

import matplotlib.pyplot as plt

from ..task_1.equity_tick import DataQualityChecks

import pandas as pd
import matplotlib.pyplot as plt

#%%
class FutureData:
    def __init__(self, future_price_file, future_ref_file):
        self.future_price = pd.read_csv(future_price_file)
        self.future_ref = pd.read_csv(future_ref_file)

    def merge_and_filter(self):
        merged_data = pd.merge(self.future_price, self.future_ref, on='ts_code')
        listed_futures = merged_data[(merged_data['list_date'] <= merged_data['trade_date']) & (merged_data['delist_date'] > merged_data['trade_date'])]
        return listed_futures.sort_values(by=['ts_code','trade_date','delist_date'])

    def process_index_futures(self, futures):
        index_futures = futures[futures['fut_code'] == 'IF']
        index_futures['expiry_rank'] = index_futures.groupby('trade_date')['delist_date'].rank(method='min')
        index_futures['generic'] = index_futures['expiry_rank'].map({1: 'IFc1', 2: 'IFc2', 3: 'IFc3'})
        index_futures = index_futures[index_futures['generic'].isin(['IFc1','IFc2','IFc3'])]
        index_futures['contract_change'] = index_futures['ts_code'].shift(-1) != index_futures['ts_code']
        return index_futures
    
    def process_commodity_futures(self,futures):
        commodity_futures = futures[futures['fut_code'] == 'P']
        commodity_futures['activity_rank'] = commodity_futures.groupby('trade_date')['volume'].rank(method='min')
        commodity_futures['generic'] = commodity_futures['activity_rank'].map({1: 'Pv1', 2: 'Pv2', 3: 'Pv3'})
        commodity_futures = commodity_futures[commodity_futures['generic'].isin(['Pv1','Pv2','Pv3'])]
        commodity_futures['contract_change'] = commodity_futures['ts_code'].shift(-1) != commodity_futures['ts_code']
        return commodity_futures        

    def adjust_close(self, futures):
        ratio = futures['close'] / futures['close'].shift(-1)
        futures['adj_factor'] = (futures['contract_change'].shift().fillna(0) * ratio).replace(0, 1).cumprod()
        futures['adj_close'] = futures['adj_factor'] * futures['close']
        futures['adj_close'] = futures['adj_close'].round(1)
        return futures.sort_values('trade_date')

    def export_data(self, futures, filename):
        futures.to_csv(filename, index=False)

    def plot(self, futures):
        plt.figure(figsize=(20, 10))
        plt.plot(futures['trade_date'], futures['close'], label='Original Close Price')
        plt.plot(futures['trade_date'], futures['adj_close'], label='Adjusted Close Price')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.grid()
        plt.title('Original and Adjusted Close Prices Over Time')
        plt.show()

class MyAnalysis:
    def __init__(self, future_price_file, future_ref_file):
        self.data = FutureData(future_price_file, future_ref_file)

    def run(self, fut_code, activity_measure, data_filename, plot_filename):
        futures = self.data.merge_and_filter()
        index_futures = self.data.process_index_futures(futures)
        commodity_futures = self.data.process_commodity_futures(futures)
        index_futures = self.data.adjust_close(index_futures)
        commodity_futures = self.data.adjust_close(commodity_futures)
        self.export_data(index_futures, 'output/task_2/futures_rolling/index_futures.csv')
        self.export_data(commodity_futures, 'output/task_2/futures_rolling/commodity_future.csv')
        self.plot(index_futures, 'index_future_plot.png')
        self.plot(commodity_futures, 'commodity_future_plot.png')
# usage

#%%
# class FutureRolling():

#     def __init__(self):


#     def run():
#         # Merge `future_price` and `future_ref` on the common columns
#         merged_data = pd.merge(future_price, future_ref, on='ts_code')

#         # Filter futures that are still listed
#         listed_futures = merged_data[(merged_data['list_date'] <= merged_data['trade_date']) & (merged_data['delist_date'] > merged_data['trade_date'])]

#         listed_futures = listed_futures.sort_values(by=['ts_code','trade_date','delist_date'])

#         index_futures = listed_futures[listed_futures['fut_code'] == 'IF']

#         # Get rank of expiry within each date
#         index_futures['expiry_rank'] = index_futures.groupby('trade_date')['delist_date'].rank(method='min')

#         # Assign generic series based on rank
#         index_futures['generic'] = index_futures['expiry_rank'].map({1: 'IFc1', 2: 'IFc2', 3: 'IFc3'})

#         index_futures = index_futures[index_futures['generic'].isin(['IFc1','IFc2','IFc3'])]

#         index_futures['contract_change'] = index_futures['ts_code'].shift(-1) != index_futures['ts_code']

#         # Calculate the ratio based on your reference
#         ratio = index_futures['close'] / index_futures['close'].shift(-1)

#         # Create a cumulative product of the ratios after each contract change
#         index_futures['adj_factor'] = (index_futures['contract_change'].shift().fillna(0) * ratio).replace(0, 1).cumprod()

#         # Adjust the close prices
#         index_futures['adj_close'] = index_futures['adj_factor'] * index_futures['close']

#         #rounding
#         index_futures['adj_close'] = index_futures['adj_close'].round(1)

#         plt.figure(figsize=(20, 10))

#         index_futures = index_futures.sort_values('trade_date')

#         # Plot original close prices
#         plt.plot(index_futures['trade_date'], index_futures['close'], label='Original Close Price')

#         # Plot adjusted close prices
#         plt.plot(index_futures['trade_date'], index_futures['adj_close'], label='Adjusted Close Price')

#         plt.xlabel('Date')
#         plt.ylabel('Price')
#         plt.legend()
#         plt.grid()
#         plt.title('Original and Adjusted Close Prices Over Time')
#         plt.show()        

#%%
def main():
    future_ref = pd.read_csv('data/future_ref.csv')
    future_price = pd.read_csv('data/future_price.csv')
    # future_price = pd.read_csv(os.path.join(os.path.join(os.getcwd(),'data','future_price.csv')))
    # future_ref = pd.read_csv(os.path.join(os.path.join(os.getcwd(),'data','future_ref.csv')))
    future_ref_check = DataQualityChecks(future_ref,'future_ref')
    future_ref_check.run_ref_data()
    future_ref_check.write_to_file('output/task_2/data_quality_check/future_ref_check.txt')

    future_data_check = DataQualityChecks(future_price,'future_price')
    future_data_check.run(time_stamp=False,columns=['pre_close','pre_settle','open','high','low','close','settle','vol','amount','oi'])
    future_data_check.write_to_file('output/task_2/data_quality_check/future_price_check.txt')

    analysis = MyAnalysis('data/future_price.csv', 'data/future_ref.csv')
    analysis.run()



if __name__ == '__main__':
    main()

