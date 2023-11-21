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
#%%
"""
import matplotlib.pyplot as plt

from ..task_1.equity_tick import DataQualityChecks

import pandas as pd
import matplotlib.pyplot as plt

#%%
class FutureData:
    def __init__(self, future_price, future_ref):
        self.future_price = future_price
        self.future_ref = future_ref

    def merge_and_filter(self):
        merged_data = pd.merge(self.future_price, self.future_ref, on='ts_code')
        listed_futures = merged_data[(merged_data['list_date'] <= merged_data['trade_date']) & (merged_data['delist_date'] > merged_data['trade_date'])]
        return listed_futures.sort_values(by=['trade_date',])

    def process_index_futures(self, futures):
        index_futures = futures[futures['fut_code'] == 'IF']
        index_futures['expiry_rank'] = index_futures.groupby('trade_date')['delist_date'].rank(method='min')
        index_futures['generic'] = index_futures['expiry_rank'].map({1: 'IFc1', 2: 'IFc2', 3: 'IFc3'})
        index_futures = index_futures[index_futures['generic'].isin(['IFc1','IFc2','IFc3'])]
        index_futures = self.calculate_rollover_dates(index_futures)
        #index_futures['contract_change'] = index_futures['ts_code'].shift(-1) != index_futures['ts_code']
        #index_futures = self.calculate_rollover_dates(index_futures)
        
        return index_futures
    
    def calculate_rollover_dates(self,futures):
        futures = futures.sort_values(by=['trade_date', 'delist_date'])

        # Initialize variables to keep track of the current ts_code and its volume
        current_ts_code, current_volume = futures.iloc[0]['ts_code'], futures.iloc[0]['vol']

        for i in range(1, len(futures)):
            # If a new ts_code appears that has a later delist_date and higher volume
            current_trade_date = futures.iloc[i]['trade_date']
            current_volume = futures[(futures['trade_date'] == current_trade_date) & futures['ts_code'] == current_ts_code]['vol']

            if futures.iloc[i]['delist_date'] > futures[futures['ts_code'] == current_ts_code]['delist_date'].max() and futures.iloc[i]['vol'] > current_volume:
                futures.at[futures.iloc[i].name, 'rollover'] = 1  # Flag this row as a rollover
                current_ts_code = futures.iloc[i]['ts_code']  # Update the current ts_code
                current_volume = futures.iloc[i]['vol']  # Update the current volume        

        futures['rollover'] = futures['rollover'].fillna(0)

        return futures

    # def calculate_rollover_dates(futures):
    #     futures['volume_shifted'] = futures.groupby('ts_code')['volume'].shift(-1)
    #     rollover_rows = futures[futures['volume'] < futures['volume_shifted']]
    #     rollover_dates = rollover_rows.groupby('ts_code')['trade_date'].max()
    #     return rollover_dates

    def process_commodity_futures(self,futures):
        commodity_futures = futures[futures['fut_code'] == 'P']
        commodity_futures['activity_rank'] = commodity_futures.groupby('trade_date')['vol'].rank(method='min')
        commodity_futures['generic'] = commodity_futures['activity_rank'].map({1: 'Pv1', 2: 'Pv2', 3: 'Pv3'})
        commodity_futures = commodity_futures[commodity_futures['generic'].isin(['Pv1','Pv2','Pv3'])]
        commodity_futures['contract_change'] = commodity_futures['ts_code'].shift(-1) != commodity_futures['ts_code']
        return commodity_futures        

#    def adjust_close(self, futures):
#        ratio = futures['close'] / futures['close'].shift(-1)
#        futures['adj_factor'] = (futures['contract_change'].shift().fillna(0) * ratio).replace(0, 1).cumprod()
#        futures['adj_close'] = futures['adj_factor'] * futures['close']
#        futures['adj_close'] = futures['adj_close'].round(1)
#        return futures.sort_values(['ts_code','trade_date'])

    def adjust_close(self, futures):
        unique_ts_codes = futures['ts_code'].unique()
        
        # Concatenate all adjusted futures dataframes
        adjusted_futures_df = pd.DataFrame()

        first_df = futures[futures['ts_code'] == unique_ts_codes[0]]

        first_df['ratio'] = 1

        first_df['adj_close'] = first_df['close']

        adjusted_futures_df = pd.concat([adjusted_futures_df,first_df])
        
        for i in range(len(unique_ts_codes) - 1):
            contract_a_code = unique_ts_codes[i]
            contract_b_code = unique_ts_codes[i+1]
            
            contract_a = futures[futures['ts_code'] == contract_a_code]
            contract_b = futures[futures['ts_code'] == contract_b_code]
            
            # Find the date when contract B becomes the lead
            change_date = contract_a[contract_a['rollover'] == 1]['trade_date'].values[0]
            
            # Get the closing prices of contract A and B on the change date
            close_price_a = contract_a.loc[contract_a['trade_date'] == change_date, 'close'].values[0]
            close_price_b = contract_b.loc[contract_b['trade_date'] == change_date, 'close'].values[0]
            
            # Calculate the ratio
            ratio = close_price_a / close_price_b

            # Apply the ratio to all closing prices of contract B
            contract_b['adj_close'] = contract_b['close'] * ratio

            contract_b['adj_close'] = contract_b['adj_close'].round(1)

            contract_b['ratio'] = ratio
            
            adjusted_futures_df = pd.concat([adjusted_futures_df, contract_b])
        
        return adjusted_futures_df.sort_values(['trade_date',])


    def export_data(self, futures, filename):
        futures.to_csv(filename, index=False)

    def plot(self, futures, export_png_name=None):
        plt.figure(figsize=(20, 10))
        plt.plot(futures['trade_date'], futures['close'], label='Original Close Price')
        plt.plot(futures['trade_date'], futures['adj_close'], label='Adjusted Close Price')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.grid()
        plt.title('Original and Adjusted Close Prices Over Time')
        if export_png_name:
            plt.savefig(export_png_name,dpi=300)

        plt.show()


class MyAnalysis:
    def __init__(self, future_price_file, future_ref_file):
        self.data = FutureData(future_price_file, future_ref_file)

    def run(self):
        futures = self.data.merge_and_filter()
        index_futures = self.data.process_index_futures(futures)
        index_futures = self.data.adjust_close(index_futures)
        rolling_paths = index_futures[index_futures['rollover'] == 1]
        self.data.export_data(rolling_paths, 'output/task_2/futures_rolling/rolling_path.csv')
        self.data.plot(rolling_paths, 'output/task_2/futures_rolling/rolling_plot.png')        

        #commodity_futures = self.data.process_commodity_futures(futures)
        #commodity_futures = self.data.adjust_close(commodity_futures)
        #self.data.export_data(commodity_futures, 'output/task_2/futures_rolling/commodity_future.csv')
        #self.data.plot(commodity_futures, 'output/task_2/futures_rolling/commodity_future_plot.png')

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

    analysis = MyAnalysis(future_price, future_ref)
    analysis.run()

if __name__ == '__main__':
    main()

