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
        
        return index_futures
    
    def process_commodity_futures(self, futures):
        commodity_futures = futures[futures['fut_code'] == 'P']
        commodity_futures['active_rank'] = commodity_futures.groupby('trade_date')['oi'].rank(method='max')
        commodity_futures['generic'] = commodity_futures['active_rank'].map({1: 'Pv1', 2: 'Pv2', 3: 'Pv3'})
        commodity_futures = commodity_futures[commodity_futures['generic'].isin(['Pv1','Pv2','Pv3'])]
        commodity_futures = self.calculate_rollover_dates(commodity_futures)
        
        return commodity_futures

    
    def calculate_rollover_dates(self,df):
        df = df.sort_values(by=['trade_date', 'delist_date'])

        df = df.reset_index(drop=True)

        # Initialize 'rollover' column with 0
        df['rollover'] = 0

        # Initialize current contract
        current_contract = df['ts_code'].iloc[0]

        # Iterate over the DataFrame
        for i in range(1, len(df)):
            # Get all contracts on the current trade date
            current_date_contracts = df[df['trade_date'] == df['trade_date'].iloc[i]]
            
            # Check if the current contract is still being traded
            if current_contract not in current_date_contracts['ts_code'].values:
                # If not, roll over to the next contract with the highest volume
                next_contract = current_date_contracts.sort_values('vol', ascending=False)['ts_code'].values[0]
                df.loc[(df['ts_code'] == next_contract) & (df['trade_date'] == df['trade_date'].iloc[i]), 'rollover'] = 1
                current_contract = next_contract
            else:
                # If yes, get the volume and delist_date of the current contract
                current_contract_vol = current_date_contracts[current_date_contracts['ts_code'] == current_contract]['vol'].values[0]
                current_contract_delist_date = current_date_contracts[current_date_contracts['ts_code'] == current_contract]['delist_date'].values[0]
            
                # Check if there's a new contract with higher volume and farther delist date
                rollover_contracts = current_date_contracts[(current_date_contracts['vol'] > current_contract_vol) & (current_date_contracts['delist_date'] > current_contract_delist_date)]
            
                if not rollover_contracts.empty:
                    # Mark rollover
                    df.loc[(df['ts_code'] == rollover_contracts.sort_values('vol', ascending=False)['ts_code'].values[0]) & (df['trade_date'] == df['trade_date'].iloc[i]), 'rollover'] = 1
                    # Update current contract to the one with the highest volume
                    current_contract = rollover_contracts.sort_values('vol', ascending=False)['ts_code'].values[0]

        return df

    def adjust_close(self, df):
        # Initialize column for adjusted prices
        df['adj_price'] = df['close']
        df['adj_ratio'] = 1.0  # initialize with 1's as this will not affect prices

        # Variable for storing the closing price of the last contract
        last_contract = None
        # Dictionary for storing the closing prices of the contracts
        closing_prices = {}

        # Iterate over rows to calculate adjusted prices
        for i in range(len(df)):
            # Store the closing price of the current contract
            current_contract = df.loc[i, 'ts_code']
            closing_prices[current_contract] = df.loc[i, 'close']

            # Check if there is a rollover
            if df.loc[i, 'rollover'] == 1 and last_contract is not None:
                # The previous contract should be the one that is not the current contract
                previous_contract = last_contract
                previous_price = closing_prices[previous_contract]

                # Calculate adjustment ratio
                ratio = df.loc[i, 'close'] / previous_price

                # Adjust price for all dates of the old contract
                df.loc[df['ts_code'] == previous_contract, 'adj_price'] *= ratio

                # Store adjustment ratio
                df.loc[df['ts_code'] == previous_contract, 'adj_ratio'] = ratio
            
            last_contract = current_contract

        df['adj_price'] = df['adj_price'].round(1)

        df['adj_ratio'] = df['adj_ratio'].round(1)

        return df
    
    def output_rolling_result(self, df, file_name, graph_name):
        rolling = df[df['rollover'] == 1]
        self.export_data(rolling, file_name)
        self.plot(rolling, graph_name)      

    def export_data(self, futures, filename):
        futures.to_csv(filename, index=False)

    def plot(self, futures, export_png_name=None):

        futures['trade_date'] = pd.to_datetime(futures['trade_date'], format='%Y%m%d')

        plt.figure(figsize=(20, 10))
        plt.plot(futures['trade_date'], futures['close'], label='Original Close Price')
        plt.plot(futures['trade_date'], futures['adj_price'], label='Adjusted Close Price')
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
        self.futures = futures

        index_futures = self.data.process_index_futures(futures)
        index_futures = self.data.adjust_close(index_futures)
        self.index_futures = index_futures
        self.data.output_rolling_result(index_futures,'output/task_2/futures_rolling/index_rolling_path.csv','output/task_2/futures_rolling/index_rolling_plot.png')
  
        commodity_futures = self.data.process_commodity_futures(futures)
        commodity_futures = self.data.adjust_close(commodity_futures)
        self.commodity_futures = commodity_futures
        self.data.output_rolling_result(commodity_futures,'output/task_2/futures_rolling/commodity_rolling_path.csv','output/task_2/futures_rolling/commodity_rolling_plot.png')

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

