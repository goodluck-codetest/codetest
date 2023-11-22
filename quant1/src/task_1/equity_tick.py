"""
1. Equity Tick Data
You have been provided one stock’s level-1 trade and quote data (trade.csv, quote.csv). Please use these raw data to generate following derived data

- Data quality check
- Aggregate trade/quote into 5 mins bar, which contains: time (end time of each bin), open, high, low, close, volume, vwap, twap, n_trd (number of trades), n_quo, bid_price (last snapshot at the end of the bin), bid_side, ask_price, ask_size.
- Calculate liquidity flow data within this bin
    - liquidity add: bid/ask size increase
    - liquidity taken: both active buy and active sell (Hint: in which you need to define some rules to categorize the trade direction)
- Performance optimization is encouraged, especially for python code
"""
from abc import ABC, abstractmethod
from typing import Callable, List, Optional
import pandas as pd
import logging


#%%
class DataQualityChecks:
    """
    This class provides methods for checking the quality of a Pandas DataFrame.
    """

    def __init__(self, df: pd.DataFrame, dataset_name: str):
        """
        Initialize the DataQualityChecks with a DataFrame to check.

        :param df: DataFrame, the data to check
        :param dataset_name: str, the name of the dataset being checked
        """
        self.df = df
        self.dataset_name = dataset_name
        self.results = {}

        # Set up logging
        self.logger = logging.getLogger('DataQualityChecks')
        self.logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler('logging/data_quality_checks.log')
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

    def set_time_index(self) -> None:
        self.df['time'] = pd.to_datetime(self.df['time'])
        self.df.set_index('time', inplace=True)  

    def check_missing_values(self) -> None:
        """
        Check for missing values in the DataFrame and log the results.
        """
        result = self.df.isnull().sum()
        self.results['Missing values'] = result
        self.logger.info(f'Checked missing values in {self.dataset_name}:\n{result}')

    def check_duplicates(self) -> None:
        """
        Check for duplicate rows in the DataFrame and log the results.
        """
        df_with_index = self.df.reset_index()
        result = df_with_index.duplicated().sum()        
        self.results['Duplicate rows'] = result
        self.logger.info(f'Checked duplicate rows in {self.dataset_name}: {result}')

    def check_data_types(self) -> None:
        """
        Check the data types of the DataFrame's columns and log the results.
        """
        result = self.df.dtypes
        self.results['Data types'] = result
        self.logger.info(f'Checked data types in {self.dataset_name}:\n{result}')

    def check_outliers(self, outlier_check: Optional[Callable[[pd.DataFrame], pd.DataFrame]]=None) -> None:
        """
        Check for outliers in the DataFrame using a user-provided function, 
        or by computing summary statistics if no function is provided.

        :param outlier_check: function, optional, a function that takes a DataFrame and returns an outlier check result
        """
        if outlier_check is None:
            result = self.df.describe()
        else:
            result = outlier_check(self.df)
        self.results['Outliers'] = result
        self.logger.info(f'Checked outliers in {self.dataset_name}:\n{result}')

    def check_consistency(self, columns: Optional[List[str]]=None) -> None:
        """
        Check for negative values in specified columns of the DataFrame.

        :param columns: list, optional, a list of column names to check
        """
        if columns is None:
            columns = self.df.columns
        result = self.df[columns][self.df[columns] < 0].count()
        self.results['Negative values'] = result
        self.logger.info(f'Checked negative values in columns {columns} of {self.dataset_name}:\n{result}')

    def check_timestamps(self) -> None:
        """
        Check for non-unique timestamps in the DataFrame's index.
        """
        try:
            result = self.df.index.duplicated().sum()
            self.results['Non-unique timestamps'] = result
            self.logger.info(f'Checked non-unique timestamps in {self.dataset_name}: {result}')
        except TypeError:
            self.logger.error("Index is not of type datetime")

    def run(self, time_stamp: bool , outlier_check: Optional[Callable[[pd.DataFrame], pd.DataFrame]]=None, columns: Optional[List[str]]=None) -> None:
        """
        Run all data quality checks and log the results.

        :param outlier_check: function, optional, a function that takes a DataFrame and returns an outlier check result
        :param columns: list, optional, a list of column names to check for negative values
        """
        if time_stamp:
            self.set_time_index()
            self.check_timestamps()

        self.check_missing_values()
        self.check_duplicates()
        self.check_data_types()
        self.check_outliers(outlier_check)
        self.check_consistency(columns)
        
    def run_ref_data(self, outlier_check: Optional[Callable[[pd.DataFrame], pd.DataFrame]]=None, columns: Optional[List[str]]=None) -> None:
        """
        Run all data quality checks and log the results specifically for ref dataset

        :param outlier_check: function, optional, a function that takes a DataFrame and returns an outlier check result
        :param columns: list, optional, a list of column names to check for negative values
        """
        self.check_missing_values()
        self.check_duplicates()
        self.check_data_types()

    def write_to_file(self, filename: str) -> None:
        """
        Write the results of the checks to a file.

        :param filename: str, the name of the file to write the results to
        """
        with open(filename, 'w') as f:
            for key, value in self.results.items():
                f.write(f"{key}:\n{value}\n\n")
        self.logger.info(f'Wrote results of {self.dataset_name} checks to file: {filename}')


class DataAggregator(ABC):
    """
    This abstract base class provides common methods for aggregating data.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.cols = df.columns
        self.renaming = {
            'first':'open',
            'max':'high',
            'min':'low',
            'last':'close',
        }

        # Set up logging
        self.logger = logging.getLogger('DataAggregator')
        self.logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler('logging/data_aggregator.log')
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

    @abstractmethod
    def aggregate_data(self, freq='5T'):
        """
        Aggregate the data.
        """
        self.df_resampled = self.df.resample(freq)

        self.df_agg = pd.DataFrame()

        for col, func in self.agg_funcs.items():
            if isinstance(func,list):
                for func_item in func:
                    self.df_agg[self.renaming.get(func_item,func_item)] = self.df_resampled[col].agg(func_item)
            else:
                self.df_agg[col] = self.df_resampled[col].agg(func)

    def set_other_aggregator(self, other_aggregator):
        self.other_aggregator = other_aggregator                

    def run(self):
        """
        Run the aggregation process.
        """
        self.aggregate_data()

    def write_to_file(self, filename: str) -> None:
        """
        Write the data to a CSV file.

        :param filename: str, the name of the file to write to
        """
        self.logger.info(f'Writing results to {filename}...')
        self.df_agg.to_csv(filename)
        self.logger.info('Done writing results to file.')

class TradeDataAggregator(DataAggregator):
    """
    This class aggregates trade data.
    """
    def __init__(self, df: pd.DataFrame):
        super().__init__(df)

        self.agg_funcs = {
            'price': ['first', 'max', 'min', 'last'],
            'volume': 'sum'
        }

        self.dataset_name = 'TradeData'

    def aggregate_data(self, freq='5T'):
        super().aggregate_data(freq)

        # Calculate vwap
        self.df_agg['vwap'] = (self.df['price'] * self.df['volume']).resample(freq).sum() / self.df_resampled['volume'].sum()
        self.df_agg['vwap'] = self.df_agg['vwap'].round(2)

        # Calculate twap
        self.df_agg['twap'] = self.df_resampled['price'].mean()
        self.df_agg['twap'] = self.df_agg['twap'].round(2)


    def calculate_active_trades(self):
        # Ensure we have a reference to the other aggregator
        if not hasattr(self, 'other_aggregator'): 
            raise Exception('Other aggregator not set')

        quote_df_agg = self.other_aggregator.df_agg

        # Merge dataframes on the index
        merged_df = self.df_agg.merge(quote_df_agg, left_index=True, right_index=True, how='outer')

        # Create active_buy_vol and active_sell_vol columns using vectorized operations
        #If a market buy order (an order to buy at whatever price is available) is placed when there is not enough volume 
        #at the current ask price, the order will start filling at higher prices. 
        #This could potentially push the closing price of that 5-minute bin higher than the recorded ask price.
        merged_df['active_buy_vol'] = merged_df.loc[merged_df['close'] >= merged_df['ask_price'], 'volume']
        merged_df['active_sell_vol'] = merged_df.loc[merged_df['close'] <= merged_df['bid_price'], 'volume']

        # Replace NaN values with 0 (since NaN means there was no active buy/sell)
        merged_df.fillna({'active_buy_vol': 0, 'active_sell_vol': 0}, inplace=True)

        # Add new columns to original dataframe
        self.df_agg = self.df_agg.merge(merged_df[['active_buy_vol', 'active_sell_vol']], left_index=True, right_index=True, how='left')

    def run(self):
        self.aggregate_data()
        self.calculate_active_trades()

class QuoteDataAggregator(DataAggregator):
    """
    This class aggregates quote data.
    """
    def __init__(self, df: pd.DataFrame):

        super().__init__(df)
        
        self.agg_funcs = {
            'bid_price': 'last',
            'ask_price': 'last',
            'bid_size': 'last',
            'ask_size': 'last'
        }

        self.dataset_name = 'QuoteData'

    def aggregate_data(self, freq='5T'):
        super().aggregate_data(freq)
        
        self.logger.info('Calculating liquidity flow data...')

        # Calculate liquidity added
        bid_size_increase = self.df_agg['bid_size'].diff().clip(lower=0)
        ask_size_increase = self.df_agg['ask_size'].diff().clip(lower=0)
        self.df_agg['liquidity_added'] = bid_size_increase + ask_size_increase

#%%
def main():
    # Load your data
    trades = pd.read_csv('data/trade.csv')
    quotes = pd.read_csv('data/quote.csv')

    # Create DataQualityChecks objects
    trades_check = DataQualityChecks(trades,'trades')
    quotes_check = DataQualityChecks(quotes,'quotes')

    # Run all checks
    trades_check.run()
    quotes_check.run()

    # Run all checks
    trades_check.write_to_file('output/task_1/data_quality_check/trade_checks.txt')
    quotes_check.write_to_file('output/task_1/data_quality_check/quotes_check.txt')

    quotes_aggregator = QuoteDataAggregator(quotes)
    quotes_aggregator.run()

    # Make aggregators aware of each other
    trade_aggregator = TradeDataAggregator(trades)
    trade_aggregator.set_other_aggregator(quotes_aggregator)
    trade_aggregator.run()

    trade_aggregator.write_to_file('output/task_1/aggregation/trades_aggregate.csv')
    quotes_aggregator.write_to_file('output/task_1/aggregation/quotes_aggregate.csv')
    
    
if __name__ == '__main__':
    main()

