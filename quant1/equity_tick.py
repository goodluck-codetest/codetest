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

# Set up logging
logger = logging.getLogger('DataQualityChecks')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('data_quality_checks.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

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

    def check_missing_values(self) -> None:
        """
        Check for missing values in the DataFrame and log the results.
        """
        result = self.df.isnull().sum()
        self.results['Missing values'] = result
        logger.info(f'Checked missing values in {self.dataset_name}:\n{result}')

    def check_duplicates(self) -> None:
        """
        Check for duplicate rows in the DataFrame and log the results.
        """
        df_with_index = self.df.reset_index()
        result = df_with_index.duplicated().sum()        
        self.results['Duplicate rows'] = result
        logger.info(f'Checked duplicate rows in {self.dataset_name}: {result}')

    def check_data_types(self) -> None:
        """
        Check the data types of the DataFrame's columns and log the results.
        """
        result = self.df.dtypes
        self.results['Data types'] = result
        logger.info(f'Checked data types in {self.dataset_name}:\n{result}')

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
        logger.info(f'Checked outliers in {self.dataset_name}:\n{result}')

    def check_consistency(self, columns: Optional[List[str]]=None) -> None:
        """
        Check for negative values in specified columns of the DataFrame.

        :param columns: list, optional, a list of column names to check
        """
        if columns is None:
            columns = self.df.columns
        result = self.df[columns][self.df[columns] < 0].count()
        self.results['Negative values'] = result
        logger.info(f'Checked negative values in columns {columns} of {self.dataset_name}:\n{result}')

    def check_timestamps(self) -> None:
        """
        Check for non-unique timestamps in the DataFrame's index.
        """
        try:
            result = self.df.index.duplicated().sum()
            self.results['Non-unique timestamps'] = result
            logger.info(f'Checked non-unique timestamps in {self.dataset_name}: {result}')
        except TypeError:
            logger.error("Index is not of type datetime")

    def run(self, outlier_check: Optional[Callable[[pd.DataFrame], pd.DataFrame]]=None, columns: Optional[List[str]]=None) -> None:
        """
        Run all data quality checks and log the results.

        :param outlier_check: function, optional, a function that takes a DataFrame and returns an outlier check result
        :param columns: list, optional, a list of column names to check for negative values
        """
        self.check_missing_values()
        self.check_duplicates()
        self.check_data_types()
        self.check_outliers(outlier_check)
        self.check_consistency(columns)
        self.check_timestamps()

    def write_to_file(self, filename: str) -> None:
        """
        Write the results of the checks to a file.

        :param filename: str, the name of the file to write the results to
        """
        with open(filename, 'w') as f:
            for key, value in self.results.items():
                f.write(f"{key}:\n{value}\n\n")
        logger.info(f'Wrote results of {self.dataset_name} checks to file: {filename}')


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

        file_handler = logging.FileHandler('data_aggregator.log')
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

    @abstractmethod
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

    def run(self):
        super().run()


class OrderDataAggregator(DataAggregator):
    """
    This class aggregates order data.
    """
    def __init__(self, df: pd.DataFrame):
        
        self.agg_funcs = {
            'bid_price': 'last',
            'ask_price': 'last',
            'bid_size': 'last',
            'ask_size': 'last'
        }

        self.dataset_name = 'OrderData'

    def aggregate_data(self, freq='5T'):
        super().aggregate_data(freq)
        
        self.logger.info('Calculating liquidity flow data...')

        # Calculate liquidity added
        bid_size_increase = self.df_agg['bid_size'].diff().clip(lower=0)
        ask_size_increase = self.df_agg['ask_size'].diff().clip(lower=0)
        self.df_agg['liquidity_added'] = bid_size_increase + ask_size_increase

        # Calculate liquidity taken
        active_buy_volume = self.df[self.df['trade_side'] == 'buy']['volume'].resample('5T').sum()
        active_sell_volume = self.df[self.df['trade_side'] == 'sell']['volume'].resample('5T').sum()
        self.df_agg['liquidity_taken'] = active_buy_volume + active_sell_volume

        self.logger.info(f'Done calculating liquidity flow data for {self.dataset_name}.')

    def run(self):
        super().run()

def main():
    # Load your data
    trades = pd.read_csv('trade.csv')
    quotes = pd.read_csv('quote.csv')

    # Convert 'time' to datetime and set as index
    # Assuming the date is today
    trades['time'] = pd.to_datetime(trades['time'])
    trades.set_index('time', inplace=True)
    quotes['time'] = pd.to_datetime(quotes['time'])
    quotes.set_index('time', inplace=True)

    # # Create DataQualityChecks objects
    # trades_check = DataQualityChecks(trades,'trades')
    # quotes_check = DataQualityChecks(quotes,'quotes')

    # # Run all checks
    # trades_check.run()
    # quotes_check.run()

    # # Run all checks
    # trades_check.write_to_file('trade_checks.txt')
    # quotes_check.write_to_file('quotes_check.txt')

    trades_agg_funcs = {
        'price':['first', 'max', 'min', 'last',],
        'volume':'sum',
    }

    quotes_agg_funcs = {
        'bid_price': 'last',
        'ask_price': 'last',
        'bid_size': 'last',
        'ask_size': 'last'
    }

    trades_aggregate = DataAggregator(trades,trades_agg_funcs,'trades')
    # quotes_aggregate = DataAggregator(quotes,quotes_agg_funcs,'quotes')

    trades_aggregate.run()
    # quotes_aggregate.run()

    trades_aggregate.write_to_file('trades_aggregate.csv')
    # quotes_aggregate.write_to_file('quotes_aggregate.csv')
    
if __name__ == '__main__':
    main()