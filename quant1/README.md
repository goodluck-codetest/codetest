# Equity Tick Data Processing Report

This report summarizes the results of processing level-1 trade and quote data for a stock.

## Data Quality Checks

The data quality checks were performed on both the trade and quote data. The checks included looking for missing values, duplicate rows, incorrect data types, outliers, negative values, and non-unique timestamps. The detailed reports can be found in the `output/task_1/data_quality_check` directory.

- [Trade Data Quality Check Report](output/task_1/data_quality_check/trade_checks.txt)
- [Quote Data Quality Check Report](output/task_1/data_quality_check/quotes_check.txt)

## Aggregated Data

The trade and quote data was aggregated into 5-minute bars. The aggregated data includes time, open, high, low, close, volume, vwap, twap, n_trd, n_quo, bid_price, bid_size, ask_price, ask_size, liquidity_added, active_buy_vol, and active_sell_vol. The aggregated data can be found in the `output/task_1/aggregation` directory.

- [Aggregated Trade Data](output/task_1/aggregation/trades_aggregate.csv)
- [Aggregated Quote Data](output/task_1/aggregation/quotes_aggregate.csv)

## Logging

The process and results were logged using Python's logging module. The logs can be found in the `logging` directory.

- [Data Quality Checks Log](logging/data_quality_checks.log)
- [Data Aggregator Log](logging/data_aggregator.log)

## Performance Optimization

The script was designed to handle large datasets efficiently by using Pandas' vectorized operations. This allows for better memory management and faster computation.

## Conclusion

This processing yielded valuable insights into the trade and quote data. The aggregated data can be used for further analysis or for training machine learning models.