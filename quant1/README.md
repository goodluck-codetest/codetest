# Project Overview

This project consists of several Python scripts that are used for financial data analysis and processing. The scripts perform various functions such as data quality checks, building data series, and web scraping. Here's a brief introduction to each file:

## 1. equity_tick.py

This script is used for processing level-1 trade and quote data for a particular stock. The raw data comes from two CSV files, `trade.csv` and `quote.csv`. The script performs data quality checks and generates several derived data based on the raw data.

## 2. future_generic_series.py

`future_generic_series.py` constructs a continuous future series from futures contracts data. The script handles expiration and rollover, two crucial aspects of futures contracts. It takes as input two CSV files containing future reference and daily market data. 

## 3. us_treasury_auction.py

The `us_treasury_auction.py` script is designed to scrape the US Treasury Department's website for data on upcoming bond issuances. The script implements a scraper to fetch data, designs the schema, and saves the data in a point-in-time database for future retrieval and backtest purposes.

In addition to scraping, the script includes functionality to schedule the scraper to run on a daily basis, handle changes in auction dates, and cancellations of events. The database can be in any format, either KDB or SQL DB.

The data is stored in the database, with the schema of the database clearly defined. The script also includes functionality to export the schema or sample data in CSV format for inspection.

# Requirements

The scripts require Python 3.8 and several Python libraries, including:

- `requests` and `BeautifulSoup` for web scraping.
- `selenium` for automating web browser interaction.
- `sqlite3` for working with SQLite databases.
- `logging` for generating log files.
- `csv` for reading from and writing to CSV files.
- `datetime` for handling date and time data.
- `webdriver_manager.chrome` for managing the ChromeDriver needed by Selenium.

# Using the Scripts

This document provides instructions on how to run the scripts in this project.

## Prerequisites

Ensure you have Python installed on your system. If not, you can download and install Python from the [official website](https://www.python.org/downloads/).

## Running the Scripts

To run the scripts, you need to navigate to the `quant1/` directory and run the desired script. Here's how you can do this:

1. Open a terminal.

2. Change the current working directory to `quant1/`. You can do this using the `cd` command:

    ```bash
    cd path/to/quant1/
    ```

    Replace `path/to/quant1/` with the actual path to the `quant1/` directory.

3. Run the script. For example:

    - To run the `equity_tick` script, you can use the following command:

        ```bash
        python -m src.task_1.equity_tick
        ```

    - To run the `future_generic_series` script, you can use the following command:

        ```bash
        python -m src.task_2.future_generic_series
        ```

    - To run the `us_treasury_aution` script, you can use the following command:

        ```bash
        python -m src.task_3.us_treasury_aution
        ```

Please note that you might need to install some dependencies before running the scripts. If a `requirements.txt` file is provided in the project, you can install the required dependencies using the following command:

```bash
pip install -r requirements.txt


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