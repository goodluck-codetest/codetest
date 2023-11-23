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
    > _**NOTE:** This script is **not** designed to be run manually. Instead, it is intended to be scheduled to run at specific intervals using a job scheduler such as `crontab`. Please follow the instructions provided below to set up `crontab` and schedule the script to run._

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


# Future Generic Series Construction

This Python project is designed to construct continuous future series for index futures and commodity futures. The code reads in two csv files: future reference data and daily market data. Based on these inputs, it constructs continuous future series for nearest-to-expiry index futures (IFc1, IFc2, IFc3) and for most active commodity futures (Pv1, Pv2, Pv3), where "active" is defined by volume, open interest, or composite rules. The project includes data quality checking, price adjustment according to defined ratios, and visualization of the rolling path and adjusted prices.

## Outputs

The outputs of this program are as follows:

1. **Data Quality Checks:** The data quality check results are saved as text files in the folder 'output/task_2/data_quality_check'. The files 'future_ref_check.txt' and 'future_price_check.txt' contain the results of the data quality checks for the future reference data and the daily market data, respectively.

2. **Continuous Future Series:** The constructed continuous future series for index and commodity futures are saved as CSV files in the 'output/task_2/futures_rolling' directory. The CSV files 'index_rolling_path.csv' and 'commodity_rolling_path.csv' represent the rolling paths for the index and commodity futures, respectively.

3. **Visualizations:** The rolling path and the adjusted prices are visualized in plots, which are saved as PNG files in the 'output/task_2/futures_rolling' directory. The files 'index_rolling_plot.png' and 'commodity_rolling_plot.png' represent the visualizations for the index and commodity futures, respectively.

## Interpreting the Results

The output CSV files contain the trade dates, the futures code, the close and adjusted close prices, the rolling dates, and the adjusted ratios for each index and commodity future.

The PNG files visually display the rolling path and the adjusted prices over time. The x-axis represents the date, and the y-axis represents the price. The 'Original Close Price' and 'Adjusted Close Price' are plotted in different colors and include a legend for reference.

Remember, the adjusted price takes into account the rollover from one future contract to another and adjusts the prices based on the closing price ratio between the two contracts. This adjustment allows you to compare prices over time in a more meaningful way.

Please note that this project assumes that once a futures contract has been rolled from near to far, it cannot be rolled back again.

# US treasury auction

The `us_treasury_auction.py` Python script is developed to scrape data for upcoming U.S. Treasury auctions from the website `https://treasurydirect.gov/auctions/upcoming/`. The script processes and validates the scraped data, stores the processed data into a SQLite database, and exports the table schema and the data to CSV files.


## Running the Script

1. Open a terminal.
2. Type `crontab -e` to open the crontab file.
3. Add a line to schedule your script. The line should follow this format:

    ```
    * * * * *  /usr/bin/python3 /path/to/your/project/directory/src/task_3/us_treasury_auction.py
    ```
    Replace `/path/to/your/project/directory` with the actual path to your project directory.

    For instance, if you want to schedule the `us_treasury_auction.py` script to run every day at 5 PM, your cron job would look like this:

    ```
    0 17 * * * /usr/bin/python3 /path/to/your/project/quant1/src/task_3/us_treasury_auction.py
    ```

4. Save and close the crontab file.

**Note:** Make sure the Python and script paths are correct. You can check your Python path by running `which python3` in the terminal.

## Checking the Log

The script generates a log file named `us_treasury_auction.log` in the `logging` directory. This log file contains important error messages or status updates from the script. Its recommended to check the log file to understand the execution flow and to troubleshoot potential issues.

## Locating the Output

The script stores processed data into a SQLite database and exports two types of CSV files:

1. **SQLite Database**: The script creates a SQLite database to store the processed data. The name and location of this database depend on the script code. 

2. **CSV Files**: The script exports the table schema and the data to CSV files. These files are typically located in a directory specified in the script. Based on the given script snippet, the files should be located in the `output/task_3` directory. There should be two files:
    - `schema.csv`: This file contains the table schema.
    - `sql_data.csv`: This file contains the stored data.

You can open these CSV files with any spreadsheet software (like Microsoft Excel or Google Sheets) or a text editor to view their contents.

## Note

This script uses Selenium with ChromeDriver to scrape data from the TreasuryDirect website. If you encounter issues with ChromeDriver, you may need to update it or adjust the path to match your systems configuration.