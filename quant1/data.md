The coding test conducts in 3 parts, please choose the programming you are familiar with to implement below tasks.

# 1. Equity Tick Data

You have been provided one stock’s level-1 trade and quote data (trade.csv, quote.csv). Please use these raw data to generate following derived data

- Data quality check
- Aggregate trade/quote into 5 mins bar, which contains: time (end time of each bin), open, high, low, close, volume, vwap, twap, n_trd (number of trades), n_quo, bid_price (last snapshot at the end of the bin), bid_side, ask_price, ask_size.
- Calculate liquidity flow data within this bin
    - liquidity add: bid/ask size increase
    - liquidity taken: both active buy and active sell (Hint: in which you need to define some rules to categorize the trade direction)
- Performance optimization is encouraged, especially for python code

# 2. Future Generic Series

Futures contracts have a limited lifespan that will influence the outcome of your trades and exit strategy. The two most important expiration terms are expiration and rollover. Please construct a continuous future series according to https://ibkrguides.com/tws/usersguidebook/technicalanalytics/continuous.htm

You have 2 csv files which contain future reference and daily market data, please implement below requirements

- Data quality check
- Build generic index future series with suffix “c1” which points to the futures with nearest-to-expiry. Similarly, c2 means 2nd nearest to expiry. please construct generic series for IF future: IFc1, IFc2, IFc3.
- Build generic commodity future series with suffix “v1” which points to the most active futures, “active” can be either measured by volume or open-interest or composite rules. please construct generic series for P future: Pv1, Pv2, Pv3
    - Note: if one future has been rolled from near to far, it can’t be rolled back again. E.g., Pv1 (P2101) on day1, then rolls to Pv1(P2102) on day2. It’s not allowed to roll back Pv1(P2101) on day 3.
- Apply price adjustment according to ratio defined in reference page
- Please visualize the rolling path and adjusted price

# 3. US Treasury Auction

US Treasury department publish announcement of their upcoming bond issuances, the objective is to build a pipeline for this dataset. Please implement a scraper to scrape the website: https://treasurydirect.gov/auctions/upcoming/  design the schema and implement a point in time database to save all data that could possibly be of interest so it can be retrieved in the future for backtest purposes. Below are some specific requirements:

- Implement the scraper and perform data check.
- Design job scheduler to run scraper in daily basis.
- Databases can be in any format you are familiar with, either in KDB or SQL DB.
- For the schema design, please be specific regarding the key/index to be applied. Submit final schema or sample data in csv formats.
- Please note that the events might be cancelled or auction date/cusip could be up to changes. That information should be captured by the job.
