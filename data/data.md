1. US Treasury Auction

US Treasury department publish announcement of their upcoming bond issuances, the objective is to build a pipeline for this dataset. Please implement a scraper to scrape the website:
https://treasurydirect.gov/auctions/upcoming/
And design the schema and implement a point in time database to save all data that could possibly be of interest so it can be retrieved in the future for backtest purposes. Below are some specific requirements:
- Implement the scraper and perform data check.
- Design job scheduler to run scraper in daily basis.
- Databases can be in any format you are familiar with, preferably in kdb or just a binary file that can be loaded in python. 
- For the schema design, please be specific regarding the key/index to be applied. Submit final schema or sample data in csv formats.
- Please note that the events might be cancelled or auction date/cusip could be up to changes. That information should be captured by the job.

2. Future Series Construction

Futures contracts have a limited lifespan that will influence the outcome of your trades and exit strategy. The two most important expiration terms are expiration and rollover. Please construct a continous future series according to below defintion, refer to:
https://www.cmegroup.com/education/courses/introduction-to-futures/understanding-futures-expiration-contract-roll.html
https://ibkrguides.com/tws/usersguidebook/technicalanalytics/continuous.htm

You have 2 csv files which contain future reference and daily market data, please implement below requirements
- Data sanity check
- Constructure two series with suffixes "1" and "A", e.g. IF1.CFX and IFA.CFX
  - "1": the contract nearest to expiry
  - "A": the contract which most heavily trade or largest open interest 
- Apply price adjustment according to ratio defined in IB document
