1. Database Design

US Treasury department publish announcement of their upcoming bond issuances, the objective is to build a pipeline for this dataset. Please implement a scraper to scrape the website:
https://treasurydirect.gov/auctions/upcoming/
And design the schema and implement a point in time database to save all data that could possibly be of interest so it can be retrieved in the future for backtest purposes. Below are some specific requirements:
- The scraper is supposed to run daily.
- Please describe without implementation of the data validation for the scraper.
- Databases can be in any format you are familiar with, preferably in kdb or just a binary file that can be loaded in python. 
- For the schema design, please be specific regarding the key/index to be applied.
- Please note that the events might be cancelled or auction date/cusip could be up to changes. That information should be captured by the job.
