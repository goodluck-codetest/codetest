""" 3. US Treasury Auction

US Treasury department publish announcement of their upcoming bond issuances,
 the objective is to build a pipeline for this dataset. 
 Please implement a scraper to scrape the website:
  https://treasurydirect.gov/auctions/upcoming/  design the schema and 
 implement a point in time database to save all data that could 
 possibly be of interest so it can be retrieved in the future for backtest purposes. 
 Below are some specific requirements:

- Implement the scraper and perform data check.
- Design job scheduler to run scraper in daily basis.
- Databases can be in any format you are familiar with, either in KDB or SQL DB.
- For the schema design, please be specific regarding the key/index to be applied. 
Submit final schema or sample data in csv formats.
- Please note that the events might be cancelled or auction date/cusip could be up to changes. 
That information should be captured by the job.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import csv
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Scraper:
    def __init__(self, url, driver_path):
        self.url = url
        # Setup the webdriver
        options = Options()
        options.add_argument("--headless")  # Ensure GUI is off
        self.driver = webdriver.Chrome(executable_path=driver_path, options=options)

    def fetch_data(self, table_ids):
        self.driver.get(self.url)
        data = []
        for table_id in table_ids:
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, table_id)))
            data.extend(self.parse_table_data(table_id))
        return data

    def parse_table_data(self, table_id):
        table_data = []
        table = self.driver.find_element_by_id(table_id)
        headers = [th.text for th in table.find_elements_by_tag_name('th')]
        rows = table.find_elements_by_tag_name('tr')
        for row in rows[1:]:  # Skip the header row
            cols = row.find_elements_by_tag_name('td')
            row_data = {}
            for header, col in zip(headers, cols):
                row_data[header] = col.text
            table_data.append(row_data)
        return table_data

class Checker:
    def __init__(self, data):
        self.data = data

    def check_empty_fields(self, row):
        if any(not field for field in row):
            logging.warning("Row contains empty field(s). Skipping...")
            return False
        return True

    def check_offering_amount(self, offering_amount):
        try:
            offering_amount = float(offering_amount)
            if offering_amount < 0:
                raise ValueError("Offering amount cannot be negative.")
        except ValueError as e:
            logging.warning(f"Invalid offering amount '{offering_amount}': {e}. Skipping...")
            return False
        return True

    def check_dates(self, announcement_date, auction_date):
        try:
            announcement_date = datetime.datetime.strptime(announcement_date, '%Y-%m-%d')
            auction_date = datetime.datetime.strptime(auction_date, '%Y-%m-%d')

            if announcement_date > auction_date:
                raise ValueError("Announcement date cannot be after auction date.")
        except ValueError as e:
            logging.warning(f"Invalid date(s) '{announcement_date}'/'{auction_date}': {e}. Skipping...")
            return False
        return True

    def validate_data(self):
        validated_data = []
        for row in self.data:
            if not self.check_empty_fields(row):
                continue
            if not self.check_offering_amount(row[3]):
                continue
            if not self.check_dates(row[4], row[5]):
                continue

            validated_data.append(row)

        return validated_data

class Database:

    def __init__(self, db_name):
        try:
            self.conn = sqlite3.connect(db_name)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")

    def create_table(self):
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Upcoming_Auctions (
                    id INTEGER PRIMARY KEY,
                    bills TEXT,
                    cmb TEXT,
                    cusip TEXT UNIQUE,
                    offering_amount REAL,
                    announcement_date DATE,
                    auction_date DATE,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except sqlite3.Error as e:
            logging.error(f"Table creation error: {e}")

    def insert_data(self, data):
        checker = Checker(data)
        validated_data = checker.validate_data()
        self.cursor.executemany("INSERT INTO Auctions VALUES (?, ?, ?, ?, ?, ?)", validated_data)
        self.conn.commit()

    def export_schema(self):
        table_info = self.cursor.execute("PRAGMA table_info(Upcoming_Auctions)").fetchall()
        with open('schema.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'])
            writer.writerows(table_info)

    def export_to_csv(self):
        with open('output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            self.cursor.execute("SELECT * FROM Auctions")
            rows = self.cursor.fetchall()
            writer.writerows(rows)
#%%
def main():
    logging.basicConfig(level=logging.INFO)

    scraper = Scraper('https://treasurydirect.gov/auctions/upcoming/')
    database = Database('my_database.db')

    database.create_table()

    data = scraper.fetch_data()
    if data is not None:
        database.insert_data(data)

    database.generate_schema()
    database.export_to_csv()

if __name__ == "__main__":
    main()