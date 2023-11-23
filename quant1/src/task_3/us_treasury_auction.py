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
#%%
import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import csv
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager

#%%
# Configure logging
logging.basicConfig(filename='logging/us_treasury_auction.log', 
                    filemode='w', format='%(name)s - %(levelname)s - %(message)s', 
                    level=logging.INFO)

class Scraper:

    table_ids = ['institTableBillsUpcoming',
                        'institTableNotesUpcoming',
                        'institTableBondsUpcoming',
                        'institTableTIPSUpcoming',
                        'institTableFRNUpcoming',
                        ]

    def __init__(self, url, driver_path):
        self.url = url
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), 
                                       options=chrome_options)

    def fetch_data(self):
        logging.info(f"Start fetching data from {self.url}")
        self.driver.get(self.url)

        data = []
        for table_id in Scraper.table_ids:
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, table_id)))
            data.extend(self.parse_table_data(table_id))
        self.driver.quit()
        logging.info(f"Finished fetching data from {self.url}")
        return data

    def parse_table_data(self, table_id):
        logging.info(f"Start parsing table {table_id}")
        table_data = []
        #table = self.driver.find_element_by_id(table_id)
        table = self.driver.find_element(by='id',value=table_id)
        headers = [th.text for th in table.find_elements(By.TAG_NAME,'th')]
        rows = table.find_elements(By.TAG_NAME,'tr')
        for row in rows[1:]:  # Skip the header row
            cols = row.find_elements(By.TAG_NAME,'td')
            row_data = {}
            for header, col in zip(headers, cols):
                row_data[header] = col.text
            table_data.append(row_data)
        logging.info(f"Finished parsing table {table_id}")
        return table_data
    
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
                CREATE TABLE IF NOT EXISTS upcoming_auctions (
                    id INTEGER PRIMARY KEY,
                    bond_type TEXT,
                    bond_duration TEXT,
                    cmb INTEGER,
                    reopening INTEGER,
                    cusip TEXT UNIQUE,
                    offering_amount INTEGER,
                    announcement_date DATE,
                    auction_date DATE,
                    issue_date DATE,
                    cancelled INTEGER DEFAULT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except sqlite3.Error as e:
            logging.error(f"Table creation error: {e}")

    def drop_table(self):
        with self.conn:
            self.cursor.execute("""
                DROP TABLE IF EXISTS upcoming_auctions
            """)
        self.conn.commit()

    def cancel_event(self, cusip):
        with self.conn:
            self.cursor.execute("""
                UPDATE upcoming_auctions SET
                    cancelled = 1
                WHERE cusip = ?
            """, (cusip,))
        self.conn.commit()        

    def insert_data(self, data):
        with self.conn:
            self.cursor.executemany("""
                INSERT OR IGNORE INTO upcoming_auctions (
                    bond_type, 
                    bond_duration, 
                    cmb, 
                    reopening, 
                    cusip, 
                    offering_amount, 
                    announcement_date, 
                    auction_date, 
                    issue_date
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            self.conn.commit()
            logging.info("Inserted data: %s", data)

    def update_cusip(self, current_cusip, new_cusip):
        with self.conn:
            self.cursor.execute("""
                UPDATE upcoming_auctions SET
                    cusip = ?
                WHERE cusip = ?
            """, (new_cusip, current_cusip))
        self.conn.commit()
        logging.info("Updated cusip from %s to %s", current_cusip, new_cusip)

    def update_auction_date(self, cusip, new_auction_date):
        with self.conn:
            self.cursor.execute("""
                UPDATE upcoming_auctions SET
                    auction_date = ?
                WHERE cusip = ?
            """, (new_auction_date, cusip))
        self.conn.commit()
        logging.info("Updated auction date for cusip %s to %s", cusip, new_auction_date)

    def export_schema(self):
        table_info = self.cursor.execute("PRAGMA table_info(upcoming_auctions)").fetchall()
        with open('output/task_3/schema.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'])
            writer.writerows(table_info)

    def export_to_csv(self):
        with open('output/task_3/sql_data.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            self.cursor.execute("SELECT * FROM upcoming_auctions")
            rows = self.cursor.fetchall()
            writer.writerows(rows)

class Processor:
    def __init__(self, data):
        self.data = data

    def validate_required_fields(self, record):
        required_fields = ['CUSIP', 'Announcement Date', 'Auction Date', 'Issue Date']

        for field in required_fields:
            if field not in record:
                logging.error(f"Missing required field {field} in record {record}")
                return False
        return True

    def validate_bond_type(self, record):
        if not any(key in record for key in ['Bills', 'Notes', 'Bonds', 'TIPS', 'FRNs']):
            logging.error(f"Missing bond type in record {record}")
            return False
        return True

    def validate_data(self, record):
        if not self.validate_required_fields(record):
            return False
        if not self.validate_bond_type(record):
            return False
        return True
    
    def convert_offering_amount(self, offering_amount):
        if offering_amount:
            amount, unit = offering_amount.split(' ')
            amount = float(amount)
            scales = {'Billion': 1e9, 'Million': 1e6, 'Thousand': 1e3}
            if unit in scales:
                return int(amount * scales[unit])
        return None
    
    def convert_date_format(self, date_str):
        date_obj = datetime.datetime.strptime(date_str, '%m/%d/%Y')
        return date_obj.strftime('%Y%m%d')    
    
    def convert_data(self, record):
        bond_type = next((k for k in ['Bills', 'Notes', 'Bonds', 'TIPS', 'FRNs'] if k in record), None)
        bond_duration = record.get(bond_type)
        cmb = 1 if record.get('CMB') == 'Yes' else 0
        reopening = 1 if record.get('Reopening') == 'Yes' else 0
        cusip = record.get('CUSIP')
        offering_amount = self.convert_offering_amount(record.get('Offering Amount'))
        announcement_date = self.convert_date_format(record.get('Announcement Date'))
        auction_date = self.convert_date_format(record.get('Auction Date'))
        issue_date = self.convert_date_format(record.get('Issue Date'))

        return (bond_type, bond_duration, cmb, reopening, cusip, offering_amount, announcement_date, auction_date, issue_date)

    def process_data(self):
        processed_data = []

        for record in self.data:
            if self.validate_data(record):
                try:
                    converted_record = self.convert_data(record)
                    processed_data.append(converted_record)
                except Exception as e:
                    logging.error(f"Error converting record {record}: {e}")

        return processed_data

#%%
def main():
    logging.basicConfig(level=logging.INFO)

    scraper = Scraper('https://treasurydirect.gov/auctions/upcoming/','driver/chromedriver.exe')
    data = scraper.fetch_data()

    processor = Processor(data)
    processed_data = processor.process_data()

    if processed_data:

        db = Database('db/USAuction.db')
        db.create_table()
        db.insert_data(processed_data)
        db.export_schema()
        db.export_to_csv()

    else:
        logging.warning("No data was processed.")
        
    
if __name__ == "__main__":
    main()
# %%
