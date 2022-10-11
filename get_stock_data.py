import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *

class GetStockData:
    username = "mth989722"
    password = "SystTrade2022"

    def __init__(self, **kwargs) -> None:
        self.tickers = kwargs["tickers"]
        self.startdate = kwargs["startdate"]
        self.enddate = kwargs["enddate"]

    @staticmethod
    def _get_stock_data(conn, ticker, startdate, enddate):
        """
        conn = wrds.Connection(wrds_username=self.username)
        startdate: string format 01/01/2021

        adjusted price = prc/CFACPR
        """
        data_raw = conn.raw_sql(f"""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd, b.ticker,
                      a.ret, a.retx, a.shrout, a.prc, a.cfacshr, a.openprc, a.vol, a.askhi, a.bidlo
                      from crsp.dsf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.ticker = '{ticker}'
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '{startdate}' and '{enddate}'
                      and b.exchcd between 1 and 3
                      """, date_cols=['date']) 

        if data_raw.shape[0] == 0:
            print(f"Empty data, ticker: {ticker}")
            return None

        #adjust prices
        data_raw['openprc'] = data_raw.eval('openprc/cfacshr') 
        data_raw['prc'] = data_raw.eval('prc/cfacshr') 
        data_raw['askhi'] = data_raw.eval('askhi/cfacshr') 
        data_raw['bidlo'] = data_raw.eval('bidlo/cfacshr') 

        data = data_raw[['date', 'ticker', 'openprc', 'askhi', 'bidlo', 'prc', 'vol', 'cfacshr']]
        data = data.rename(columns = {
            "openprc": "open", 
            "askhi": "high",
            "bidlo": "low",
            "prc": "close",
            'vol': "volume",
            'cfacshr': "adj_factor"
        })
        return data.set_index("date")

    def run(self):
        conn = wrds.Connection(wrds_username=self.username)

        res = {}
        for ticker in self.tickers:
            res[ticker] = self._get_stock_data(conn=conn, ticker=ticker, startdate=self.startdate, enddate=self.enddate)

        return res


import pandas as pd
import numpy as np
import datetime as dt
import wrds
import os
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from scipy import stats

username = "mth989722"
password = "SystTrade2022"
conn = wrds.Connection(wrds_username='mth989722')

import bs4 as bs
import requests
import yfinance as yf
import datetime
from sec_edgar_downloader import Downloader
resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'class': 'wikitable sortable'})

sp500_tickers = []

for row in table.findAll('tr')[1:]:
    ticker = row.findAll('td')[0].text
    sp500_tickers.append(ticker)
sp500_tickers = [s.replace("\n", "") for s in sp500_tickers]

import time
from tqdm import tqdm
from multiprocessing import Pool

if not os.path.exists("./stock_data"):
    os.mkdir("./stock_data")

def download_edgar_data(ticker):
    try:
        dl = Downloader("./")
        
        if not os.path.exists(f'./stock_data/{ticker}.csv'):
            print('-----Start downloading ', ticker)
            data = GetStockData._get_stock_data(conn=conn, ticker=ticker, startdate='01/01/2018', enddate='12/31/2021')
            if not data is None:
                dl.get("4", ticker, after="2018-01-01", before="2021-12-31")
                data.to_csv(f"./stock_data/{ticker}.csv")
                time.sleep(15)
            print('-----End downloading ', ticker)
        return None
    except Exception as e: 
        print("Error ticker: ", ticker, ", error: ", e)
        time.sleep(15)
        return None

if __name__ == "__main__":

    # download_edgar_data(sp500_tickers[-1])
    with Pool(3) as p:
        with tqdm(total=len(sp500_tickers)) as pbar:
            for _ in p.imap_unordered(download_edgar_data, sp500_tickers):
                pbar.update()