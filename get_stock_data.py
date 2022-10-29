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
                      a.ret, a.retx, a.shrout, a.prc, a.cfacpr, a.openprc, a.vol, a.askhi, a.bidlo
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
        data_raw['openprc'] = data_raw.eval('openprc/cfacpr') 
        data_raw['prc'] = data_raw.eval('prc/cfacpr') 
        data_raw['askhi'] = data_raw.eval('askhi/cfacpr') 
        data_raw['bidlo'] = data_raw.eval('bidlo/cfacpr') 

        data = data_raw[['date', 'ticker', 'openprc', 'askhi', 'bidlo', 'prc', 'vol', 'cfacpr']]
        data = data.rename(columns = {
            "openprc": "open", 
            "askhi": "high",
            "bidlo": "low",
            "prc": "close",
            'vol': "volume",
            'cfacpr': "adj_factor"
        })
        return data.set_index("date")

    def run(self):
        conn = wrds.Connection(wrds_username=self.username)

        res = {}
        for ticker in self.tickers:
            res[ticker] = self._get_stock_data(conn=conn, ticker=ticker, startdate=self.startdate, enddate=self.enddate)

        return res
