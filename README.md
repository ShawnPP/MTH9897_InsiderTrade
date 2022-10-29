# MTH9897_InsiderTrade

## 1. get_stock_data.ipynb
Download stock data from WRDS and EDGAR data from sec_edgar_downloader for SP500 constituent stocks.

## 2. get_edgar_data.ipynb
Turn sec filing data to numerical dataframe.

## 3. backtrader_backtesting.ipynb
Backtest your own strategy by backtrader. Feed all csv files in bt_stock_data_dir into backtester.  
You can just run this scripy if you don't want to start from downloadig data. You can download bt_stock_data_*, and change bt_stock_data_dir in backtrader_backtesting.ipynb as your data path. Then you just need to start backtrader.
  
Data link:  
**sec-edgar-filings**: https://drive.google.com/file/d/1NUy_CIG5R-F6iK6vrtuwiSXN07K-vwW1/view?usp=share_link  
**stock_data**: https://drive.google.com/file/d/1B1az42hh-Tw-mzb9iHSmtjL5DTA3eBKz/view?usp=share_link  
**bt_stock_data**: https://drive.google.com/file/d/1Ep51l3F48yFL_299icjnY32lc2BPKvMm/view?usp=share_link  
**bt_stock_data_forward**(merge edgar data and stock data by transaction date instead of report date): https://drive.google.com/file/d/1k2b5D-GG9TXUtGegUxAKwd-kwHkud_tI/view?usp=share_link  
**bt_stock_data_industry_neutral**: https://drive.google.com/file/d/1WCM_myc0EhbVKWaTxYrYKBnRcmUP44Ci/view?usp=share_link  

## 4. Presentation Slides
**Link**: https://docs.google.com/presentation/d/1U1pwXKoHRNWVRzSCHkFlm1Dwz1zGO44SqZHk3PBLoX0/edit?usp=sharing
