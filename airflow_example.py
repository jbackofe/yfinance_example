import yfinance as yf
import pandas as pd
# import boto3
import awswrangler as wr
import datetime

def pullUploadFinanceData():
    tickers = yf.Tickers('MSFT META')

    histories = []
    for symb in tickers.tickers:
        daily = tickers.tickers[symb].history(period="1d")
        daily['Ticker'] = symb
        daily = daily.reset_index()
        daily['Date'] = daily['Date'].dt.to_pydatetime()[0].strftime("%Y-%m-%d")
        daily = daily[['Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Date']]
        histories.append(daily)
        
    data = pd.concat(histories)
    print(data)
    todays_date = datetime.datetime.today().strftime("%Y-%m-%d")

    # Storing data on Data Lake
    # wr.s3.to_parquet(
    #     df=data.copy(),
    #     path="s3://yfinance-test/daily/",
    #     dataset=True,
    #     database="yfinance",
    #     table="history_daily",
    #     filename_prefix=todays_date
    # )