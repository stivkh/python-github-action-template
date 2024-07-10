import logging
import logging.handlers
import os
import datetime as dt

import pandas as pd
import pandas_market_calendars as mcal
import streamlit as st
import numpy as np
import yaml
import yfinance as yf
from sqlalchemy import create_engine, text

from db_connector import db_connector
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    #logger.info("Token not available!")
    #raise


nyse = mcal.get_calendar('NYSE')

engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' % (
   os.environ["username"],os.environ["password"],os.environ["host"], 3306,os.environ["database"]))

date_refreshed = pd.read_sql("select max(Date) from constat where `Adj Close` != -1 and asset_class != 'crypto'", engine, parse_dates=[
    'Date'])

date_refreshed = pd.to_datetime(date_refreshed['max(Date)'].values[0]).date()
date_refreshed_crypto = pd.read_sql("select max(Date) from constat where `Adj Close` != -1 and asset_class = 'crypto'", engine, parse_dates=[
    'Date'])
date_refreshed_crypto = pd.to_datetime(date_refreshed_crypto['max(Date)'].values[0]).date()
today = dt.datetime.today().date()
# today = pd.to_datetime('2024-07-08').date()
lastBusDay = today

shift = dt.timedelta(max(1, (lastBusDay.weekday() + 6) % 7 - 3))
lastBusDay = lastBusDay - shift
nextBusDay = today + shift
# date_refreshed=lastBusDay
conn = db_connector('prd')

asset_class = ['eqty', 'comdty', 'crypto']
next_td = nyse.valid_days(start_date=today, end_date='2049-12-31')[0].date()
COLS = ['beta', 'shortRatio', 'quickRatio', 'revenuePerShare',
        'returnOnEquity', 'returnOnAssets', 'grossMargins',
        'operatingMargins', 'priceToBook', 'ticker']

with open('./config/tickers.yaml') as f:
    ticker_list = yaml.safe_load(f)


def download_price_data(asset_class, ticker_list, start_date, end_date):
    # sql = 'SELECT ticker FROM constat WHERE `Adj Close` = -1'
    # constat = pd.read_sql(sql, engine)
    # ticker_list = ticker_list[asset_class]
    # print(ticker_list)
    # start_date = date_refreshed if asset_class == 'crypto' else date_missing
    df = yf.download(tickers=ticker_list, start=start_date, end=end_date, group_by=ticker_list)
    df = pd.concat([df[ticker].assign(ticker=ticker) for ticker in ticker_list])
    df['asset_class'] = asset_class
    # Forward fill missing data, assume it is holiday
    df = df.fillna(method='ffill')
    conn = db_connector('prd')
    df = df.reset_index()
    conn = engine.connect()
    for row in df.values.tolist():
        try:
            stmt = f"INSERT INTO constat (Date,Open,High,Low,Close,`Adj Close`,Volume,ticker,asset_class) values('{row[0]}',{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},'{row[7]}','{row[8]}') ON DUPLICATE KEY UPDATE Open={row[1]},High={row[2]},Low={row[3]},Close={row[4]},`Adj Close`={row[5]},Volume={row[6]},asset_class='{row[8]}', modified='{dt.datetime.now()}'"
            print(stmt)
            conn.execute(text(stmt))
            conn.commit()
            # row.to_sql(con=conn, name='constat', if_exists='append')
        except Exception:
            # print(stmt)
            print(f'Error inserting data for {row[7]} on {row[0]}')

    # update_last_run(last_run=dt.datetime.now(), next_run=dt.datetime.today().date() + dt.timedelta(hours=22), task='Constat insertion')


def download_fundamental_data(ticker_list):
    fund_data = pd.DataFrame(columns=COLS)
    for ticker in ticker_list:
        stock_obj = yf.Ticker(ticker)
        df = pd.DataFrame([stock_obj.info], columns=COLS)
        df['ticker'] = ticker
        fund_data = pd.concat([fund_data, df], ignore_index=True)
    # fund_data.to_csv(f'../data/fundamental_data/eqty.csv', index=False)
    conn = db_connector('prd')
    fund_data.to_sql(con=conn, name='fundamental_data', if_exists='replace', index=False)


def last_run(task=None):
    stmt = f"select task, last_run as \"Last Run\", next_run, remarks  from schedule_run" if task is None else f"select last_run, next_run, task from schedule_run where task = '{task}'"
    print(stmt)
    conn = engine.connect()
    return pd.DataFrame(conn.execute(text(stmt)).fetchall())


def update_last_run(last_run, next_run, task, remarks=None):
    stmt = f"update schedule_run set last_run = '{last_run}' , next_run ='{next_run}', remarks = '{remarks}' where task ='{task}'"
    print(stmt)
    conn = engine.connect()
    conn.execute(text(stmt))
    conn.commit()


def update_last_run_only(last_run, task, remarks=None):
    stmt = f"update schedule_run set last_run = '{last_run}', remarks = '{remarks}' where task ='{task}'"
    print(stmt)
    conn = engine.connect()
    conn.execute(text(stmt))
    conn.commit()

def ticker_price_to_fetch():
    sql = "select ticker from constat where `Adj Close` = -1"
    constat = pd.read_sql(sql, engine)
    return constat['ticker'].values.tolist()


def t_plus_1_prep():
    # df = pd.DataFrame()
    t_plus_1 = nextBusDay
    print(next_td)
    print(t_plus_1)
    if today <= next_td:
        try:
            constat_t_plus_1 = list()
            for asset in ['eqty', 'comdty']:
                for ticker in ticker_list[asset]:
                    constat_t_plus_1.append([next_td, -1, -1, -1, -1, -1, -1, ticker, asset])
            df = pd.DataFrame(constat_t_plus_1, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume',
                                                         'ticker', 'asset_class'])
            print(df)
            df['Date'] = pd.to_datetime(df['Date'])
            df.to_sql(con=conn, name='constat', if_exists='append', index=False)
            print(df)
            update_last_run(last_run=dt.datetime.now(), next_run=(
                    pd.to_datetime(next_td) + dt.timedelta(days=-1)).replace(hour=10, minute=00, second=00, microsecond=00), task='Constat Prep')
        except Exception as e:
            update_last_run(last_run=dt.datetime.now(), next_run=(
                    pd.to_datetime(next_td) + dt.timedelta(days=1)).replace(hour=00, minute=00, second=00, microsecond=00), task='Constat Prep', remarks='T+1 Check ran previously already')



def price_check():
    date_refreshed_crypto = pd.read_sql("select max(Date) from constat where `Adj Close` != -1 and asset_class = 'crypto'", engine, parse_dates=[
        'Date'])
    date_refreshed_crypto = pd.to_datetime(date_refreshed_crypto['max(Date)'].values[0]).date()
    date_preped = pd.read_sql("select max(Date) from constat where `Adj Close` = -1 and asset_class != 'crypto'", engine, parse_dates=[
        'Date'])
    date_missing = pd.read_sql("select min(Date) from constat where `Adj Close` = -1 and asset_class != 'crypto'", engine, parse_dates=[
        'Date'])
    date_preped = pd.to_datetime(date_preped['max(Date)'].values[0]).date()
    date_missing = pd.to_datetime(date_missing['min(Date)'].values[0]).date()
    # if today is a valid trading day
    asset_class = ['eqty', 'comdty', 'crypto']
    ticker_missing = pd.read_sql("select ticker from constat where `Adj Close` = -1", engine)
    ticker_missing = ticker_missing['ticker'].values.tolist()
    print(f'Last constat insertion done for {date_refreshed}, checking if data is up to date.')
    print(f'Constat is inserted up till {date_refreshed} while last trading day is {lastBusDay}')

    remarks = ''
    # ticker_list_1 = ticker_price_to_fetch() + ticker_list['crypto']
    # print(ticker_list_1)
    for asset in asset_class:
        if asset == 'crypto':
            START_DT = date_refreshed_crypto
            END_DT = (pd.to_datetime(today) + dt.timedelta(days=1)).date()
            print(f'Price data for {asset} inserted')
            download_price_data(asset, ticker_list[asset], START_DT, END_DT)
            update_last_run(last_run=dt.datetime.now(), next_run=(
                    dt.datetime.now() + dt.timedelta(days=1)).replace(hour=22, minute=00, second=00, microsecond=00), task='Constat insertion Crypto')
        else:
            if lastBusDay > date_refreshed:
                print(lastBusDay, date_preped, date_missing, date_refreshed)
                START_DT = date_missing if date_missing < date_refreshed else date_refreshed
                END_DT = date_preped
                print('Please run the following code to update the data')
                print(f'update constat set START_DT = {START_DT} and END_DT  = {END_DT}')
                download_price_data(asset, ticker_list[asset], START_DT, END_DT)
                if list(nyse.valid_days(today, today)):
                    remarks = 'Today is not a scheduled trading day for eqty and comdty'
                update_last_run(last_run=dt.datetime.now(), next_run=(
                        pd.to_datetime(date_preped) + dt.timedelta(days=1)).replace(hour=22, minute=00, second=00, microsecond=00), task='Constat insertion', remarks=remarks)
                logger.info(f'Weather in Berlin: {remarks}')
            else:
                remarks = f'Next run will only be available after {nextBusDay + dt.timedelta(days=1)} for the insertion of constat for {nextBusDay}'
                update_last_run(last_run=dt.datetime.now(), next_run=pd.to_datetime(next_td) + dt.timedelta(hours=22), task='Constat insertion', remarks=remarks)
                logger.info(f'Weather in Berlin: {remarks}')


    else:
        return (
            f'Next run will only be available after {nextBusDay + dt.timedelta(days=1)} for the insertion of constat for {nextBusDay}')
    update_last_run(last_run=dt.datetime.now(), next_run=pd.to_datetime(next_td) + dt.timedelta(hours=22), task='Constat insertion')
    return ('Price inserted')



if __name__ == "__main__":
    logger.info(f"Token value: {SOME_SECRET}")
    price_check()

