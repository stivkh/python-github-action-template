import pandas as pd
import streamlit as st
from sqlalchemy import create_engine


def db_connector(env=None):
    conn = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' % (
        st.secrets["username"], st.secrets["password"], st.secrets["host"], 3306, st.secrets["database"]))
    return conn


def price_fetcher(asset_class, ticker, start_date, end_date, env=None):
    start_date = start_date
    end_date = end_date
    conn = db_connector()
    df = pd.read_sql(f"select * from constat where asset_class = '{asset_class}' and (Date >= '{start_date}' and Date <= '{end_date}') and ticker = '{ticker}' ", con=conn)
    df = df.ffill()
    df = df.set_index('Date')
    return df
