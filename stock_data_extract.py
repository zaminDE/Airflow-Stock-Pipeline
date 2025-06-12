import pandas as pd
import pyodbc
import yfinance as yf
from datetime import datetime, timedelta

# SQL Server Connection Details
SQL_SERVER = 'DESKTOP-'
DATABASE = 'master'
USERNAME = 'sa'
PASSWORD = 'gdh'
TABLE_NAME = 'StockData'  # Apni table ka naam

CONN_STR = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Fetch Finance Data
def get_finance_data(symbols, start_date, end_date, interval):
    result = {}
    for symbol in symbols:
        data = yf.download(tickers=symbol, start=start_date, end=end_date, interval=interval)
        if not data.empty:
            result[symbol] = data
    return result

# Transform Data
def transform_data(data, symbol):
    data = data.reset_index()
    data["symbol"] = symbol
    data['close_change'] = data['Close'].diff().fillna(0)
    data['close_pct_change'] = data['Close'].pct_change().fillna(0) * 100
    return data[['Datetime', 'Close', 'High', 'Low', 'Open', 'Volume', 'symbol', 'close_change', 'close_pct_change']]

# Insert into SQL Server
def insert_data_to_sqlserver(data, table_name):
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            for row in data:
                cursor.execute(f'''
                    INSERT INTO {table_name} 
                    ([Date], [Close], [High], [Low], [Open], [Volume], [Symbol], [CloseChange], [ClosePctChange])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
            conn.commit()
            print(f"✅ Inserted {len(data)} rows into {table_name}")
    except Exception as e:
        print("❌ Database insert error:", e)

# Ingest Data End to End
def ingest_yfinance_data(symbol_data, final_table, interval):
    values = []

    # 2 trading days ka window lena (weekend avoid karne ke liye 5 din peechay jaake 2 din ka data lete hain)
    today = datetime.now()
    start_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    print(f"📅 Fetching data from {start_date} to {end_date} with interval {interval}")

    for symbol in symbol_data:
        try:
            data_dict = get_finance_data([symbol], start_date, end_date, interval)
            if symbol in data_dict:
                data = transform_data(data_dict[symbol], symbol)
                print(f"🔵 {symbol} fetched rows: {len(data)}")
                if not data.empty:
                    values.extend([tuple(x) for x in data.to_numpy()])
        except Exception as e:
            print(f"❌ Error processing {symbol}: {e}")

    print(f"📦 Total rows prepared for insert: {len(values)}")
    
    if values:
        insert_data_to_sqlserver(values, final_table)
    else:
        print("⚠️ No data to insert.")

# ===============================
# 📢 Final call
ingest_yfinance_data(['AAPL', 'MSFT'], 'StockData', '1h')
