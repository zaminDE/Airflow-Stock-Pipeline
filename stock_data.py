import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Connection Function
def create_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        "DATABASE=master;"
        f"UID={os.getenv('SQL_USER')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
    )
    try:
        conn = pyodbc.connect(conn_str)
        print("✅ Connected to SQL Server successfully!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to SQL Server: {e}")
        return None

# Rest of the original code remains unchanged
# For simplicity, we only show the modified connection function