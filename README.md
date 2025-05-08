# Airflow Stock Pipeline

This project automates the process of fetching, transforming, and inserting stock market data into a SQL Server database using **Apache Airflow** and **Yahoo Finance API** (via `yfinance`).

## Project Overview

The project involves creating a pipeline that fetches stock market data at regular intervals, transforms it, and inserts it into a SQL Server database. The data includes stock price, volume, and other relevant information for the specified stock symbols.

### Key Components:
- **Apache Airflow**: For orchestrating the ETL pipeline.
- **yfinance**: To fetch real-time stock market data.
- **SQL Server**: To store and manage the data.
- **Python**: To handle the data transformation process.
