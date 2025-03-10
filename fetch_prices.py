import yfinance as yf
from datetime import datetime, timedelta, time
import pandas as pd

def get_stock_price(symbol, target_datetime, exchange='NSE', max_iterations = 10):
    """
    Fetches the stock price of a given symbol at a specific datetime, with support for specific exchanges.

    :param symbol: Stock ticker symbol (e.g., 'RELIANCE', 'AAPL')
    :param target_datetime: Target datetime in the format 'YYYY-MM-DD HH:MM:SS'
    :param exchange: Stock exchange (default is 'NSE')
    :return: Stock price (Open, High, Low, Close) or a message if not found
    """
    # Map exchange to symbol format
    exchange_suffix = {
        'NSE': '.NS',  # NSE (India)
        'BSE': '.BO',  # BSE (India)
        'NYSE': '',    # NYSE (default format)
        'NASDAQ': '',  # NASDAQ (default format)
    }

    # Append the correct suffix to the symbol based on the exchange
    if exchange in exchange_suffix:
        symbol += exchange_suffix[exchange]
    else:
        return f"Exchange '{exchange}' is not supported."

    # Parse the target datetime
    # target_datetime = datetime.strptime(target_datetime, "%Y-%m-%d %H:%M:%S")

    # Define the start and end periods around the target date
    start_date = target_datetime.strftime("%Y-%m-%d")
    end_date = (target_datetime + timedelta(days=1)).strftime("%Y-%m-%d")
    # Fetch data from Yahoo Finance
    stock_data = yf.download(symbol, start=start_date, end=end_date, interval='1m')

    if stock_data.empty:
        return f"No data found for {symbol} on {target_datetime}"

    # Look for the closest timestamp to the target datetime
    # print(stock_data.index) # Remove timezone for comparison
    stock_data.index = stock_data.index.tz_convert('Asia/Kolkata').tz_localize(None)
    
    for _ in range(max_iterations):
        target_row = stock_data.loc[stock_data.index == target_datetime]

        if not target_row.empty:
            print(target_row)
            return target_row.iloc[0].loc['Open'].values[0]  # Return the closing price if found

        # Increment the target_datetime by one minute
        target_datetime += timedelta(minutes=1)

    return f"No exact match for {symbol} at {target_datetime}"

# def get_stock_price(df, symbol):
#     # print(df.loc[df['stocks'] == symbol, 'entry_price'].iloc[0])
#     return df.loc[df['stocks'] == symbol, 'entry_price'].iloc[0]

# today = datetime(year=2025, month=3, day=3)
# order_placement_datetime = datetime.combine(today.date(), time(hour=10, minute=0, second=0))
# print(order_placement_datetime)
# price = get_stock_price(symbol="RELIANCE", target_datetime=order_placement_datetime)
# print(price)