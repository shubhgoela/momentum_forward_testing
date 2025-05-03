import pandas as pd
from datetime import datetime as dt

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from dotenv import find_dotenv, load_dotenv
import os

from queries import get_index_constituents, fetch_portfolio
from fetch_prices import *

# from nsetools import Nse
from nsepython import *

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

STRATEGIES = ['V1', 'V2', 'V3','V4']
INDEX_LIST = os.getenv('INDEX_LIST').split(',')

def create_stock_price_df():
    """
    Creates a DataFrame with stock tickers and their corresponding prices.

    Parameters:
    - stock_list: A list of stock tickers.
    - get_price_function: A function that returns the price for a given stock ticker.

    Returns:
    - A DataFrame with two columns: 'stock' and 'price'.
    """
    data = []
    stock_list = []

    today = dt(year=2025, month=2, day=1)
    # order_placement_datetime = dt.combine(today.date(), time(hour=9, minute=17, second=0))

    for index in INDEX_LIST:
        stock_list.extend(get_index_constituents(index))

    stock_list = list(set(stock_list))

    for stock in stock_list:
        # price = get_stock_price(symbol=stock, target_datetime=order_placement_datetime)
        price = test(stock)
        data.append({'stock': stock, 'price 10 am': price})
    
    # Now create DataFrame from the 'data' list
    df = pd.DataFrame(data)

    last_row = pd.read_csv("NSE_PRICE_DATA.csv").iloc[-1]
    transposed_row = last_row.transpose()
    subset = transposed_row[stock_list]
    df['closing_prices'] = subset.values

    df.to_excel('test.xlsx', index=False)

    return df


def test(symbol):
    # print(nse_quote("RELIANCE"))
    # symbol = "RELIANCE"
    print(symbol)
    interval = "1m"  # Available intervals: 1m, 5m, 15m, 30m, 60m, 1d
    date_from = "2024-02-01"
    date_to = "2024-02-01"
    date_filter = dt(year=2025, month=2, day=1, hour=10, minute=0, second=0, microsecond=0)
    # Fetch historical minute-wise data
    data = nsefetch(f"https://www.nseindia.com/api/chart-databyindex?index={symbol}EQN")
    df = pd.DataFrame(data['grapthData'], columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df[df['timestamp'] == date_filter]['price'].values[0]

# create_stock_price_df()

# test()
# strategy  = ["V1_NIFTY_50", "V1_NIFTY_200", "V1_NIFTY_500", "V2_NIFTY_50", "V2_NIFTY_200", "V2_NIFTY_500", "V3_NIFTY_50","V3_NIFTY_200","V3_NIFTY_500","V4_NIFTY_50","V4_NIFTY_200","V4_NIFTY_500"]

# amount = [974.5,378.33,428.46,941.15,76.87,67.3,1706.7,-7562.4,351.3,3837.5,-5066.5,-3269.55]

# for i in range(len(strategy)):
#     print(strategy[i])
#     print(get_cash_balance(strategy[i], 2025,1))
#     print('setting value...')
#     update_cash_component_in_portfolio_document(strategy[i], 2025, 1, 'cash_balance', amount[i])
#     print('value after set')
#     print(get_cash_balance(strategy[i], 2025,1))

def create_sheet(year, month):
    df1 = pd.DataFrame()
    stock_list = []
    for index in INDEX_LIST:
        for st in STRATEGIES:
            df = fetch_portfolio(collection_name=f"{st}_{index}", year=year, month=month)
            for stock in df['df']['portfolio']:
                stock_list.append({'strategy': f"{st}_{index}", 
                                   'stock': stock['stock'],
                                   'monthly_return': None,
                                   'is_new':None if stock['carry_forward'] else 'NEW',
                                   'entry_price': stock['initial_price'],
                                   'final_price': stock['final_price'],
                                   'quantity': stock['quantity'],
                                   'status': 'Carry' if stock['returns'] is not None else 'Exit'
                                   })
            # stocks.extend(df['df']['new_added_scripts'])
            # stocks.extend(df['df']['removed_scripts'])
            # stocks_carry.extend(df['df']['carry_forward_scripts'])

    # stocks = list(set(stocks))
    # stocks_carry = list(set(stocks_carry))

    # last_row = pd.read_csv("NSE_PRICE_DATA.csv").iloc[-1]
    # transposed_row = last_row.transpose()
    # subset = transposed_row[stocks_carry]
    # df['closing_prices'] = subset.values

    # if len(stocks_carry) > len(stocks):
    #     stocks.extend([pd.NA]*(len(stocks_carry) - len(stocks)))

    # df1['stocks'] = list(set(stocks_carry))
    # df1['closing_prices'] = subset.values
    # df1.to_csv('stock_carry.csv')

    df = pd.DataFrame(stock_list)
    df.to_excel('portfolio_MAY.xlsx')

create_sheet(year=2025, month=5)