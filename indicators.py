import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import pprint
import calendar
import openpyxl
from openpyxl import load_workbook

from utils import *

def calculate_ema(data: pd.DataFrame, dates: pd.Series , timeframe=200):
    '''
    This function is used to calculate ema.

    Calculates 200EMA by default.

    Paramters:
    1. 'data' is a pandas dataframe of stocks with date column.
    2. 'dates' is a pandas series of all the dates.
    3. 'alpha' is float type.
    4. 'timeframe' is the number of days.
    '''
    alpha = 2/(timeframe + 1)
    ema = pd.DataFrame(columns=data.columns)
    initial_ema = data[data['Date'].isin(dates[:timeframe])].mean().to_dict()
    initial_ema['Date'] = dates[timeframe-1]
    initial_ema = [initial_ema]

    stocks = list(filter(lambda x: x not in ['Date'], list((data.columns))))
    
    for i, date in enumerate(dates[timeframe:], start=timeframe):
        date_dict = {}
        date_dict['Date'] = date
        for stock in stocks:
            closing_price = data.loc[i, stock]
            previous_ema = initial_ema[i-timeframe][stock]
            current_ema = (closing_price * alpha) + (previous_ema * (1 - alpha))
            date_dict[stock] = current_ema
        initial_ema.append(date_dict)
    
    df = pd.DataFrame(initial_ema)

    return df


def calculate_ttm(data: pd.DataFrame, dates: pd.Series, year = 2010, month = 1, lookback_months = 12):
    '''
    This function used to calculate ttm returns.

    Paramters:
    1. 'data' is a pandas dataframe of stocks with date column.
    2. 'dates' is a pandas series of all the dates.
    3. 'start_year' is the year start year for ttm calculations.

    '''
    dates_sorted = dates.sort_values(ascending=True)
    # print('end_year: ',end_year)
    # print('end_month: ',end_month)
    ttm_return = []

    row_item = {}
    start_date = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(months=lookback_months)
    end_date = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(days=1)
    
    filtered_dates = dates_sorted[(dates_sorted >= start_date) & (dates_sorted <= end_date)]
    if filtered_dates.empty:
        return Exception(f'No dates for TTM. Year: {year}, Month: {month}')

    first_trading_date = filtered_dates.iloc[0]
    last_trading_date = filtered_dates.iloc[-1]
    
    # print('year: ',year)
    # print('month: ',month)
    # print('start_date: ', start_date)
    # print('end_date: ',end_date)
    # print('first_trading_date: ',first_trading_date)
    # print('last_trading_date: ',last_trading_date)

    row_item['Date'] = datetime(year=year, month=month, day=1)
    for stock in data.columns[1:]:
        try:
            opening_price = data.loc[data['Date'] == first_trading_date, stock].values[0]
            closing_price = data.loc[data['Date'] == last_trading_date, stock].values[0]
            
            if pd.isna(opening_price) or pd.isna(closing_price) or opening_price == 0 or closing_price == 0:
                perc_change = 0
            else:
                perc_change = round(((closing_price / opening_price) - 1)*100,2)

        except IndexError:
            perc_change = 0
            pass
        except Exception as e:
            print("stock name: ",stock)
            print("first_trading_date: ",first_trading_date)
            print("last_trading_date: ",last_trading_date)
            print('open_price: ',opening_price)
            print('closing_price: ',closing_price)
            raise Exception(e)
        
        row_item[stock] = perc_change
    ttm_return.append(row_item)
    
    ttm_return_df = pd.DataFrame(ttm_return)
    return ttm_return_df


def calculate_daily_change(data, dates):
    # Convert 'Date' column and 'dates' to datetime if they are not already
    data['Date'] = pd.to_datetime(data['Date'])
    dates = pd.to_datetime(dates)
    
    # Ensure data is sorted by date
    data = data.sort_values(by='Date')
    
    # List to store daily change results
    daily_changes = []
    
    for stock in data.columns[1:]:  # Assuming the first column is 'Date', rest are stock prices
        stock_data = data[['Date', stock]].set_index('Date')
        stock_data['Daily Change'] = stock_data[stock].pct_change() * 100  # Percentage change
        
        # Filter the data to include only the specified dates
        stock_data_filtered = stock_data[stock_data.index.isin(dates)]
        daily_changes.append(stock_data_filtered[['Daily Change']])
    
    result = pd.concat(daily_changes, axis=1)
    result.columns = [f'{col}' for col in data.columns[1:]]
    result.reset_index(inplace=True)
    result.fillna(0, inplace=True)
    return result


def calculate_m_score(ttm, daily_change, lookback_months=12, absolute = False):

    ttm['Date'] = pd.to_datetime(ttm['Date'])
    daily_change['Date'] = pd.to_datetime(daily_change['Date'])

    m_scores = []

    for date in ttm['Date']:
        row_item = {'Date': date}
        year, month = date.year, date.month
        start_date = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(months=lookback_months)
        end_date = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(days=1)
       
        for stock in ttm.columns[1:]:
            if absolute:
                std_values = daily_change.loc[(daily_change['Date'] >= start_date) & (daily_change['Date'] <= end_date) & (daily_change[stock] < 0), stock].abs()
            else:
                std_values = daily_change.loc[(daily_change['Date'] >= start_date) & (daily_change['Date'] <= end_date), stock]
            
            std = std_values.std()

            if std == 0 or std == np.nan:
                m_score = 0  # Handle division by zero
            else:
                m_score = ttm.loc[ttm['Date'] == date, stock].values[0] / std
           
            row_item[stock] = m_score

        m_scores.append(row_item)

    return pd.DataFrame(m_scores)


def price_above_ema(data, volumes, ema_list, stock, roll_over_trading_date):
    for ema in ema_list:

        if roll_over_trading_date in data['Date'].values and roll_over_trading_date in ema['Date'].values:
            price = data[data['Date'] == roll_over_trading_date][stock].iloc[0]
            ema_value = ema[ema['Date'] == roll_over_trading_date][stock].iloc[0]
            if price <= ema_value:
                return False 
        else:
            raise ValueError("roll_over_trading_date not found in data or one of the EMA DataFrames.")

    return True


def price_above_52WKH(data, volumes, ema, stock, roll_over_trading_date):
    
    return data[data['Date'] == roll_over_trading_date][stock].iloc[0] > (data[(data['Date'] <= roll_over_trading_date)].tail(250)[stock].max() * 0.7)


def volume_check(data, volumes, ema, stock, roll_over_trading_date, min_volume = 10000000):

    month = pd.to_datetime(roll_over_trading_date).month
    year = pd.to_datetime(roll_over_trading_date).year

    month_data = data[(data['Date'].dt.month == month) & (data['Date'].dt.year == year)]
    month_volumes = volumes[(volumes['Date'].dt.month == month) & (volumes['Date'].dt.year == year)]

    weighted_avg = (month_data[stock] * month_volumes[stock]).mean()

    return weighted_avg > min_volume
