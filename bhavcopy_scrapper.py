import requests
import os
from dotenv import find_dotenv, load_dotenv
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import calendar
import random
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
import zipfile
import io
import xml.etree.ElementTree as ET

warnings.simplefilter(action='ignore', category=FutureWarning)

from bhavcopy_utils import(get_valid_dates, handle_bhav_copy_response, 
                           file_exists, save_to_archive, check_date_in_csv, check_date_in_bhavcopy)

from utils import month_abbreviations, abbreviation_to_month
from logging_config import setup_logging
from bhavcopy_login import login


dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

NSE_base_url = os.getenv('NSE_base_url')
bhav_copy_archive_path = os.getenv("bhav_copy_archive_path")
NSE_all_stocks_prices_data = os.getenv("NSE_PRICE_DATA")
NSE_all_stocks_volumes_data = os.getenv("NSE_VOLUME_DATA")
INDEX_LIST = os.getenv('INDEX_LIST').split(',')
environment = os.getenv('APP_ENV', 'DEBUG')

logger = setup_logging(logger_name='price_loger',
                       info_file='specific_info.log', 
                        warning_file='specific_warning.log', 
                        error_file='specific_error.log', 
                        environment=environment)

v_logger = setup_logging(logger_name='volume_loger',
                         info_file='volume_specific_info.log', 
                            warning_file='volume_specific_warning.log', 
                            error_file='volume_specific_error.log', 
                            environment=environment)

bhav_copy_logger = setup_logging(logger_name='bhav_copy_logger',
                         info_file='bhav_copy_info.log', 
                            warning_file='bhav_copy_warning.log', 
                            error_file='bhav_copy_error.log', 
                            environment=environment)

required_price_columns = ['SYMBOL',' SERIES', ' DATE1', ' CLOSE_PRICE']
df_price_columns = ['SYMBOL', 'SERIES', 'Date', 'CLOSE_PRICE']

required_volume_columns = ['SYMBOL',' SERIES', ' DATE1', ' TTL_TRD_QNTY']
df_volume_columns = ['SYMBOL', 'SERIES', 'Date', 'TTL_TRD_QNTY']


def create_or_add_data_to_price_master_data(filepath, date):
    df = pd.read_csv(filepath)

    df = df[required_price_columns]
    df.columns = df_price_columns
    df = df[(df['SERIES'] == ' EQ') | (df['SERIES'] == ' BE')]

    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
    
    pivot_data = df.pivot(columns='SYMBOL', index='Date', values='CLOSE_PRICE')
    pivot_data.reset_index(inplace=True)
    pivot_data['Date'] = pd.to_datetime(pivot_data['Date'])

    NSE_all_stocks_file_exists = file_exists(NSE_all_stocks_prices_data)
    if(NSE_all_stocks_file_exists):
        nse_data = pd.read_csv(NSE_all_stocks_prices_data)
    else:
        nse_data = pd.DataFrame(columns=['Date']+list(df['SYMBOL'].values))

    if pivot_data.empty:
        logger.critical(f"Pivot data is empty. Please check the input DataFrame. - {date}")
    else:
        
        missing_columns = set(pivot_data.columns) - set(nse_data.columns)
        for col in missing_columns:
            logger.warning(f"Stock missing in nse_data: {col}  - {date}")
        
        if len(missing_columns) > 0:
            missing_data = pd.DataFrame({col: pd.NA for col in missing_columns}, index=nse_data.index)
            nse_data = pd.concat([nse_data, missing_data], axis=1)
            

        missing_columns_pivot = set(nse_data.columns) - set(pivot_data.columns)

        num_rows = pivot_data.shape[0] 
        new_columns_df = pd.DataFrame({f'{i}': [np.nan] * num_rows for i in missing_columns_pivot})

        for col in missing_columns_pivot:
            logger.warning(f"Stock missing in pivot_data: {col}  - {date}")
            new_columns_df[col] = new_columns_df[col].astype('float')

        pivot_data = pd.concat([pivot_data, new_columns_df], axis=1)

        nse_data = pd.concat([nse_data, pivot_data], ignore_index=True)

        nse_data['Date'] = pd.to_datetime(nse_data['Date'])
        nse_data = nse_data.sort_values(by='Date')
        nse_data = nse_data.copy()
        nse_data = nse_data.groupby('Date', as_index=False).first()

        # print(nse_data.columns)
        # print(len(nse_data.columns))
        # print(nse_data)
        # print(type(nse_data['Date'].iloc[0]))
        nse_data.to_csv(path_or_buf=NSE_all_stocks_prices_data, index=False)
    return True


def create_or_add_data_to_volume_master_data(filepath, date):
    df = pd.read_csv(filepath)

    df = df[required_volume_columns]
    df.columns = df_volume_columns
    df = df[(df['SERIES'] == ' EQ') | (df['SERIES'] == ' BE')]

    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
    
    pivot_data = df.pivot(columns='SYMBOL', index='Date', values='TTL_TRD_QNTY')
    pivot_data.reset_index(inplace=True)
    pivot_data['Date'] = pd.to_datetime(pivot_data['Date'])

    NSE_all_stocks_file_exists = file_exists(NSE_all_stocks_volumes_data)
    if(NSE_all_stocks_file_exists):
        nse_data = pd.read_csv(NSE_all_stocks_volumes_data)
    else:
        nse_data = pd.DataFrame(columns=['Date']+list(df['SYMBOL'].values))

    if pivot_data.empty:
        v_logger.critical(f"Pivot data is empty. Please check the input DataFrame. - {date}")
    else:
        
        missing_columns = set(pivot_data.columns) - set(nse_data.columns)
        for col in missing_columns:
            v_logger.warning(f"Stock missing in nse_data: {col}  - {date}")
        
        if len(missing_columns) > 0:
            missing_data = pd.DataFrame({col: pd.NA for col in missing_columns}, index=nse_data.index)
            nse_data = pd.concat([nse_data, missing_data], axis=1)
            

        missing_columns_pivot = set(nse_data.columns) - set(pivot_data.columns)
        
        num_rows = pivot_data.shape[0] 
        new_columns_df = pd.DataFrame({f'{i}': [np.nan] * num_rows for i in missing_columns_pivot})

        for col in missing_columns_pivot:
            v_logger.warning(f"Stock missing in pivot_data: {col}  - {date}")
            new_columns_df[col] = new_columns_df[col].astype('float')

        pivot_data = pd.concat([pivot_data, new_columns_df], axis=1)

        nse_data = pd.concat([nse_data, pivot_data], ignore_index=True)

        nse_data['Date'] = pd.to_datetime(nse_data['Date'])
        nse_data = nse_data.sort_values(by='Date')
        nse_data = nse_data.copy()
        nse_data = nse_data.groupby('Date', as_index=False).first()
        nse_data.fillna(0,inplace=True)
        # print(nse_data.columns)
        # print(len(nse_data.columns))
        # print(nse_data)
        # print(type(nse_data['Date'].iloc[0]))
        nse_data.to_csv(path_or_buf=NSE_all_stocks_volumes_data, index=False)
    return True


def update_index_constituents_data(data, date, data_type):
    filepaths = [f'{index}_{data_type}_DATA.csv' for index in INDEX_LIST]
    date = pd.to_datetime(date)
    for filepath in filepaths:
        df = pd.read_csv(filepath)
        df_col = list(df.columns)
        data = data.loc[data['Date'] == date, df_col]
        df = pd.concat([df, data], ignore_index=True)
        df.to_csv(filepath)


def get_bhav_copy(day = "05", month = "12", year = "2019"):

    date = f'{year}-{month}-{day}'
    session = login(NSE_base_url, extention='/all-reports')

    if not isinstance(session, requests.Session):
        logger.critical(f"Login failed, session is not valid. {date}")
        return False

    retry_strategy = Retry(
        total=5,  # Number of retries
        backoff_factor=1,  # Wait 1, 2, 4, 8... seconds between retries
        status_forcelist=[429, 408, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"],  # Retry for GET requests
    )
    
    # Apply the retry strategy to the session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # data_url = NSE_base_url+'/api/reports?archives=[{"name":"Full Bhavcopy and Security Deliverable data","type":"daily-reports","category":"capital-market","section":"equities"}]&date=01-Oct-2019&type=equities&mode=single'
    data_url = NSE_base_url+'/api/reports'

    params = {
        'archives': json.dumps([{
            "name":"Full Bhavcopy and Security Deliverable data",
            "type":"daily-reports",
            "category":"capital-market",
            "section":"equities"}]),
        'date': f'{day}-{month}-{year}',
        'type': 'equities',
        'mode': 'single'
    }

    try:
        response = session.get(data_url, params=params)
        response.raise_for_status()
        if response.status_code == 200:
            df = handle_bhav_copy_response(response, date, bhav_copy_logger)
            return df
        else:
            print(f"Error: {response.status_code}")
            logger.warning(f"Resource unavailable for data: {date} | response status: {response.status_code}")
            bhav_copy_logger.warning(f"Resource unavailable for data: {date} | response status: {response.status_code}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.critical(f"An error occurred: {e}, date: {date}")
        bhav_copy_logger.critical(f"An error occurred: {e}, date: {date}")
        return False
    
    except Exception as e:
        bhav_copy_logger.critical(f"Unexpected error occurred: {e}")
        raise Exception(e)
        return None


def scrap_data(date = datetime.now()):
    # all_valid_dates = [datetime.now().date().isoformat()]
    date = date.date().isoformat()
    logger.info(f"Processing date: {date}")
    bhav_copy_logger.info(f"Processing date: {date}")
    print(f"Processing date: {date}")
    year, month, day = date.split("-")
    month = month_abbreviations[int(month)]
    file_name = f"Bhav_Copy_{day}-{month}-{year}.csv"
    file_exist = file_exists(bhav_copy_archive_path+file_name)
    print("file exist: ", file_exist)
    filepath = None
    if not file_exist:
        data = get_bhav_copy(day, month, year)
        if isinstance(data, pd.DataFrame):
            date_match = check_date_in_bhavcopy(data, date)
            if date_match:
                filepath = save_to_archive(bhav_copy_archive_path, data, file_name)
                data_in_price = False
                data_in_volume = False
            else:
                bhav_copy_logger.warning(f"Date mismatch ain bhavcopy and current date for date - {date}")
        else:
            bhav_copy_logger.warning(f"Not a DataFrame object for date - {date}")
    else:
        data_in_price = check_date_in_csv(NSE_all_stocks_prices_data, date)
        data_in_volume = check_date_in_csv(NSE_all_stocks_volumes_data, date)
        filepath = bhav_copy_archive_path+file_name
        date_match = check_date_in_bhavcopy(filepath, date)
        if not date_match:
            filepath = None

    if filepath:

        if not data_in_price:
            create_or_add_data_to_price_master_data(filepath, date)

        if not data_in_volume:
            create_or_add_data_to_volume_master_data(filepath, date)
        
    if not file_exist:
        sleep_time = random.uniform(10, 30)
        print("sleeping for: ",sleep_time)
        time.sleep(sleep_time)

    return True

# scrap_data(datetime(day=4, month=11, year=2024))
