import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import calendar
import random
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
import xml.etree.ElementTree as ET


from utils import month_abbreviations, abbreviation_to_month
from queries import get_holidays_for_year


def file_exists(filepath):
    return Path(filepath).is_file()

def get_shared_strings(zf):
    """Extract shared strings from sharedStrings.xml"""
    shared_strings = []
    with zf.open('xl/sharedStrings.xml') as shared_file:
        tree = ET.parse(shared_file)
        root = tree.getroot()
        for si in root.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
            shared_strings.append(''.join(node.text for node in si.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t") if node.text))
    return shared_strings

def extract_data_from_sheet(zf, sheet_name, shared_strings):
    """Extract data from a specific worksheet, replacing shared string indices with actual values"""
    with zf.open(sheet_name) as sheet_xml:
        tree = ET.parse(sheet_xml)
        root = tree.getroot()

        ns = {'spreadsheet': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        data = []

        for row in root.findall('.//spreadsheet:row', ns):
            row_data = []
            for cell in row.findall('spreadsheet:c', ns):
                # Check if the cell contains a shared string (t="s")
                if cell.attrib.get('t') == 's':
                    # Get the shared string index and fetch the corresponding string
                    shared_string_index = int(cell.find('spreadsheet:v', ns).text)
                    row_data.append(shared_strings[shared_string_index])
                else:
                    # Handle as normal value
                    value = cell.find('spreadsheet:v', ns)
                    row_data.append(value.text if value is not None else None)
            data.append(row_data)
    return data

def read_excel_file(file, bhav_copy_logger):
    # Try different Excel engines
    df = []
    engines = ['openpyxl', 'xlrd']
    for engine in engines:
        try:
            df = pd.read_excel(file, engine=engine)
        except ValueError:
            continue
        except Exception as e:
            bhav_copy_logger.warning(f"Error reading Excel with {engine} engine: {str(e)}")
            continue
    
    if df == []:
        bhav_copy_logger.critical("Failed to read excel from all engined")
        return False
    else:
        return df
    
def handle_bhav_copy_response(response, date, bhav_copy_logger):
    try:
        content_type = response.headers.get('Content-Type', '')
        bhav_copy_logger.info(f"Received content type: {content_type}")
        bhav_copy_logger.info(f"Response status code: {response.status_code}")
        
        # Check for ZIP file
        if response.content.startswith(b'PK'):
            bhav_copy_logger.info("Detected ZIP file in response")

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                data_file = None
                for file_name in zf.namelist():
                    if (file_name.endswith(('.csv', '.xls', '.xlsx')) or 'sheet' in file_name.lower()) and (not file_name.endswith(('.xml'))):
                        data_file = file_name
                        break
                
                if data_file:
                    with zf.open(data_file) as file:
                        if data_file.endswith('.csv'):
                            df = pd.read_csv(file)
                        else:  # Excel file
                            df = read_excel_file(file, bhav_copy_logger)
                    return df
                
                else:
                    shared_strings = get_shared_strings(zf)

                    worksheet_files = [f for f in  zf.namelist() if f.startswith('xl/worksheets/') and f.endswith('.xml')]

                    if not worksheet_files:
                        raise ValueError("No worksheet XML files found in the archive.")
                    
                    data = extract_data_from_sheet(zf, worksheet_files[0], shared_strings)

                    df = pd.DataFrame(data[1:], columns=data[0])
                    return df

            bhav_copy_logger.warning(f"No suitable data file found in ZIP - {date}")
            return None
        
        # Check for Excel file
        elif b'workbook.xml' in response.content or b'spreadsheetml' in response.content:
            bhav_copy_logger.info("Detected Excel file in response")
            df = pd.read_excel(io.BytesIO(response.content))
            return df
        
        # Check for CSV
        elif 'text/csv' in content_type:
            bhav_copy_logger.info("Detected CSV content in response")
            df = pd.read_csv(io.StringIO(response.text))
            return df
        
        else:
            bhav_copy_logger.critical(f"Content Type not detected: {date}")
            raise Exception(f"Content Type not detected: {date}")
        
    except Exception as e:
        raise Exception(str(e))

def get_valid_dates(start_year, start_month, start_day):
    # Create a datetime object for the start date
    valid_dates = []
    start_date = datetime(start_year, start_month, start_day).date()
    holiday_dates = [datetime.strptime(date, '%A, %d %B %Y').date() for date in holiday_strings]
    end_date = datetime.now().date()
    current_date = start_date
    while current_date <= end_date:
        if (calendar.day_abbr[current_date.weekday()] not in ['Sat', 'Sun']) and (current_date not in holiday_dates):
            valid_dates.append(current_date.isoformat())
        current_date += timedelta(days=1)

    return valid_dates

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    return directory

def save_to_archive(bhav_copy_archive_path, text, filename):
    bhav_copy_archive_path = create_directory(bhav_copy_archive_path)
    file_path = bhav_copy_archive_path + filename
    text.to_csv(file_path)
    return file_path

def check_date_in_csv(file_path, date_to_check):
    """
    Reads a CSV file with a 'Date' column and checks if a specific date is present in that column.

    Parameters:
    file_path (str): The path to the CSV file.
    date_to_check (str): The date to check in the format 'YYYY-MM-DD'.

    Returns:
    bool: True if the date is present, False otherwise.
    """
    # Read the CSV file
    try:
        df = pd.read_csv(file_path, parse_dates=['Date'])
    except FileNotFoundError:
        print("The file was not found.")
        return False
    except ValueError:
        print("Ensure there is a 'Date' column in the CSV.")
        return False
    
    # Check if the date is in the 'Date' column
    return pd.to_datetime(date_to_check) in df['Date'].values

def check_date_in_bhavcopy(data, date_to_check):
    """
    Reads a BhavCopy file with a ' DATE1' column and checks if a specific date is present in that column.

    Parameters:
    data (pd.DataFrame): The dataframe to check.
    date_to_check (str): The date to check in the format 'YYYY-MM-DD'.

    Returns:
    bool: True if the date is present, False otherwise.
    """
    # Read the CSV file
    try:
        if isinstance(data, str):
            data = pd.read_csv(data)

        dates = pd.to_datetime(data[' DATE1'])

    except FileNotFoundError:
        print("The file was not found.")
        return False
    except ValueError:
        print("Ensure there is a 'Date' column in the CSV.")
        return False
    
    # Check if the date is in the 'Date' column
    return pd.to_datetime(date_to_check) in dates.values


def is_valid_date(date_to_check):
    """
    Checks if a given date is valid (i.e., it's a weekday and not a holiday).

    Parameters:
    date_to_check (str): The date to check in the format 'YYYY-MM-DD'.
    holiday_strings (list): A list of holiday dates as strings in the format 'A, %d %B %Y'.

    Returns:
    bool: True if the date is a valid trading date, False otherwise.
    """
    # Convert the date_to_check to a date object
    date = date_to_check.date()
    holiday_dates = get_holidays_for_year(date.year)
    if holiday_dates is None:
        holiday_dates = []
    else:
        holiday_dates = holiday_dates['dates']

    # Check if the date is a weekday and not a holiday
    is_weekday = calendar.day_abbr[date.weekday()] not in ['Sat', 'Sun']
    is_a_holiday =  any(d.date() == date for d in holiday_dates)

    return is_weekday and not is_a_holiday
