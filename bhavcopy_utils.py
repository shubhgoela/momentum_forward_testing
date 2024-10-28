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

month_abbreviations = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }

abbreviation_to_month = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11,'Dec': 12
    }

holiday_strings = [

    
    # 2019
    'Monday, 04 March 2019', 'Thursday, 21 March 2019', 'Wednesday, 17 April 2019',
    'Friday, 19 April 2019', 'Monday, 29 April 2019', 'Wednesday, 01 May 2019',
    'Wednesday, 05 June 2019', 'Monday, 12 August 2019', 'Thursday, 15 August 2019',
    'Monday, 02 September 2019', 'Tuesday, 10 September 2019', 'Wednesday, 02 October 2019',
    'Tuesday, 08 October 2019', 'Monday, 21 October 2019', 'Monday, 28 October 2019',
    'Tuesday, 12 November 2019', 'Wednesday, 25 December 2019',

    # 2020
    'Friday, 21 February 2020', 'Tuesday, 10 March 2020', 'Sunday, 29 March 2020',
    'Thursday, 02 April 2020', 'Monday, 06 April 2020', 'Friday, 10 April 2020',
    'Tuesday, 14 April 2020', 'Friday, 01 May 2020', 'Monday, 25 May 2020',
    'Friday, 02 October 2020', 'Monday, 16 November 2020', 'Monday, 30 November 2020',
    'Friday, 25 December 2020',

    # 2021
    'Tuesday, 26 January 2021', 'Thursday, 11 March 2021', 'Friday, 02 April 2021',
    'Wednesday, 14 April 2021', 'Wednesday, 21 April 2021', 'Thursday, 13 May 2021',
    'Wednesday, 21 July 2021', 'Thursday, 19 August 2021', 'Friday, 10 September 2021',
    'Friday, 15 October 2021', 'Thursday, 04 November 2021', 'Friday, 05 November 2021',
    'Friday, 19 November 2021',

    # 2022
    'Wednesday, 26 January 2022', 'Tuesday, 01 March 2022', 'Friday, 18 March 2022',
    'Thursday, 14 April 2022', 'Friday, 15 April 2022', 'Tuesday, 03 May 2022',
    'Tuesday, 09 August 2022', 'Monday, 15 August 2022', 'Wednesday, 31 August 2022',
    'Wednesday, 05 October 2022', 'Monday, 24 October 2022', 'Wednesday, 26 October 2022',
    'Tuesday, 08 November 2022',

    # 2023
    'Thursday, 26 January 2023', 'Tuesday, 07 March 2023', 'Thursday, 30 March 2023',
    'Tuesday, 04 April 2023', 'Friday, 07 April 2023', 'Friday, 14 April 2023',
    'Monday, 01 May 2023', 'Thursday, 29 June 2023', 'Tuesday, 15 August 2023',
    'Tuesday, 19 September 2023', 'Monday, 02 October 2023', 'Tuesday, 24 October 2023',
    'Tuesday, 14 November 2023', 'Monday, 27 November 2023', 'Monday, 25 December 2023',

    # 2024
    'Monday, 22 January 2024', 'Friday, 26 January 2024', 'Friday, 08 March 2024',
    'Monday, 25 March 2024', 'Friday, 29 March 2024', 'Thursday, 11 April 2024',
    'Wednesday, 17 April 2024', 'Wednesday, 01 May 2024', 'Monday, 20 May 2024',
    'Monday, 17 June 2024', 'Wednesday, 17 July 2024', 'Thursday, 15 August 2024',
    'Wednesday, 02 October 2024', 'Friday, 01 November 2024', 'Friday, 15 November 2024',
    'Wednesday, 25 December 2024'
]

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