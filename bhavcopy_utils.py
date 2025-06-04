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
from email.message import EmailMessage
import mimetypes
import smtplib
from dotenv import find_dotenv, load_dotenv

from utils import month_abbreviations, abbreviation_to_month
from queries import get_holidays_for_year, get_exception_trading_dates_to_year

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT'))

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


def read_excel_file(file, logger):
    # Try different Excel engines
    df = []
    engines = ['openpyxl', 'xlrd']
    for engine in engines:
        try:
            df = pd.read_excel(file, engine=engine)
        except ValueError:
            continue
        except Exception as e:
            logger.warning(f"Error reading Excel with {engine} engine: {str(e)}")
            continue
    
    if df == []:
        logger.critical("Failed to read excel from all engined")
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

    exception_trading_dates = get_exception_trading_dates_to_year(date.year)

    if exception_trading_dates is None:
        exception_trading_dates = []
    else:
        exception_trading_dates = [d.date() for d in list(set(exception_trading_dates['dates']))]
    
    if any(d == date for d in exception_trading_dates):
        return True
    # Check if the date is a weekday and not a holiday
    is_weekday = calendar.day_abbr[date.weekday()] not in ['Sat', 'Sun']
    is_a_holiday =  any(d.date() == date for d in holiday_dates)

    return is_weekday and not is_a_holiday


def generate_html_table(template, data):
    """
    Generates an HTML table from a list of dictionaries, with support for clickable file download links.
    
    Parameters:
        data (list[dict]): A list of dictionaries where each dictionary represents a row. 
                           If a key is 'file_link', its value will be rendered as a clickable link.
    
    Returns:
        str: The generated HTML table as a string.
    """
    if not data:
        return "<p>No data available</p>"
    
    # Extract headers from the keys of the first dictionary
    headers = data[0].keys()
    
    # Start building the HTML table
    html = "<table border='1' style='border-collapse: collapse; width: 100%;'>"
    
    # Add table headers
    html += "<thead><tr>"
    for header in headers:
        html += f"<th style='padding: 8px; text-align: left;'>{header}</th>"
    html += "</tr></thead>"
    
    # Add table rows
    html += "<tbody>"
    for row in data:
        html += "<tr>"
        for header in headers:
            value = row.get(header, "")
            
            # Handle 'file_link' key by creating a download link
            if header == "file_link" and value:
                value = f"<a href='{value}' target='_blank' style='color: blue; text-decoration: none;'>Click to download</a>"
            
            html += f"<td style='padding: 8px;'>{value}</td>"
        html += "</tr>"
    html += "</tbody>"
    
    html += "</table>"

    if template is not None:
        body = template.get('body'," {{table}} ")
        body = body.replace("{{table}}", html)
    else:
        body = html
        
    return body


def send_email(
    recipient_emails, subject, body, 
    html_body=None, cc_emails=None, bcc_emails=None, 
    attachments=None, use_ssl=True
):
    """
    Sends an email with optional CC, BCC, HTML body, and attachments.
    
    Parameters:
        smtp_server (str): The SMTP server address.
        port (int): The port to connect to the SMTP server.
        sender_email (str): The sender's email address.
        sender_password (str): The sender's email account password.
        recipient_emails (list[str]): List of recipient email addresses.
        subject (str): The subject of the email.
        body (str): The plain text body content of the email.
        html_body (str, optional): HTML version of the email content. Defaults to None.
        cc_emails (list[str], optional): List of CC email addresses. Defaults to None.
        bcc_emails (list[str], optional): List of BCC email addresses. Defaults to None.
        attachments (list[str], optional): List of file paths to attach to the email. Defaults to None.
        use_ssl (bool): Whether to use SSL for the SMTP connection. Defaults to True.
    
    Raises:
        Exception: If the email fails to send for any reason.
    """
    try:
        # Create the email message
        msg = EmailMessage()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(recipient_emails)
        msg['Subject'] = subject
        
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)
        
        if bcc_emails:
            msg['Bcc'] = ', '.join(bcc_emails)
        
        # Email body: Add plain text and optional HTML content
        if html_body:
            msg.set_content(body)
            msg.add_alternative(f"<p>{html_body}<p>", subtype='html')
        else:
            msg.set_content(body)
        
        # Attach files if provided
        if attachments:
            for file_path in attachments:
                if os.path.exists(file_path):
                    ctype, encoding = mimetypes.guess_type(file_path)
                    ctype = ctype or 'application/octet-stream'
                    maintype, subtype = ctype.split('/', 1)
                    
                    with open(file_path, 'rb') as file:
                        file_data = file.read()
                        file_name = os.path.basename(file_path)
                        msg.add_attachment(file_data, maintype=maintype, subtype=subtype, filename=file_name)
                else:
                    print(f"Warning: File not found: {file_path}")
        
        # Prepare recipient list (includes BCC for sending but not visible in email headers)
        all_recipients = recipient_emails + (cc_emails or []) + (bcc_emails or [])
        
        # Connect to the SMTP server and send the email
        if use_ssl:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.send_message(msg, from_addr=SENDER_EMAIL, to_addrs=all_recipients)
        else:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.send_message(msg, from_addr=SENDER_EMAIL, to_addrs=all_recipients)
        
        print("Email sent successfully.")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False
    

def handle_file_response(response, date, logger):
    try:
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"Received content type: {content_type}")
        logger.info(f"Response status code: {response.status_code}")
        
        # Check for ZIP file
        if response.content.startswith(b'PK'):
            logger.info("Detected ZIP file in response")

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
                            df = read_excel_file(file, logger)
                    return df
                
                else:
                    shared_strings = get_shared_strings(zf)

                    worksheet_files = [f for f in  zf.namelist() if f.startswith('xl/worksheets/') and f.endswith('.xml')]

                    if not worksheet_files:
                        raise ValueError("No worksheet XML files found in the archive.")
                    
                    data = extract_data_from_sheet(zf, worksheet_files[0], shared_strings)

                    df = pd.DataFrame(data[1:], columns=data[0])
                    return df

            logger.warning(f"No suitable data file found in ZIP - {date}")
            return None
        
        # Check for Excel file
        elif b'workbook.xml' in response.content or b'spreadsheetml' in response.content:
            logger.info("Detected Excel file in response")
            df = pd.read_excel(io.BytesIO(response.content))
            return df
        
        # Check for CSV
        elif 'text/csv' in content_type:
            logger.info("Detected CSV content in response")
            df = pd.read_csv(io.StringIO(response.text))
            return df
        
        else:
            logger.critical(f"Content Type not detected: {date}")
            raise Exception(f"Content Type not detected: {date}")
        
    except Exception as e:
        raise Exception(str(e))
    

def get_next_valid_trading_datetime(current_datetime):
    next_date = current_datetime + timedelta(days=1)
    while not is_valid_date(next_date):
        next_date += timedelta(days=1)
    return next_date.replace(hour=16, minute=0, second=0, microsecond=0)