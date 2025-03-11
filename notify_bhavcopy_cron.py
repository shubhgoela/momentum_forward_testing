from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from requests.exceptions import RequestException
import time
import random
from dotenv import find_dotenv, load_dotenv
import os
import time
from datetime import datetime
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import subprocess
import re
import pandas as pd
import numpy as np
import base64
import time
from PIL import Image
import io
import json


from bhavcopy_login import login
from logging_config import setup_logging
from bhavcopy_utils import generate_html_table, send_email, handle_file_response
from queries import get_mail_template


dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

logger = setup_logging(logger_name='noti_logger',
                       info_file='noti_logger_info.log', 
                        warning_file='noti_logger_warning.log', 
                        error_file='noti_logger_error.log', 
                        environment="DEBUG")

NSE_base_url = os.getenv('NSE_base_url')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT'))
BHAVCOPY_FILE_NAMES = ['F&O-UDiFF Common Bhavcopy Final (zip)', 'F&O-Participant wise Open Interest (csv)', 'Full Bhavcopy and Security Deliverable data']
MAIL_RECIPIENTS = ['shubh.goela@mnclgroup.com', 'ketan.kaushik@mnclgroup.com', 'ankush.jain1@mnclgroup.com', 'mayank.jain@mnclgroup.com', 'jainankush4u@gmail.com']
MAIL_RECIPIENTS_REPORT = ['shubh.goela@mnclgroup.com', 'ketan.kaushik@mnclgroup.com', 'ankush.jain1@mnclgroup.com', 'mayank.jain@mnclgroup.com', 'jainankush4u@gmail.com', 'amit.jain1@mnclgroup.com']


def run_terminal_command(command):
    """
    Runs a terminal command and returns the result.
    """
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        logger.info(f"Command succeeded: {command}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}. Error: {e.stderr}")
        return None


def update_chromedriver():
    """
    This function forces an update of the ChromeDriver to the latest version.
    """
    try:
        logger.info("Updating ChromeDriver...")
        # Run the webdriver-manager to install the latest ChromeDriver
        run_terminal_command("webdriver-manager update")
        logger.info("ChromeDriver updated successfully.")
    except Exception as e:
        logger.error(f"Failed to update ChromeDriver: {e}")


def clean_user_temp():
    """
    Clean user-specific temporary directories without needing sudo.
    """
    try:
        # Clean user cache directories
        user_cache_dirs = [
            "~/Library/Caches",
            "~/.cache",
            "/tmp"
        ]
        for directory in user_cache_dirs:
            command = f"rm -rf {directory}/*"
            run_terminal_command(command)
            logger.info(f"Cleaned {directory}")
    except Exception as e:
        logger.error(f"Error cleaning user temp directories: {e}")


def get_display_and_file_names(base_url, endpoint, section_id="cr_deriv_equity_daily_Current", max_retries=3, scroll_pause_time=2):
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--headless')  # Uncomment for headless operation
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    driver = None
    results = []
    for attempt in range(max_retries):
        try:
            full_url = base_url + endpoint
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.get(full_url)
            time.sleep(7)  # Allow initial content load
            
            # Progressive scrolling
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time + random.uniform(1, 3))  # Random delay for bot detection evasion
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    driver.execute_script("window.scrollTo(document.body.scrollHeight, 0);")
                    break
                last_height = new_height

            # Wait for the section to load
            section = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, section_id)))

            # Extract display names and file names
            report_elements = section.find_elements(By.CLASS_NAME, "reportsDownload")
            
            if report_elements == []:
                if driver:
                    driver.quit()
                print(f"Element not found on attempt {attempt + 1}. Retrying...")
                continue
            else:
                for element in report_elements:
                    d = {}
                    display_name = element.find_element(By.TAG_NAME, "label").text.strip().replace("\n", "")
                    file_name = element.find_element(By.CLASS_NAME, "reportCardSegment").text.strip()
                    file_link = element.get_attribute("data-link")  # Extract the download link
                    d = {'display_name': display_name,
                        'file_name': file_name,
                        'file_link': file_link}
                    results.append(d)

                return results

        except TimeoutException:
            logger.error(f"Timeout on attempt {attempt + 1}. Retrying...")
            run_terminal_command("pkill -f chromedriver")
        except NoSuchElementException:
            logger.error(f"Element not found on attempt {attempt + 1}. Retrying...")
            try:
                run_terminal_command("rm -rf ~/.caches/google-chrome/Default/Cache/*")
            except Exception as e:
                continue
        except (WebDriverException, RequestException) as e:
            logger.error(f"WebDriverException: {e} - Retrying... (Attempt {attempt+1})")
            run_terminal_command("pkill -f chromedriver")
            update_chromedriver()
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            clean_user_temp()

        finally:
            if driver:
                driver.quit()

        time.sleep(10 + random.uniform(1, 5))  # Randomized delay before retry

    print("All attempts failed")
    return results


def check_for_files( eq_section_id= "cr_equity_daily_Current", der_section_id="cr_deriv_equity_daily_Current"):

    filtered_docs = []
    print(eq_section_id,',',der_section_id)
    logger.info('getting files from derivatives section...')
    # cr_deriv_equity_daily_Previous, cr_deriv_equity_daily_Current
    derivative_file_details = get_display_and_file_names(NSE_base_url, endpoint = '/all-reports-derivatives', section_id = der_section_id)
    if derivative_file_details != []:
        for file in derivative_file_details:
            if file['display_name'] in BHAVCOPY_FILE_NAMES:
                filtered_docs.append(file)
    
    logger.info('getting files from capital markets section...')
    # cr_equity_daily_Previous, cr_equity_daily_Current
    equity_file_details = get_display_and_file_names(NSE_base_url, endpoint = '/all-reports', section_id= eq_section_id)
    if equity_file_details != []:
        for file in equity_file_details:
            if file['display_name'] in BHAVCOPY_FILE_NAMES:
                filtered_docs.append(file)
    
    file_names = [file.get('display_name') for file in filtered_docs]

    return file_names, filtered_docs


def process_filter_docs_for_noti(filtered_docs, template_name = 'bhavcopy_noti', sent_files = []):
    print('in process_filter_docs_for_noti')
    print(sent_files)
    print(filtered_docs)
    if len(filtered_docs) > 0:
        for file in filtered_docs:
            print('generating html...')
            template = get_mail_template(template_name)
            template = None
            body = generate_html_table(template, [file])

            if file.get('display_name') not in sent_files:
                # file_names.append(file.get('display_name'))
                if template is None:
                    mail_sent = send_email( 
                        recipient_emails=MAIL_RECIPIENTS,
                        # recipient_emails= ['shubh.goela@mnclgroup.com'],
                        subject=file.get('display_name'),
                        body=body,
                        html_body=body)  
                else:  
                    mail_sent = send_email( 
                        recipient_emails=template.get('recipients', MAIL_RECIPIENTS),
                        subject=file.get('display_name'),
                        body=body,
                        html_body=body)
    return True


def extract_date_from_csv(text, year):
    match_1 = re.search(r'([A-Za-z]+)\s(\d{1,2})', text)
    if match_1:
        month = match_1.group(1)
        day = match_1.group(2)
    
    match_2 = re.search(r'\d{4}', year)
    if match_2:
        year = int(match_2.group(0))  # Convert to integer
        
    date_str = f"{day} {month} {year}"
    date_obj = datetime.strptime(date_str, "%d %b %Y").strftime('%d/%m/%y')
    
    return date_obj


def process_open_interest_df(df):
    dt = extract_date_from_csv(df.columns[0], df.columns[1])
    df.dropna(axis=1, how='all', inplace=True)
    df.columns = [v.strip() for v in df.iloc[0]]
    df = df.loc[1:]
    for column in df.columns:
        if df[column].apply(pd.to_numeric, errors='coerce').notna().all():
            df[column] = pd.to_numeric(df[column], errors='coerce')
    
    return df, dt


def create_index_futures_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))
    merged_df["Future Index Long Change"] = merged_df[f"Future Index Long_{cr_dt}"] - merged_df[f"Future Index Long_{prev_dt}"]
    merged_df["Future Index Short Change"] = merged_df[f"Future Index Short_{cr_dt}"] - merged_df[f"Future Index Short_{prev_dt}"]
    merged_df['Signal'] = np.where(
    (merged_df['Future Index Long Change'] - merged_df['Future Index Short Change']) > 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Future Index Long_{cr_dt}", f"Future Index Short_{cr_dt}",  # Current values
        f"Future Index Long_{prev_dt}", f"Future Index Short_{prev_dt}", 
        "Future Index Long Change", "Future Index Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    v1 = result_df.loc[result_df['Client Type'] == 'FII',f'Future Index Long_{cr_dt}'].values[0]
    v2 = result_df.loc[result_df['Client Type'] == 'FII',f'Future Index Short_{cr_dt}'].values[0]
    long_exposure = round(((v1/(v1+v2))*100),2)
    return result_df , long_exposure 


def create_index_call_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))

    merged_df["Option Index Call Long Change"] = merged_df[f"Option Index Call Long_{cr_dt}"] - merged_df[f"Option Index Call Long_{prev_dt}"]
    merged_df["Option Index Call Short Change"] = merged_df[f"Option Index Call Short_{cr_dt}"] - merged_df[f"Option Index Call Short_{prev_dt}"]

    merged_df['Signal'] = np.where(
    (merged_df['Option Index Call Long Change'] - merged_df['Option Index Call Short Change']) > 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Option Index Call Long_{cr_dt}", f"Option Index Call Short_{cr_dt}",  # Current values
        f"Option Index Call Long_{prev_dt}", f"Option Index Call Short_{prev_dt}", 
        "Option Index Call Long Change", "Option Index Call Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    return result_df


def create_index_put_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))

    merged_df["Option Index Put Long Change"] = merged_df[f"Option Index Put Long_{cr_dt}"] - merged_df[f"Option Index Put Long_{prev_dt}"]
    merged_df["Option Index Put Short Change"] = merged_df[f"Option Index Put Short_{cr_dt}"] - merged_df[f"Option Index Put Short_{prev_dt}"]

    merged_df['Signal'] = np.where(
    (merged_df['Option Index Put Long Change'] - merged_df['Option Index Put Short Change']) < 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Option Index Put Long_{cr_dt}", f"Option Index Put Short_{cr_dt}",  # Current values
        f"Option Index Put Long_{prev_dt}", f"Option Index Put Short_{prev_dt}", 
        "Option Index Put Long Change", "Option Index Put Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    return result_df


def create_stock_futures_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))
    merged_df["Future Stock Long Change"] = merged_df[f"Future Stock Long_{cr_dt}"] - merged_df[f"Future Stock Long_{prev_dt}"]
    merged_df["Future Stock Short Change"] = merged_df[f"Future Stock Short_{cr_dt}"] - merged_df[f"Future Stock Short_{prev_dt}"]
    merged_df['Signal'] = np.where(
    (merged_df['Future Stock Long Change'] - merged_df['Future Stock Short Change']) > 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Future Stock Long_{cr_dt}", f"Future Stock Short_{cr_dt}",  # Current values
        f"Future Stock Long_{prev_dt}", f"Future Stock Short_{prev_dt}", 
        "Future Stock Long Change", "Future Stock Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    v1 = result_df.loc[result_df['Client Type'] == 'FII',f'Future Stock Long_{cr_dt}'].values[0]
    v2 = result_df.loc[result_df['Client Type'] == 'FII',f'Future Stock Short_{cr_dt}'].values[0]
    long_exposure = round(((v1/(v1+v2))*100),2)
    return result_df , long_exposure 


def create_stock_call_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))

    merged_df["Option Stock Call Long Change"] = merged_df[f"Option Stock Call Long_{cr_dt}"] - merged_df[f"Option Stock Call Long_{prev_dt}"]
    merged_df["Option Stock Call Short Change"] = merged_df[f"Option Stock Call Short_{cr_dt}"] - merged_df[f"Option Stock Call Short_{prev_dt}"]

    merged_df['Signal'] = np.where(
    (merged_df['Option Stock Call Long Change'] - merged_df['Option Stock Call Short Change']) > 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Option Stock Call Long_{cr_dt}", f"Option Stock Call Short_{cr_dt}",  # Current values
        f"Option Stock Call Long_{prev_dt}", f"Option Stock Call Short_{prev_dt}", 
        "Option Stock Call Long Change", "Option Stock Call Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    return result_df


def create_stock_put_table(cr_df, prev_df, cr_dt, prev_dt):
    merged_df = cr_df.merge(prev_df, on="Client Type", suffixes=(f'_{cr_dt}', f'_{prev_dt}'))

    merged_df["Option Stock Put Long Change"] = merged_df[f"Option Stock Put Long_{cr_dt}"] - merged_df[f"Option Stock Put Long_{prev_dt}"]
    merged_df["Option Stock Put Short Change"] = merged_df[f"Option Stock Put Short_{cr_dt}"] - merged_df[f"Option Stock Put Short_{prev_dt}"]

    merged_df['Signal'] = np.where(
    (merged_df['Option Stock Put Long Change'] - merged_df['Option Stock Put Short Change']) < 0,
        'Bullish',
        'Bearish'
    )
    result_df = merged_df[[
        "Client Type", 
        f"Option Stock Put Long_{cr_dt}", f"Option Stock Put Short_{cr_dt}",  # Current values
        f"Option Stock Put Long_{prev_dt}", f"Option Stock Put Short_{prev_dt}", 
        "Option Stock Put Long Change", "Option Stock Put Short Change", 'Signal' # Daily change
    ]]
    result_df.iloc[-1, -1] = ''
    return result_df


def get_styled_html(df):
    def apply_styles(row):
        def style_cell(value, is_signal=False, is_currency=False):
            """Applies styles to table cells, formatting integer currency values and setting background colors for the 'Signal' column."""
            bg_color = ""
            if is_signal:
                if value == "Bullish":
                    bg_color = "background-color: rgb(106, 168, 79);"
                elif value == "Bearish":
                    bg_color = "background-color: rgb(224, 102, 102);"

            # Format integer currency values with commas
            if is_currency and isinstance(value, int):
                value = f"{value:,}"  # Format as integer with commas (e.g., 1000000 → 1,000,000)

            return f"<td style='border: 1px solid black; padding: 5px; text-align: center; {bg_color}'>{value}</td>"

        # Detect integer columns dynamically
        int_columns = row.index[row.map(lambda x: isinstance(x, int))].tolist()

        # Generate row with styles applied
        row_html = "".join(
            style_cell(value, is_signal=(col == "Signal"), is_currency=(col in int_columns))
            for col, value in zip(row.index, row)
        )

        return f"<tr>{row_html}</tr>"
    # Apply styles to each row
    rows = "".join(df.apply(apply_styles, axis=1))
    
    # Create the complete table with inline styles
    html_table = f"""
    <table style='width: auto; border-collapse: collapse; border: 1px solid black;'>
        <thead>
            <tr>
                {''.join(f"<th style='border: 1px solid black; padding: 5px; text-align: center;'>{col}</th>" for col in df.columns)}
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """
    return html_table


def create_html_for_exposure(index_long_exposure, stock_long_exposure):
    html = f'<tr><td style="vertical-align: middle; text-align: center; padding: 10px;"> FII Net Long Exposure - Index : {index_long_exposure}%</td><td style="vertical-align: middle; text-align: center; padding: 10px;"> FII Net Long Exposure - Stock : {stock_long_exposure}%</td></tr>'
    return html


def get_commentry(r1, index_long_exposure):
    filename = 'data.json'

    def save_file(index_long_exposure):
        data = {"index_long_exposure": index_long_exposure}
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)
        return

    def get_file():
        with open(filename, "r") as file:
            return json.load(file)
    
    commentry = []
    fii_long_change = r1.loc[r1['Client Type'] == 'FII','Future Index Long Change'].values[0]
    fii_short_change = r1.loc[r1['Client Type'] == 'FII','Future Index Short Change'].values[0]
    if (fii_long_change > 0 and fii_short_change > 0):
        if (fii_long_change > fii_short_change):
            commentry.append('Net long addition today.')
        elif (fii_long_change < fii_short_change):
            commentry.append('Net short addition today.')
        else:
            commentry.append('Equal long and short addition today.')

    elif (fii_long_change < 0 and fii_short_change < 0):
        if (fii_long_change > fii_short_change):
            commentry.append('Net short unwinding today.')
        elif (fii_long_change < fii_short_change):
            commentry.append('Net long unwinding today.')
        else:
            commentry.append('Equal long and short unwinding today.')

    else:
        if (fii_long_change > fii_short_change):
            commentry.append('Net long addition and short unwinding today.')
        elif (fii_long_change < fii_short_change):
            commentry.append('Net short addition and long unwinding today.')
        else:
            commentry.append('No change in net long and short today.')


    loaded_data = get_file()
    
    prev_index_long_exposure = loaded_data['index_long_exposure']
    if prev_index_long_exposure > index_long_exposure:
        commentry.append(f'Net long exposure decreases to {index_long_exposure}%.')
    elif prev_index_long_exposure < index_long_exposure:
        change = round(index_long_exposure - prev_index_long_exposure, 2)
        commentry.append(f'Net long exposure increases to {index_long_exposure}%.')
    else:
        commentry.append('No change in net long exposure.')

    save_file(index_long_exposure)

    html_commentry = ''
    for comment in commentry:
        html_commentry += f'<b>{comment}</b><br>'
    
    # html_commentry = f'<tr><td style="vertical-align: middle; text-align: left; padding: 10px;"> {html_commentry} </td><td style="vertical-align: middle; text-align: center; padding: 10px;"></td></tr>'
    html_commentry = f'<tr><td style="vertical-align: middle; text-align: left; padding: 10px; font-size: 16px; font-weight: bold;"> {html_commentry} </td><td style="vertical-align: middle; text-align: center; padding: 10px;"></td></tr>'

    return html_commentry


def create_html_table_with_predefined_html(df_dict_list, extra_table_data, commentry):
    # Ensure the list contains exactly 6 dictionaries
    if len(df_dict_list) != 6:
        raise ValueError("The list must contain exactly 6 dictionaries with 'heading' and 'df' keys.")
    
    # Initialize the table HTML string

    table_html = '<table border="1" style="border-collapse: collapse; width: auto; font-family: Arial, sans-serif;">'
    
    table_html += commentry
    # table_html += extra_table_data
    
    # Loop to create the table rows and columns
    for i in range(3):  # 3 rows
        table_html += '<tr>'
        for j in range(2):  # 2 columns
            index = i * 2 + j  # Calculate the dictionary index
            if index < len(df_dict_list):
                heading = df_dict_list[index]['heading']
                df_html = get_styled_html(df_dict_list[index]['df'])  # HTML string of the DataFrame
                
                # Create the table cell with the heading and DataFrame HTML
                # table_html += f'<td><strong style="text-align: center; display: block; width: 100%;">{heading}</strong><br>{df_html}</td>'
                table_html += f'''
                    <td style="vertical-align: middle; text-align: center; padding: 10px;">
                        <div style="background: lightblue;"><strong>{heading}</strong></div><br>{df_html}
                    </td>
                '''
        table_html += '</tr>'
    
    table_html += '</table>'
    
    return table_html


def save_html_as_png(html_string, output_file="output.png"):
    """Captures a full-page screenshot of an HTML string using Chrome DevTools Protocol (CDP)."""
    
    # Wrap HTML properly
    full_html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Test</title>
        <style>
            body {{ margin: 0; padding: 0; zoom: 1.0; }}
            ::-webkit-scrollbar {{
                width: 10px;
            }}
            ::-webkit-scrollbar-thumb {{
                background: gray;
                border-radius: 5px;
            }}
        </style>
    </head>
    <body>
        {html_string}
    </body>
    </html>
    """
    
    encoded_html = base64.b64encode(full_html.encode()).decode()
    data_url = f"data:text/html;base64,{encoded_html}"

    # Set up Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Enables full-page screenshot mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    driver.get(data_url)

    # Give the page time to render
    time.sleep(2)

    # Use Chrome DevTools Protocol for full-page screenshot
    screenshot = driver.execute_cdp_cmd("Page.captureScreenshot", {"format": "png", "captureBeyondViewport": True, "fromSurface": True})
    
    driver.quit()

    # Save the screenshot
    with open(output_file, "wb") as f:
        f.write(base64.b64decode(screenshot["data"]))

    print(f"✅ PNG saved as {output_file}")

    return


def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File {file_path} deleted successfully.")
    except FileNotFoundError:
        print(f"The file {file_path} does not exist.")
    except Exception as e:
        print(f"Error: {e}")
    
    return True


def loop_question_between_times(start_time="00:00", end_time="23:00", interval=60):
    """
    Continuously prompts a question between specified times.

    Parameters:
        question (str): The question to be asked in the loop.
        start_time (str): The start time in HH:MM format (24-hour format).
        end_time (str): The end time in HH:MM format (24-hour format).
        interval (int): The number of seconds to wait between each prompt.

    """
    today = datetime.now().strftime('%Y-%m-%d')
    start = datetime.strptime(start_time, "%H:%M").time()
    end = datetime.strptime(end_time, "%H:%M").time()
    print(start)
    print(end)
    files = []
    filtered_docs = []
    while True:
        now = datetime.now().time()
        # print('now: ', now)
        # print(files)
        if start <= now <= end and set(files) != set(BHAVCOPY_FILE_NAMES):
            f, fd = check_for_files( eq_section_id="cr_equity_daily_Current" , der_section_id="cr_deriv_equity_daily_Current")
            process_filter_docs_for_noti(filtered_docs = fd, sent_files=files)
            if f != []:
                files.extend(f)
                files = list(set(files))
                filtered_docs.extend(fd)
                filtered_docs = list({tuple(sorted(d.items())) for d in filtered_docs})
                filtered_docs = [dict(t) for t in filtered_docs]

            print('sleeping...')
            time.sleep(30)
        else:
            if now > end:
                print("The time window has closed. Exiting loop.")
            break
    
    
    session = login(NSE_base_url)
    MAX_RETRIES = 5  # Prevent infinite loops
    retry_count = 0

    current_df, prev_df = None, None

    while retry_count <= MAX_RETRIES:
        retry_count += 1
        for file in filtered_docs:
            if file['display_name'] == 'F&O-Participant wise Open Interest (csv)':
                response = session.get(file['file_link'])
                response.raise_for_status()
                if response.status_code == 200:
                    current_df = handle_file_response(response, today, logger)
                    current_df, current_date = process_open_interest_df(current_df)
                    break
        

        f, fd = check_for_files( eq_section_id="cr_equity_daily_Previous" , der_section_id="cr_deriv_equity_daily_Previous")
        for file in fd:
            if file['display_name'] == 'F&O-Participant wise Open Interest (csv)':
                response = session.get(file['file_link'])
                response.raise_for_status()
                if response.status_code == 200:
                    prev_df = handle_file_response(response, today, logger)
                    prev_df, prev_date = process_open_interest_df(prev_df)
                    break
        
        if current_df is not None and prev_df is not None:
            break


    print('########curren_df')
    print(current_df)
    print('########previous_df')
    print(prev_df)


    r1, index_long_exposure = create_index_futures_table(current_df, prev_df, current_date, prev_date)
    r2 = create_index_call_table(current_df, prev_df, current_date, prev_date)
    r3 = create_index_put_table(current_df, prev_df, current_date, prev_date)
    r4, stock_long_exposure = create_stock_futures_table(current_df, prev_df, current_date, prev_date)
    r5 = create_stock_call_table(current_df, prev_df, current_date, prev_date)
    r6 = create_stock_put_table(current_df, prev_df, current_date, prev_date)

    extra_table_data = create_html_for_exposure(index_long_exposure, stock_long_exposure)
    commentry = get_commentry(r1, index_long_exposure)
    html = create_html_table_with_predefined_html([{'heading': 'Index Futures', 'df': r1},
                                                {'heading': 'Stock Futures', 'df': r4},
                                                {'heading': 'Index Call Options', 'df': r2},
                                                {'heading': 'Stock Call Options', 'df': r5},
                                                {'heading': 'Index Put Options', 'df': r3},
                                                {'heading': 'Stock Put Options', 'df': r6}], extra_table_data, commentry)

    try:
        save_html_as_png(html_string=html)
    except Exception as e:
        print('Failed to save html as image')
        pass

    send_email( 
                recipient_emails=['shubh.goela@mnclgroup.com'],
                bcc_emails=MAIL_RECIPIENTS_REPORT,
                subject='Participant Wise Derivatives FII-DII Data',
                body=html,
                html_body=html,
                attachments=['output.png'])

    delete_file('output.png')

# Example usage:
loop_question_between_times(interval=300)
