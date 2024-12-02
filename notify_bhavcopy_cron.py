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

from logging_config import setup_logging
from bhavcopy_utils import generate_html_table, send_email
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
            run_terminal_command("rm -rf ~/Library/Caches/Google/Chrome/Default/Cache/*")
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


def check_for_files(template_name = 'bhavcopy_noti'):
    filtered_docs = []
    logger.info('getting files from derivatives section...')
    # cr_deriv_equity_daily_Previous, cr_deriv_equity_daily_Current
    derivative_file_details = get_display_and_file_names(NSE_base_url, endpoint = '/all-reports-derivatives', section_id="cr_deriv_equity_daily_Current")
    if derivative_file_details != []:
        for file in derivative_file_details:
            if file['display_name'] in ['F&O-UDiFF Common Bhavcopy Final (zip)', 'F&O-Participant wise Open Interest (csv)']:
                filtered_docs.append(file)
    
    logger.info('getting files from capital markets section...')
    # cr_equity_daily_Previous, cr_equity_daily_Current
    equity_file_details = get_display_and_file_names(NSE_base_url, endpoint = '/all-reports', section_id="cr_equity_daily_Current")
    if equity_file_details != []:
        for file in equity_file_details:
            if file['display_name'] in ['Full Bhavcopy and Security Deliverable data']:
                filtered_docs.append(file)
    
    file_names = []
    if len(filtered_docs) > 0:

        for file in filtered_docs:
            print('generating html...')
            template = get_mail_template(template_name)
            template = None
            body = generate_html_table(template, [file])
            file_names.append(file.get('display_name'))
            if template is None:
                mail_sent = send_email( 
                    recipient_emails=['shubh.goela@mnclgroup.com'],
                    subject=file.get('display_name'),
                    body=body,
                    html_body=body)  
            else:  
                mail_sent = send_email( 
                    recipient_emails=template.get('recipients', ['shubh.goela@mnclgroup.com', 'ketan.kaushik@mnclgroup.com', 'ankush.jain@mnclgroup.com', 'mayank.jain@mnclgroup.com']),
                    subject=file.get('display_name'),
                    body=body,
                    html_body=body)

    return file_names
    # if mail_sent:
    #     return


def loop_question_between_times(start_time="17:30", end_time="23:00", interval=60):
    """
    Continuously prompts a question between specified times.

    Parameters:
        question (str): The question to be asked in the loop.
        start_time (str): The start time in HH:MM format (24-hour format).
        end_time (str): The end time in HH:MM format (24-hour format).
        interval (int): The number of seconds to wait between each prompt.

    """
    start = datetime.strptime(start_time, "%H:%M").time()
    end = datetime.strptime(end_time, "%H:%M").time()
    print(start)
    print(end)
    files = []
    while True:
        now = datetime.now().time()
        print('now: ', now)
        # Check if the current time falls within the specified range
        if start <= now <= end:
            f = check_for_files()
            if f != []:
                files.extend(f)
                files = list(set(files))
        
        # Wait for the specified interval before asking again
        print('sleeping...')
        time.sleep(60)

        if files == ['F&O-UDiFF Common Bhavcopy Final (zip)', 'F&O-Participant wise Open Interest (csv)', 'Full Bhavcopy and Security Deliverable data']:
            break
        # Exit condition after the time window closes
        if now > end:
            print("The time window has closed. Exiting loop.")
            break

# Example usage:
loop_question_between_times(interval=300)
