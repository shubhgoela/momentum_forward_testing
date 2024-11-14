from dotenv import find_dotenv, load_dotenv
from datetime import datetime, timedelta
import os
import re
import pandas as pd

from NSE_Selenium_login import get_data_with_selenium_nse_api

from queries import save_corp_action, save_all_corp_action, get_adjusted_corp_actions

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

NSE_base_url = os.getenv('NSE_base_url')
INDEX_LIST = os.getenv('INDEX_LIST').split(',')

def read_data(csv):
    df = pd.read_csv(csv, parse_dates=['Date'])
    df = df.loc[:, ~(df.columns.str.contains('^Unnamed') | df.columns.isnull())].copy()
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)
    return df

def filter_and_enrich_json(json_data):
    # Regular expression to detect patterns like "X:Y" (e.g., "1:1", "2:1")
    ratio_pattern = re.compile(r'(\d+\s*:\s*\d+)')
    face_value_split_pattern_1 = re.compile(r'From\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share\s*To\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share')
    face_value_split_pattern_2 = re.compile(r'From\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share\s*To\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share')
    face_value_split_pattern_3 = re.compile(r'From\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share\s*To\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share')
    face_value_split_pattern_4 = re.compile(r'From\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s*To\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s')
    face_value_split_pattern_5 = re.compile(r'From\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s*To\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s')
    face_value_split_pattern_6 = re.compile(r'From\s+Rs\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s*To\s+Re\s*(\d+)\s*/?-*\s*Per\s*Share\sUnit\s')
    face_value_split_pattern_7 = re.compile(r'Rs\.(\d+)\s*/-\s*To\s*Re\.(\d+)\s*/-\s*Per\s*Share')
    dates = []
    enriched_data = []

    for record in json_data:
        try:
            date_obj = datetime.strptime(record['exDate'], "%d-%b-%Y")
        except Exception as e:
            print(e)
            continue

        record['exDate'] = date_obj.strftime("%Y-%m-%d")
        subject = record['subject']

        if "Face Value Split" in subject:
            match = face_value_split_pattern_1.search(subject)
            if not match:
                match = face_value_split_pattern_2.search(subject)
                if not match:
                    match = face_value_split_pattern_3.search(subject)
                    if not match:
                        match = face_value_split_pattern_4.search(subject)
                        if not match:
                            match = face_value_split_pattern_5.search(subject)
                            if not match:
                                match = face_value_split_pattern_6.search(subject)
                                if not match:
                                    match = face_value_split_pattern_7.search(subject)
            if match:
                from_value = int(match.group(1))
                to_value = int(match.group(2))
                ratio = f"1:{from_value // to_value}" 

                # Add ratio and div_value
                record['ratio'] = ratio
                record['div_value'] = from_value / to_value  
                enriched_data.append(record)

        elif "Bonus" in subject:
            match = ratio_pattern.search(subject)
            if match:
                ratio = match.group(0)
                x, y = map(int, ratio.split(":"))
                # Add ratio and div_value for bonus
                record['ratio'] = ratio
                record['div_value'] = (x + y) / y 

                enriched_data.append(record)
                dates.append(record['exDate'])
                    
            # elif "Rights" in subject:
            #     enriched_data.append(record)
        
    return enriched_data, dates


def get_corp_actions(date = datetime.now()):
    try:
        date = date.date().strftime('%d-%m-%Y')
        print(date)
        endpoint = f'/api/corporates-corporateActions?index=equities&from_date={date}&to_date={date}'
        all_corp_actions = get_data_with_selenium_nse_api(NSE_base_url, endpoint)
        corp_actions, dates = filter_and_enrich_json(all_corp_actions)
        all_corp_actions = {
            'created_on': datetime.now(),
            'date': datetime.strptime(date, '%d-%m-%Y'),
            'actions': all_corp_actions
        }

        corp_actions = {
            'created_on': datetime.now(),
            'date': datetime.strptime(date, '%d-%m-%Y'),
            'actions': corp_actions
        }

        return all_corp_actions, corp_actions
    except Exception as e:
        print('####Exception')
        print(e)
        return None, None


def adjust_price_and_volumes(corp_actions):
    try:
        index = 'NSE'
        pd_path = os.getenv(f'{index}_PRICE_DATA')
        vd_path = os.getenv(f'{index}_VOLUME_DATA')
        print(pd_path, vd_path)
        pdf = read_data(pd_path)
        vdf = read_data(vd_path)
        columns = list(pdf.columns)
        for action in corp_actions['actions']:
            stock = action['symbol']
            date = action['exDate']
            div_value = action['div_value']

            if stock in columns:
                pdf.loc[pdf['Date'] < date, stock] = pdf.loc[pdf['Date'] < date, stock] / div_value
                vdf.loc[vdf['Date'] < date, stock] = (vdf.loc[vdf['Date'] < date, stock] * div_value).round(0)

            pdf.to_csv(pd_path, index=False)
            vdf.to_csv(vd_path, index=False)
        
        return True
    except Exception as e:
        print(str(e))
           
def adjust_corp_actions(date = datetime.now()):

    adjusted_actions = get_adjusted_corp_actions(date)

    if adjusted_actions is not None:
        print('Corp actions already adjusted.')
        return True
    
    all_corp_actions, corp_actions = get_corp_actions(date)
    save_all_corp_action(all_corp_actions)
    print(len(corp_actions['actions']))
    if len(corp_actions['actions']) > 0:
        adjust_price_and_volumes(corp_actions)
        save_corp_action(corp_actions)
    else:
        print('No corporate actions found')

    return True

