import pandas as pd
from dotenv import find_dotenv, load_dotenv
import os

from queries import get_index_constituents, update_index_constituents

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

INDEX_LIST = os.getenv('INDEX_LIST').split(',')

def merge_col(new_name, old_name):
    
    data = pd.read_csv("NSE_PRICE_DATA.csv")
    volumes = pd.read_csv("NSE_VOLUME_DATA.csv")
    
    if new_name not in data.columns:
        raise Exception(f'{new_name} does not exists in NSE Data')
    
    if old_name not in data.columns:
        raise Exception(f'{old_name} does not exists in NSE Data')
    
    data[new_name] = data[old_name].combine_first(data[new_name])

    data.drop(columns=[old_name], inplace=True)

    volumes[new_name] = volumes[old_name].combine_first(volumes[new_name])

    volumes.drop(columns=[old_name], inplace=True)

    data.to_csv("NSE_PRICE_DATA.csv", index=False)
    volumes.to_csv("NSE_VOLUME_DATA.csv", index=False)

    for index in INDEX_LIST:
        scrips = get_index_constituents(index)
        if old_name in scrips:
            old_name_index = scrips.index(old_name)
            scrips[old_name_index] = new_name

            data = {
                'scrip_list': scrips
            }

            update_index_constituents(index=index, update=data)

    return True

# merge_col(new_name= 'ABREL', old_name='CENTURYTEX')