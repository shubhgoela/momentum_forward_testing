import os
from dotenv import find_dotenv, load_dotenv
from datetime import datetime

from pymongo import MongoClient

dotenv_path = find_dotenv()
load_dotenv(dotenv_path=dotenv_path, override=True)

DB_URI = os.getenv('DB_URI')
DB_NAME = os.getenv('DB_NAME')

client = MongoClient(DB_URI)

db = client[DB_NAME]


def fetch_portfolio(collection_name, year, month):
    data = db[collection_name].find_one({"year": year, "month": month})
    return data

def save_portfolio(collection_name, portfolio):
    data = db[collection_name].insert_one(portfolio)
    return data.acknowledged

def save_all_corp_action(action):
    data = db['collection_corp_action'].insert_one(action)
    return data.acknowledged

def save_corp_action(action):
    data = db['corp_action_adjusted'].insert_one(action)
    return data.acknowledged

def get_index_constituents(index):
    data = db['collection_index_constituents'].find_one({'index': index})
    return data['scrip_list']

def update_index_constituents(index, update):
    update['updated_on'] = datetime.now()
    db['collection_index_constituents'].find_one_and_update(
            {"index": index},
            {"$set": update}
        )
    return True