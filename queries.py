import os
from dotenv import find_dotenv, load_dotenv
from datetime import datetime
from pymongo import MongoClient

from enums import *

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

def get_adjusted_corp_actions(date):
    data = db['corp_action_adjusted'].find_one({'date': date})
    return data

def add_holiday_to_year(date):
    if isinstance(date, list):
        data = db['collection_holidays'].find_one_and_update({"year": date[0].year},  
                                                             {"$push": {"dates": {"$each": date}},
                                                             "$set": {'updated_on': datetime.now()}},
                                                             upsert=True,
                                                             return_document=True
                                                             )
    else:
        data = db['collection_holidays'].find_one_and_update({"year": date.year},  
                                                            {"$push": {"dates": date}, 
                                                            "$set": {'updated_on': datetime.now()}},
                                                            upsert=True,
                                                            return_document=True
                                                            )
    return data


def get_holidays_for_year(year):
    data = db['collection_holidays'].find_one({'year': year})
    return data


def get_mail_template(template_name):
    data = db['collection_mail_templates'].find_one({'template_name': template_name})
    return data


def add_order_to_ledger(order):
    data = db['collection_orders'].insert_one(order)
    return data.acknowledged


def get_pending_orders_by_date(date, strategy, order_type = 'ALL'):

    filter = {'order_placement_date': date, 'strategy_name': strategy, 'order_status': OrderStatus.PENDING.value}
    if order_type == OrderType.BUY.value or order_type == OrderType.SELL.value:
        filter['order_type'] = order_type

    data = list(db['collection_orders'].find(filter))
    return data


def update_cash_component_in_portfolio_document(strategy_name, year, month, to_update_key, to_update_value):
    db[strategy_name].find_one_and_update(
        {'year': year, 'month': month},
        {"$set": {f"df.{to_update_key}": to_update_value}}
    )
    return

def update_order_in_ledger(order_id, to_update):
    db['collection_orders'].find_one_and_update({'order_id': order_id}, {'$set': to_update})
    return


def update_price_in_portfolio(strategy_name, year, month, stock, price_type, price):
    db[strategy_name].find_one_and_update(
    {'year': year, 'month': month}, 
    {"$set": {f"df.portfolio.$[stock].{price_type}": price}}, 
    array_filters=[{"stock.stock": stock}] 
)

def update_quantity_in_portfolio(strategy_name, year, month, stock, quantity):
    db[strategy_name].find_one_and_update(
    {'year': year, 'month': month},  # Match the correct document
    {"$set": {f"df.portfolio.$[stock].quantity": quantity}},  # Update final_price for the matched stock
    array_filters=[{"stock.stock": stock}]  # Filter the correct stock in the portfolio array
)
