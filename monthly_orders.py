from datetime import datetime
import uuid


from indicators import *
from utils import *
from queries import *
from enums import *


def create_orders(strategy_version, index, collection_name,  month_portfolio):

    buy_order = month_portfolio['df']['buy_order']
    sell_order = month_portfolio['df']['sell_order']
    year = month_portfolio['year']
    month = month_portfolio['month']

    last_portfolio_year = year - 1 if month == 1 else year
    last_portfolio_month = 12 if month == 1 else month - 1

    date = get_first_trading_date(year=year, month=month)
    date = datetime.combine(date, datetime.min.time())

    sell_order_metadata = {'order_type' : OrderType.SELL.value, 
                           'order_status': OrderStatus.PENDING.value,
                           'order_placement_date': date, 
                           'strategy': strategy_version, 
                           'created_on': datetime.now(),
                           'index': index,
                           'collection_name': collection_name,
                           'strategy_name': collection_name,
                           'year': last_portfolio_year,
                           'month': last_portfolio_month
                           }
    
    buy_order_metadata = {'order_type' : OrderType.BUY.value, 
                          'order_status': OrderStatus.PENDING.value,
                           'order_placement_date': date, 
                           'strategy': strategy_version, 
                           'created_on': datetime.now(),
                           'index': index,
                           'collection_name': collection_name,
                           'strategy_name': collection_name,
                           'year': year,
                           'month': month
                           }
    
    for order in sell_order:
        order = {**sell_order_metadata, **order}
        order['order_id'] = str(uuid.uuid4())
        add_order_to_ledger(order)

    for order in buy_order:
        order = {**buy_order_metadata, **order}
        order['order_id'] = str(uuid.uuid4())
        add_order_to_ledger(order)

    return True
