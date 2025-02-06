from dotenv import find_dotenv, load_dotenv
from datetime import datetime, time
import threading
import math

from indicators import *
from utils import *
from queries import *
from enums import *
from fetch_prices import *

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

STRATEGIES = ['V1', 'V2', 'V3','V4']
INDEX_LIST = os.getenv('INDEX_LIST').split(',')

def execute_order():
    today = datetime(year=2025, month=2, day=1)
    # today = datetime.now() 

    if not is_first_trading_day_of_month(today):
        return
    
    year = today.year
    month = today.month

    last_portfolio_year = year - 1 if month == 1 else year
    last_portfolio_month = 12 if month == 1 else month - 1

    order_placement_datetime = datetime.combine(today.date(), time(hour=10, minute=0, second=0))

    # prices_df = pd.read_excel('/Users/shubhgoela/Downloads/stock_carry (1).xlsx', sheet_name='stock_carry')
    for strategy in STRATEGIES:
        for index in INDEX_LIST:
            total_cash = 0
            cash_utilised = 0
            strategy_name = f'{strategy}_{index}'

            print('strategy_name : ', strategy_name)
            cash_balance = get_cash_balance(strategy_name,last_portfolio_year, last_portfolio_month)
            sell_orders = get_pending_orders_by_date(date=today, strategy=strategy_name, order_type= OrderType.SELL.value)
            buy_orders = get_pending_orders_by_date(date=today, strategy=strategy_name, order_type= OrderType.BUY.value)

            print('sell order len: ', len(sell_orders))
            print('buy order len: ', len(buy_orders))

            if len(sell_orders) > 0:
                for order in sell_orders:
                    stock = order['stock']
                    price = get_stock_price(symbol=stock, target_datetime=order_placement_datetime)
                    # price = get_stock_price(prices_df,stock)
                    order_quantity = order['order_quantity']
                    total_cash += (round(price,2) * round(order_quantity))
                    thread = threading.Thread(target=place_order, args=(strategy_name, stock, OrderType.SELL.value, 'at_market', order_quantity, order, price))
                    thread.start()

            
            perform_cash_operations(strategy_name, last_portfolio_year, last_portfolio_month, 'recovered_cash', total_cash)

            total_cash = round(total_cash, 2) + cash_balance

            perform_cash_operations(strategy_name, year, month, 'cash', total_cash)

            if len(buy_orders)>0:
                cash_for_each_script = total_cash/len(buy_orders)
                for order in buy_orders:
                    stock = order['stock']
                    price = get_stock_price(symbol=stock, target_datetime=order_placement_datetime)
                    # price = get_stock_price(prices_df,stock)
                    order_quantity = math.floor(cash_for_each_script/price)
                    cash_utilised += (price*order_quantity)
                    thread = threading.Thread(target=place_order, args=(strategy_name, stock, OrderType.BUY.value, 'at_market', order_quantity, order, price))
                    thread.start()
                
            cash_balance = total_cash - cash_utilised
            perform_cash_operations(strategy_name, year, month, 'cash_balance', cash_balance)


def place_order(strategy_name, stock, order_type, price_mode, order_quantity, order_metadata, execution_price):
    print(f'placed order for {stock}, order type: {order_type}, price_mode: {price_mode}, execution_price: {execution_price} quantity: {order_quantity}')
    order_metadata['order_quantity'] = order_quantity
    order_metadata['price_mode'] = price_mode
    order_metadata['execution_price'] = round(execution_price,2)
    order_metadata['order_status'] = OrderStatus.EXECUTED.value
    order_metadata['order_placed_on'] = datetime.now()
    update_order_in_ledger(order_id=order_metadata['order_id'], to_update=order_metadata)
    
    price_type = None
    if order_type == OrderType.SELL.value:
        price_type = 'final_price'
    else:
        price_type = 'initial_price'
    
    update_price_in_portfolio(strategy_name=strategy_name,
                                year=order_metadata['year'],
                                month=order_metadata['month'],
                                stock=stock,
                                price_type=price_type,
                                price=round(execution_price, 2))

    update_quantity_in_portfolio(strategy_name=strategy_name,
                                year=order_metadata['year'],
                                month=order_metadata['month'],
                                stock=stock,
                                quantity=order_quantity)


def perform_cash_operations(strategy_name, year, month, type, cash_amount):
    update_cash_component_in_portfolio_document(strategy_name, year, month, type, cash_amount)

execute_order()