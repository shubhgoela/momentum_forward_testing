from dotenv import find_dotenv, load_dotenv
import os
from datetime import datetime, timedelta
import traceback

from monthly_portfolio_builder import get_month_portfolio
from utils import is_first_trading_day_of_month, TOP_N_STOCKS, load_and_set_data, get_filtered_data_based_on_index
from queries import fetch_portfolio, save_portfolio
from monthly_orders import create_orders

dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

INDEX_LIST = os.getenv('INDEX_LIST').split(',')
STRATEGY = 'V2'


def create_portfolio():
    try:
        today = datetime(year=2025, month=6, day=2)
        # today = datetime.now()
        if is_first_trading_day_of_month(today):
            year = today.year
            month = today.month

            last_portfolio_year = year - 1 if month == 1 else year
            last_portfolio_month = 12 if month == 1 else month - 1
            # print(year, month, last_portfolio_year, last_portfolio_month, INDEX_LIST)

            data = load_and_set_data(file_path=f"NSE_PRICE_DATA.csv", data_type='PRICE')
            volumes = load_and_set_data(file_path=f"NSE_VOLUME_DATA.csv", data_type='VOLUME')

            for index in INDEX_LIST:
                db_collection_name = f'{STRATEGY}_{index}'
                filtered_data = get_filtered_data_based_on_index(data=data, index=index)
                filtered_volumes = get_filtered_data_based_on_index(data=volumes, index=index)

                last_month_df = fetch_portfolio(collection_name=db_collection_name, 
                                                year= last_portfolio_year,
                                                month= last_portfolio_month)

                this_month_portfolio = get_month_portfolio(data= filtered_data,
                                                            volumes= filtered_volumes,
                                                            stock_num= TOP_N_STOCKS[index],
                                                            lookback_months= 12, 
                                                            sorting_criteria= 'm_score',
                                                            absolute= False,
                                                            price_tracking_enabled= False,
                                                            stop_loss= 0,
                                                            last_month_df= last_month_df,
                                                            year= year,
                                                            month= month,
                                                            db_collection_name = db_collection_name
                                                            )


                acknowledged = save_portfolio(collection_name=db_collection_name,
                                            portfolio= this_month_portfolio)

                if not acknowledged:
                    raise Exception(f'Error storing month portfolio, year: {year}, month: {month}, index: {index}')

                create_orders(strategy_version=STRATEGY, index = index, collection_name = db_collection_name, month_portfolio=this_month_portfolio)

            return True
    
    except Exception as e:
        traceback.print_exc()
        print('error: ', str(e))


create_portfolio()