from dotenv import find_dotenv, load_dotenv
import os
from datetime import datetime, timedelta

from monthly_portfolio_builder import get_month_portfolio
from utils import is_first_day_of_month, TOP_N_STOCKS, load_and_set_data, get_columns_for_index
from queries import fetch_portfolio, save_portfolio
dotenv_path = find_dotenv()

if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    print("No .env file found")

INDEX_LIST = os.getenv('INDEX_LIST').split(',')
STRATEGY = 'V3'


def create_portfolio():
    try:
        today = datetime(year=2024, month=12, day=1)
        if is_first_day_of_month(today):
            year = today.year
            month = today.month
            last_portfolio_year = (today-timedelta(days=1)).year
            last_portfolio_month = (today-timedelta(days=1)).month
            print(year, month, last_portfolio_year, last_portfolio_month, INDEX_LIST)
            
            data = load_and_set_data(file_path=f"NSE_PRICE_DATA.csv", data_type='PRICE')
            volumes = load_and_set_data(file_path=f"NSE_VOLUME_DATA.csv", data_type='VOLUME')

            for index in INDEX_LIST:
                db_collection_name = f'{STRATEGY}_{index}'
                filtered_col = get_columns_for_index(index)
                filtered_data = data[filtered_col]
                filtered_volumes = volumes[filtered_col]

                monthly_returns = fetch_portfolio(collection_name=db_collection_name, 
                                                year= last_portfolio_year,
                                                month= last_portfolio_month)

                this_month_portfolio = get_month_portfolio(data= filtered_data,
                                                           volumes= filtered_volumes,
                                                           stock_num= TOP_N_STOCKS[index],
                                                           lookback_months= 12, 
                                                           sorting_criteria= 'm_score',
                                                           absolute= True,
                                                           price_tracking_enabled= False,
                                                           stop_loss= 0,
                                                           monthly_returns= monthly_returns,
                                                           year= year,
                                                           month= month)

                acknowledged = save_portfolio(collection_name=db_collection_name,
                                            portfolio= this_month_portfolio)
                
                if not acknowledged:
                    raise Exception(f'Error storing month portfolio, year: {year}, month: {month}, index: {index}')
            
            return True
    
    except Exception as e:
        print('error: ', str(e))


create_portfolio()