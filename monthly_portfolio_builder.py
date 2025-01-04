from functools import partial

from indicators import *
from utils import *


def get_month_portfolio(data, volumes,
                        stock_num,
                        lookback_months, 
                        sorting_criteria, absolute,
                        price_tracking_enabled, stop_loss,
                        last_month_df,
                        year, month, 
                        db_collection_name):
    
    data, volumes, dates = check_dataframes(prices_df=data, volumes_df=volumes)
    
    dates = sort_dates(dates)

    ema_200 = calculate_ema(data = data, dates=dates,timeframe=200)

    ema = [ema_200]

    sorting_score = calculate_ttm(data, dates, year, month, lookback_months)

    if sorting_criteria == 'm_score':
        daily_change = calculate_daily_change(data,dates)
        sorting_score = calculate_m_score(sorting_score, daily_change, lookback_months, absolute)

    if sorting_criteria == 'c_score':
        daily_change = calculate_daily_change(data,dates)
        sorting_score = calculate_coefficient_of_variation(sorting_score, daily_change, lookback_months, absolute)
    
    sort_function = partial(get_scripts_sorted, sorting_score = sorting_score)

    return_calculations = partial(update_stock_list,
                                  data = data, dates = dates, 
                                  db_collection_name = db_collection_name,
                                  price_tracking_enabled = price_tracking_enabled, sl = stop_loss*-1)
    
    
    month_wise_returns = process_monthly_portfolio(data, volumes, 
                                                   dates, year, month,
                                                   ema, sort_function, stock_num, last_month_df, 
                                                   (price_above_ema, price_above_52WKH, volume_check), 
                                                   return_calculations)


    portfolio = {
        'year': year,
        'month': month,
        'df': month_wise_returns,
        'stock_num': stock_num,
        'created_on': datetime.now()
        }

    return portfolio