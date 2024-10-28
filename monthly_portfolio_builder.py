from functools import partial

from indicators import *
from utils import *


def get_month_portfolio(price_csv, volume_csv, 
                        stock_num,
                        lookback_months, 
                        sorting_criteria, absolute,
                        price_tracking_enabled, stop_loss,
                        start_year,
                        year, month):
    
    data = load_and_set_data(file_path=f"{price_csv}", data_type='PRICE')
    volumes = load_and_set_data(file_path=f"{volume_csv}", data_type='VOLUME')
    data, volumes, dates = check_dataframes(prices_df=data, volumes_df=volumes)
    
    dates = sort_dates(dates)

    ema_200 = calculate_ema(data = data, dates=dates,timeframe=200)
    ema = [ema_200]

    ttm_returns = calculate_ttm(data, dates,start_year, year, month, lookback_months)


    if sorting_criteria == 'm_score':
        daily_change = calculate_daily_change(data,dates)
        m_score = calculate_m_score(ttm_returns, daily_change, lookback_months, absolute)
    else:
        m_score = []
    
    
    sort_function = partial(get_scripts_sorted, 
                            ttm_returns=ttm_returns, m_score=m_score, 
                            sorting_criteria  = sorting_criteria)

    return_calculations = partial(calculate_month_returns,
                                  data = data, dates = dates, 
                                  price_tracking_enabled = price_tracking_enabled, sl = stop_loss*-1)
    
    monthly_returns = {}
    
    month_wise_returns = process_monthly_portfolio(data, volumes, 
                                                   dates, year, month, start_year, 
                                                   ema, sort_function, stock_num, monthly_returns, 
                                                   (price_above_ema, price_above_52WKH, volume_check), 
                                                   return_calculations)

    portfolio = {
        'year': year,
        'month': month,
        'df': month_wise_returns,
        'stock_num': stock_num
        }

    return portfolio