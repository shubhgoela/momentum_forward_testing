from datetime import datetime

from bhavcopy_scrapper import scrap_data
from bhavcopy_utils import is_valid_date
from corp_actions import adjust_corp_actions


def update_daily_prices_and_adjust_corp_actions():
    date = datetime(year=2024, month=11, day=22)
    if is_valid_date(date):
        r = scrap_data(date)
        print('data addition complete.')
        r2 = adjust_corp_actions(date)
        print('corp actions adjustement complete.')
    else:
        print('date invalid')

update_daily_prices_and_adjust_corp_actions()