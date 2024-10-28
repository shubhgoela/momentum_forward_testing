
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import calendar
import json
from dotenv import load_dotenv, find_dotenv
import os
import traceback
import traceback

dotenv_path = find_dotenv()
load_dotenv(dotenv_path=dotenv_path, override=True)
nifty_500_csv = os.getenv('nifty_500_data')



month_abbreviations = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }



def load_and_set_data(file_path, data_type='PRICE'):
    '''
    This function loads a CSV file, removes unnamed/blank columns, checks for the 'Date' column,
    processes the 'Date' column, and returns a DataFrame.

    Returns one pandas objects: a DataFrame.

    Parameters:
    - file_path: Path to the CSV file.
    '''
    try:
        data = pd.read_csv(file_path)
        # print("data columns on load: ", list(data.columns))
        # print("length data columns on load: ", len(list(data.columns)))
        # print("data rowc count: ", len(data))
        # Remove unnamed or blank columns
        data = data.loc[:, ~(data.columns.str.contains('^Unnamed') | data.columns.isnull())]
        data.dropna(axis=1, how='all', inplace=True)
        data.dropna(axis=0, how='all', inplace=True)
        # print("data rowc count after drop : ", len(data))
        # print("data columns after remove: ", list(data.columns))
        # print("length data columns after remove: ", len(list(data.columns)))
        # Check if 'Date' column exists
        if 'Date' not in data.columns:
            raise Exception("The 'Date' column is missing from the file.")
        else:
          if data.columns[0] != 'Date':
                data = data[['Date'] + [col for col in data.columns if col != 'Date']]
        
        data['Date'] = pd.to_datetime(data['Date'])
        # Convert 'Date' column to datetime if it's not already in the right format
        if not isinstance(data['Date'].iloc[0], pd.Timestamp):
            invalid_dates = []
            for index, value in enumerate(data['Date']):
                try:
                    data.at[index, 'Date'] = pd.to_datetime(value)
                except Exception:
                    invalid_dates.append(value)

            if invalid_dates:
                raise Exception(f"Invalid date values found: {invalid_dates}")
        
        data.sort_values(by='Date', ascending=True, inplace=True)
        # if data_type == 'PRICE':
        #     data = back_fill_stock_prices(data)
        data = data[~data['Date'].duplicated(keep='first')]
        print("Any duplicates in entire index:", data['Date'].duplicated().any())
        data = front_fill_stock_prices_new(data)
        return data

    except Exception as e:
        print("########Exception: ")
        traceback.print_exc()
        return None, None


def check_dataframes(prices_df, volumes_df):
    '''
    This function checks if the columns and date ranges of two DataFrames are equal.
    It raises an exception if the columns do not match and adjusts dates if there are discrepancies.

    Parameters:
    - prices_df: DataFrame containing price data.
    - volumes_df: DataFrame containing volume data.

    Raises:
    - Exception: If the columns do not match or if date adjustments are needed.
    '''

    # Check for column equality
    price_columns = set(prices_df.columns)
    volume_columns = set(volumes_df.columns)

    if price_columns != volume_columns:
        missing_in_prices = volume_columns - price_columns
        missing_in_volumes = price_columns - volume_columns
        raise Exception(f"Column mismatch: Missing in prices: {missing_in_prices}, Missing in volumes: {missing_in_volumes}")

    # Ensure 'Date' column is present in both DataFrames
    if 'Date' not in price_columns or 'Date' not in volume_columns:
        raise Exception("The 'Date' column is missing in one of the DataFrames.")

    # Check date ranges
    price_dates = prices_df['Date']
    volume_dates = volumes_df['Date']
    if not price_dates.equals(volume_dates):
        # Adjust the dates in both DataFrames to match
        common_dates = price_dates[price_dates.isin(volume_dates)]
        if common_dates.empty:
            raise Exception("No common dates found between the price and volume DataFrames.")
        
        prices_df = prices_df[prices_df['Date'].isin(common_dates)].reset_index(drop=True)
        volumes_df = volumes_df[volumes_df['Date'].isin(common_dates)].reset_index(drop=True)
    else:
      common_dates = price_dates
    return prices_df, volumes_df, common_dates


def front_fill_stock_prices_new(df):
    """
    Front-fill missing stock prices in the DataFrame up to March 28, 2024, starting from the first occurrence 
    of a non-zero value.

    Parameters:
    df (pd.DataFrame): DataFrame containing stock prices with dates as index and stock names as columns.
    
    Returns:
    pd.DataFrame: DataFrame with front-filled stock prices up to March 28, 2024.
    """
    # Ensure 'Date' column is set as index
    df.set_index('Date', inplace=True)

    # Define the date limit for forward-filling
    date_limit = pd.Timestamp('2024-03-28')

    # Replace 0 with NaN to handle forward fill correctly
    df = df.replace(0, pd.NA)

    # Apply forward-fill up to the specified date
    filled_df = df.apply(lambda x: x[:date_limit].ffill().combine_first(x))

    # Replace NaN back with 0 after filling
    filled_df = filled_df.fillna(0)

    # Reset index to have 'Date' as a column again
    filled_df.reset_index(inplace=True)
    return filled_df


def front_fill_stock_prices(df):
    """
    Front-fill missing stock prices in the DataFrame starting from the first occurrence of a non-zero value.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing stock prices with dates as index and stock names as columns.
    
    Returns:
    pd.DataFrame: DataFrame with front-filled stock prices.
    """
    df.set_index('Date', inplace=True)

    # Replace 0 with NaN to handle the forward fill correctly
    df = df.replace(0, pd.NA)
    
    # Forward fill the missing values
    filled_df = df.apply(lambda x: x.ffill())
    
    # Replace NaN back with 0 if needed after filling
    filled_df = filled_df.fillna(0)

    filled_df.reset_index(inplace=True)
    return filled_df


def back_fill_stock_prices(df):
    """
    Backfills NaN values in each column starting from the first valid occurrence of a number.
    NaN values before the first valid occurrence remain untouched.
    
    Parameters:
    df (pd.DataFrame): DataFrame with NaN values.
    
    Returns:
    pd.DataFrame: DataFrame with NaN values backfilled after the first valid value in each column.
    """
    df.set_index('Date', inplace=True)
    for col in df.columns:
        # Find the first valid (non-NaN) index
        first_valid_index = df[col].first_valid_index()
        if first_valid_index is not None:
            # Apply bfill only from the first valid index onward
            df.loc[first_valid_index:, col] = df.loc[first_valid_index:, col].bfill()
    filled_df = df.fillna(0)
    filled_df.reset_index(inplace=True)
    return filled_df


def get_period_dates(year, month):
    start_date = pd.Period(year=year, month=month, freq='M').start_time
    end_date = pd.Period(year=year, month=month, freq='M').end_time
    return start_date, end_date


def filter_dates_by_month(dates_sorted, start_date, end_date):
    return dates_sorted[(dates_sorted >= start_date) & (dates_sorted <= end_date)]


def get_trading_dates(dates_sorted, year, month, start_year):
    filtered_dates = filter_dates_by_month(dates_sorted, *get_period_dates(year, month))
    first_trading_date = filtered_dates.iloc[0]
    last_trading_date = filtered_dates.iloc[-1]
    
    if year == start_year and month == 1:
        roll_over_trading_date = first_trading_date
    else:
        temp_start_date, temp_end_date = get_period_dates(year-1 if month == 1 else year, 12 if month == 1 else month-1)
        temp_filtered_dates = filter_dates_by_month(dates_sorted, temp_start_date, temp_end_date)
        roll_over_trading_date = temp_filtered_dates.iloc[-1]

    return first_trading_date, last_trading_date, roll_over_trading_date


def get_top_n_scripts(data, volumes, ema, first_trading_date, last_trading_date, roll_over_trading_date, all_scripts, number_of_stocks, filters):
    filtered_stocks = all_scripts
    print('@@@@@@@@@@@@@@')
    print('first_trading_date: ',first_trading_date)
    print('last_trading_date: ',last_trading_date)
    print('rollover_trading_date: ',roll_over_trading_date)
    print(f'get_top_n process_id: {os.getpid()}')
    for filter_func in filters:
        print('filtername: ',str(filter_func), 
              f"process_id: {os.getpid()}", 
              f"top n: {number_of_stocks}, rollover_trading_date: {roll_over_trading_date}")
        filtered_stocks = list(filter(
            lambda stock: filter_func(data, volumes, ema, stock, first_trading_date, last_trading_date, roll_over_trading_date),
            filtered_stocks))
        print('number of scripts filtered: ', len(filtered_stocks))
        print('####################')
    print('no of scripts filtered final: ',len(filtered_stocks))
    print('@@@@@@@@@@@@@@')
    return filtered_stocks[:number_of_stocks]


def calculate_monthly_returns(data, last_trading_date, roll_over_trading_date, top_n_scripts):
    return [
        ((data[data['Date'] == last_trading_date][stock].iloc[0] / 
          data[data['Date'] == roll_over_trading_date][stock].iloc[0]) - 1) * 100 
        for stock in top_n_scripts 
    ]


def adjust_price_if_zero(data, dates, stock, date, is_carry_forward):
    delta_days = 0
    adjusted_price = data[data['Date'] == date][stock].iloc[0]
    
    while adjusted_price == 0:
        delta_days += 1
        adjusted_date = date - timedelta(days=delta_days)
        if adjusted_date in dates.values:
            adjusted_price = data[data['Date'] == adjusted_date][stock].iloc[0]
        else:
            continue

    return adjusted_price


def calculate_month_returns(data, dates, stocks,
                                first_trading_date, roll_over_trading_date, last_trading_date,
                                carry_forward_scripts, price_tracking_enabled = False, sl = -10):

    returns = []
    stock_list = []
    sl_triggered_stocks = []
    for stock in stocks:
      stock_dict = {
          "stock": stock,
          "initial_price": 0,
          "final_price": 0,
          "returns": 0,
          "carry_forward": False,
          "sl_triggered": False,
          "sl_trigger_date": '',
          "is_new": None
      }

      initial_price = 0
      final_price = 0
      stock_returns = 0
      carry_forward = stock in carry_forward_scripts
      sl_triggered = ''
      sl_trigger_date = ''
      is_new = ''

      if carry_forward:
          initial_price = data[data['Date'] == roll_over_trading_date][stock].iloc[0]
      else:
          is_new = True
          initial_price = data[data['Date'] == first_trading_date][stock].iloc[0]

      if initial_price == 0:
        initial_price = adjust_price_if_zero(data, dates, stock, first_trading_date if not carry_forward else roll_over_trading_date, carry_forward)


      if price_tracking_enabled:
        if carry_forward:
            monthly_prices = data[(data['Date'] >= roll_over_trading_date) & (data['Date'] <= last_trading_date)][['Date',stock]]
        else:
            monthly_prices = data[(data['Date'] >= first_trading_date) & (data['Date'] <= last_trading_date)][['Date',stock]]


        monthly_prices['perc_change'] = ((monthly_prices[stock] - initial_price)/initial_price)*100
        filtered_perc_change = monthly_prices[monthly_prices['perc_change'] <= sl ]

        if len(filtered_perc_change) == 0:
            final_price = data[data['Date'] == last_trading_date][stock].iloc[0]
        else:
          sl_triggered = True
          sl_triggered_stocks.append(stock)
          sl_trigger_date = filtered_perc_change['Date'].iloc[0]
          final_price = filtered_perc_change[stock].iloc[0]
      else:
        final_price = data[data['Date'] == last_trading_date][stock].iloc[0]

        if final_price == 0:
            final_price = adjust_price_if_zero(data, dates, stock, last_trading_date, carry_forward)


      stock_returns = ((final_price / initial_price) - 1) * 100

      stock_dict['initial_price'] = initial_price
      stock_dict['final_price'] = final_price
      stock_dict['returns'] = stock_returns
      stock_dict['carry_forward'] = carry_forward
      stock_dict['sl_triggered'] = sl_triggered
      stock_dict['sl_trigger_date'] = '' if sl_trigger_date == '' else str(sl_trigger_date.date())
      stock_dict['sl_trigger_date'] = '' if sl_trigger_date == '' else str(sl_trigger_date.date())
      stock_dict['is_new'] = 'NEW' if is_new else ''
      returns.append(stock_returns)
      stock_list.append(stock_dict)

    return returns, stock_list, sl_triggered_stocks


def get_scripts_sorted(ttm_returns, m_score, sorting_criteria, year, month):
    if sorting_criteria == 'ttm':
        all_scripts = list(ttm_returns[(ttm_returns['Date'].dt.year == year) & (ttm_returns['Date'].dt.month == month)]
                       .iloc[0][1:].sort_values(ascending=False).keys())
        
    if sorting_criteria == 'm_score':
        all_scripts = list(m_score[(m_score['Date'].dt.year == year) & (m_score['Date'].dt.month == month)]
                       .iloc[0][1:].sort_values(ascending=False).keys())
    
    return all_scripts


def process_monthly_portfolio(data, volumes, dates, 
                              year, month, start_year, 
                              ema, sort_function, number_of_stocks, 
                              monthly_returns, filters, return_calculations):
    
    first_trading_date, last_trading_date, roll_over_trading_date = get_trading_dates(dates, year, month, start_year)
    
    all_scripts = sort_function(year = year, month = month)
    top_n_scripts = get_top_n_scripts(data, volumes, ema, first_trading_date, last_trading_date, roll_over_trading_date, all_scripts, number_of_stocks, filters)
    
    monthly_pf = {
        'year': year,
        'month': month,
        'start_date': get_period_dates(year, month)[0],
        'end_date': get_period_dates(year, month)[1],
        'first_trading_date': first_trading_date,
        'last_trading_date': last_trading_date,
        'roll_over_trading_date': roll_over_trading_date,
        'top_n_scripts': top_n_scripts
    }

    if roll_over_trading_date == first_trading_date:
        # top_n_monthly_returns = calculate_month_returns(data, dates, top_n_scripts, first_trading_date, roll_over_trading_date, last_trading_date, carry_forward_scripts=[])
        top_n_monthly_returns, portfolio, sl_triggered_scripts = return_calculations(stocks = top_n_scripts,
                                                               first_trading_date = first_trading_date, 
                                                               roll_over_trading_date = roll_over_trading_date, 
                                                               last_trading_date = last_trading_date, 
                                                               carry_forward_scripts = [] )
        monthly_pf['top_n_monthly_returns'] = top_n_monthly_returns
        monthly_pf['scripts_with_returns'] = dict(zip(top_n_scripts, top_n_monthly_returns))
        monthly_pf['monthly_returns'] = sum(top_n_monthly_returns) / len(top_n_monthly_returns)
        monthly_pf['last_month_30'] = []
        monthly_pf['new_added_scripts'] = []
        monthly_pf['removed_scripts'] = []
        monthly_pf['carry_forward_scripts'] = []
        monthly_pf['portfolio'] = portfolio
        monthly_pf['sl_triggered_scripts'] = sl_triggered_scripts
    else:
        last_month_trades_dict = monthly_returns.get(f'{month-1}_{year}', {}) if month > 1 else monthly_returns.get(f'12_{year-1}', {})
        last_month_scripts = last_month_trades_dict.get('top_n_scripts', [])
        last_month_sl_triggered_scripts = last_month_trades_dict.get('sl_triggered_scripts', [])

        new_added_scripts = [script for script in top_n_scripts if script not in last_month_scripts]
        removed_scripts = [script for script in last_month_scripts if script not in top_n_scripts]
        carry_forward_scripts = [script for script in top_n_scripts if (script in last_month_scripts) and (script not in last_month_sl_triggered_scripts)]
        
        # top_n_monthly_returns = calculate_month_returns(data, dates, top_n_scripts, first_trading_date, roll_over_trading_date, last_trading_date, carry_forward_scripts)
        top_n_monthly_returns, portfolio, sl_triggered_scripts = return_calculations(stocks = top_n_scripts,
                                                               first_trading_date = first_trading_date, 
                                                               roll_over_trading_date = roll_over_trading_date, 
                                                               last_trading_date = last_trading_date, 
                                                               carry_forward_scripts = carry_forward_scripts)
        monthly_pf['top_n_monthly_returns'] = top_n_monthly_returns
        monthly_pf['scripts_with_returns'] = dict(zip(top_n_scripts, top_n_monthly_returns))
        monthly_pf['monthly_returns'] = sum(top_n_monthly_returns) / len(top_n_monthly_returns)
        monthly_pf['last_month_30'] = last_month_scripts
        monthly_pf['new_added_scripts'] = new_added_scripts
        monthly_pf['removed_scripts'] = removed_scripts
        monthly_pf['carry_forward_scripts'] = carry_forward_scripts
        monthly_pf['portfolio'] = portfolio
        monthly_pf['sl_triggered_scripts'] = sl_triggered_scripts

    return monthly_pf


def sort_dates(dates):
    return dates.sort_values(ascending=True)


def create_data_frame(month_wise_returns, max_stock_rows):
    # List to hold all rows
    rows = []
    columns = []

    # Process each month in the data
    for key, value in month_wise_returns.items():
        first_trading_date = value['first_trading_date']
        last_trading_date = value['last_trading_date']
        monthly_return = value['monthly_returns']
        roll_over_trading_date = value['roll_over_trading_date']
        year = value['year']
        month = calendar.month_name[value['month']][:3]

        # Append the column headers for this month
        fixed_col = ['Name', 'Monthly Return', 'Status', 'Initial Price', 'Final Price', 'SL triggered', 'SL trigger date']
        columns.extend(fixed_col)

        # Metadata rows for each month
        metadata_rows = [
            ['First Trading Date', first_trading_date.date(), '', '', '', '', ''],
            ['Last Trading Date', last_trading_date.date(), '', '', '', '', ''],
            ['Rollover Trading Date', roll_over_trading_date.date(), '', '', '', '', ''],
            ['Monthly Return', monthly_return, '', '', '', '', ''],
            ['Month-Year', f'{month}-{year}', '', '', '', '', ''],  # Blank row
            ['']*len(fixed_col)  # Blank row
        ]

        # Append stock data rows for each month
        stock_rows = []
        for key, stock in enumerate(value['portfolio']):
            # new_old_status = 'NEW' if stock['carry_forward'] is False else ''
            stock_rows.append([stock['stock'], round(stock['returns'], 2), stock['is_new'], stock['initial_price'], stock['final_price'], stock['sl_triggered'], stock['sl_trigger_date']])

        # Fill in missing stock rows with empty strings if needed
        while len(stock_rows) < max_stock_rows:
            stock_rows.append(['']*len(fixed_col))

        # Append the rows for this month to the main rows list
        if rows:
            # Extend the existing rows with metadata and stock rows for this month
            for i, metadata_row in enumerate(metadata_rows):
                if len(rows) > i:
                    rows[i].extend(metadata_row)
                else:
                    rows.append(metadata_row + [''] * (len(columns) - len(metadata_row)))
            
            # Add stock rows
            start_idx = len(metadata_rows)
            for i, stock_row in enumerate(stock_rows):
                if len(rows) <= start_idx + i:
                    rows.append([''] * (len(columns) - 5) + stock_row)
                else:
                    rows[start_idx + i].extend(stock_row)
        else:
            # Initialize the rows list with metadata and stock rows for the first month
            rows.extend(metadata_rows)
            rows.extend(stock_rows)

    # Ensure all rows have the same length by filling with blanks if needed
    max_len = len(columns)  # The length of columns should be the same as the final row length
    for i in range(len(rows)):
        if len(rows[i]) < max_len:
            rows[i].extend([''] * (max_len - len(rows[i])))

    # Creating the DataFrame
    df = pd.DataFrame(rows, columns=columns[:max_len])

    # Display the DataFrame
    return df


def create_excel(sheet_name,dfs,version,start_year = 2010,start_month = 1, end_year = 2024,end_month=3, top_n = [5, 10, 15, 20, 25, 30]):
    with pd.ExcelWriter(sheet_name) as writer:
        
        for key,df in enumerate(dfs):
            # use to_excel function and specify the sheet_name and index 
            # to store the dataframe in specified sheet
            sheet_name = f"{df['sheet_name']}_{version}"
            data_frame = create_data_frame(df['df'],df['top_n'])
            data_frame.to_excel(writer, sheet_name=sheet_name, index=False)
        
        sheet_name = 'summary'
        # data_frame = create_summary_data_frame(dfs, start_year,end_year, version=version)
        data_frame = create_summary_data_frame(dfs, start_year, start_month, end_year, end_month, version, {end_year:end_month}, top_n)

        summary_matrix = create_return_matrix_by_year(summary_df=data_frame, version=version, stock_counts=top_n)
        yearly_returns = create_yearly_returns_matrix(data_frame, stock_counts= top_n, version = version)
        dd = create_max_drawdown_matrix(summary_df=data_frame, stock_counts= top_n, version = version, initial_value=100)
        ratios = calculate_performance_metrics(yearly_returns, dd, include_last_year=False)
        summary_matrix['yearly_returns'] = yearly_returns
        summary_matrix['max_drawdown'] = dd
        summary_matrix['ratios'] = ratios

        returns_col = data_frame.columns
        top_row = [100 if 'Portfolio' in c else '' for c in returns_col ]
        top_row = pd.DataFrame([top_row], columns=returns_col)
        data_frame = pd.concat([top_row, data_frame], ignore_index=True)
        data_frame.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print("summary matrix complete")
        for sheet_name, df in summary_matrix.items():
            df.reset_index(inplace=True)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return True


def calculate_returns_till_date(month_wise_returns, initial_value = 100, index = False):
    returns = []
    if index:
        for i in month_wise_returns:
            if i != None:
                initial_value = initial_value + ((i*initial_value)/100)
                returns.append(initial_value)
            else:
                returns.append(i)
    else:
        for key in month_wise_returns.keys():
            if(month_wise_returns[key]['monthly_returns'] == float('inf') or month_wise_returns[key]['monthly_returns'] == float('-inf')):
                print(month_wise_returns[key])
            else:
                initial_value = initial_value + ((month_wise_returns[key]['monthly_returns']*initial_value)/100)
            returns.append(initial_value)

    return returns


def create_nifty_df(df, start_year, start_month, end_year, end_month):
    df['Date'] = pd.to_datetime(df['Date'])

    start_date = pd.Timestamp(year=start_year, month=start_month, day=1)  - pd.offsets.MonthBegin(1)
    end_date = pd.Timestamp(year=end_year, month=end_month, day=1) + pd.offsets.MonthEnd(1)
        
        # Filter the DataFrame based on the date range
    df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)][['Date','C']].reset_index(drop=True)

    df.set_index('Date', inplace=True)

    # Resample to get the last value of each month
    monthly_prices = df.resample('M').last()

    # Calculate monthly returns
    monthly_returns = monthly_prices.pct_change().dropna() * 100

    monthly_returns.reset_index(inplace=True)

    monthly_returns.rename(columns = {'C':'Returns_Nifty_500'}, inplace = True)

    monthly_returns['Portfolio_Returns_Nifty_500'] = np.nan

    df.reset_index(inplace=True)
    return monthly_returns[['Returns_Nifty_500','Portfolio_Returns_Nifty_500']]


def create_summary_data_frame(month_wise_returns, start_year=2010, start_month=1, end_year=2024, end_month = 3, version='', end_months={2024: 3}, top_n_values=[5, 10, 15, 20, 25, 30]):
    # Load Nifty data dynamically
    nifty_df = pd.read_csv(f"{nifty_500_csv}")
    
    # Dynamically create column names based on top_n_values
    returns_columns = [f'Returns_(Top_{n})_{version}' for n in top_n_values]
    portfolio_returns_columns = [f'Portfolio_Returns_(Top_{n})_{version}' for n in top_n_values]
    date_columns = ['Year', 'Month', 'Month End Dates']

    columns = date_columns + returns_columns + portfolio_returns_columns
    
    # Initialize empty DataFrame
    df = pd.DataFrame(columns=columns)
    index = 0

    for year in range(start_year, end_year + 1):
        if year == start_year:
            months = range(start_month, end_months.get(year, 12) + 1)
        else:
            months = range(1, end_months.get(year, 12) + 1)  # Use the dynamic end month for each year
        
        for month in months:
            # Safely retrieve last trading date and monthly returns for each top_n portfolio
            last_trading_date = month_wise_returns[0]['df'].get(f'{month}_{year}', {}).get('last_trading_date', None)
            monthly_returns = [
                month_wise_returns[i]['df'].get(f'{month}_{year}', {}).get('monthly_returns', 0) for i in range(len(top_n_values))
            ]
            
            # Add a new row to the DataFrame with dynamic returns
            df.loc[index] = [
                year, 
                month, 
                last_trading_date.date() if last_trading_date else None, 
                *monthly_returns, 
                *([0] * len(top_n_values))  # Placeholder for portfolio returns
            ]
            index += 1
    

    # Calculate portfolio returns dynamically based on top_n_values
    for i, n in enumerate(top_n_values):
        portfolio_col = f'Portfolio_Returns_(Top_{n})_{version}'
        df[portfolio_col] = calculate_returns_till_date(month_wise_returns[i]['df'])

    
    # Dynamically handle Nifty data size

    nifty_df = create_nifty_df(nifty_df, start_year, start_month, end_year, end_month)
    nifty_df_length = len(nifty_df)
    df_length = len(df)

    if nifty_df_length < df_length:
        # If Nifty data is shorter, pad it with NaNs to match the size of df
        padding_length = df_length - nifty_df_length
        padding_df = pd.DataFrame({'Returns_Nifty_500': [None] * padding_length, 'Portfolio_Returns_Nifty_500': [None] * padding_length})
        truncated_nifty_df = pd.concat([nifty_df[['Returns_Nifty_500']], padding_df], ignore_index=True)
    else:
        # Otherwise, truncate Nifty data to match df length
        truncated_nifty_df = nifty_df[['Returns_Nifty_500']].tail(df_length).reset_index(drop=True)
        
    # Add portfolio returns for Nifty 500
    truncated_nifty_df['Portfolio_Returns_Nifty_500'] = calculate_returns_till_date(truncated_nifty_df['Returns_Nifty_500'].values, index=True)
        

    # Concatenate with Nifty data dynamically
    df = pd.concat([df.reset_index(drop=True), truncated_nifty_df.reset_index(drop=True)], axis=1)

    return df


def create_return_matrix_by_year(summary_df, stock_counts=[5, 10, 15], version=''):
    # Create a dictionary to store matrices for each stock count
    return_matrices = {}

    # Loop over each stock count to create separate return matrices
    for count in stock_counts:
        column_name = f'Returns_(Top_{count})_{version}'

        # Pivot the data so that years are rows, months are columns, and returns are the values
        return_matrix = summary_df.pivot_table(
            index='Year',            # Rows as years
            columns='Month',         # Columns as months
            values=column_name       # Values as returns
        )

        # Replace month integers with abbreviations
        return_matrix.columns = return_matrix.columns.map(month_abbreviations)

        # Add a 'Total Returns' column (sum across months)
        return_matrix['Total_Returns'] = return_matrix.sum(axis=1)

        # Add a heading to the DataFrame indicating the top n count
        return_matrix.columns.name = f'Top_{count}_Returns'

        # Add the return matrix to the dictionary
        return_matrices[f'Top_{count}_Returns'] = return_matrix

    return return_matrices


def create_yearly_returns_matrix(summary_df, stock_counts=[5, 10, 15, 20, 25, 30], version = 'v3'):
    """
    Creates a DataFrame with years as rows and Top N portfolio returns, as well as Nifty 500 returns, as columns.
    Yearly returns are calculated based on the portfolio values from the summary DataFrame.

    :param summary_df: DataFrame created by create_summary_data_frame with portfolio values for each stock count.
    :param stock_counts: List of top N stock counts for which yearly returns will be calculated.
    :return: DataFrame with yearly returns.
    """
    # Initialize an empty DataFrame to store the yearly returns
    yearly_returns = pd.DataFrame()

    # Extract the list of years from the summary DataFrame
    years = summary_df['Year'].unique()

    # Set the years as the index of the result DataFrame
    yearly_returns['Year'] = years
    yearly_returns.set_index('Year', inplace=True)

    # Loop over each stock count and calculate the yearly returns
    for count in stock_counts:
        portfolio_col = f'Portfolio_Returns_(Top_{count})_{version}'  # Column name for portfolio values

        # Initialize an empty list to store yearly returns for each count
        returns_list = []

        for year in years:
            # Get the portfolio values for the current year
            year_data = summary_df[summary_df['Year'] == year][portfolio_col]

            if year_data.empty:
                returns_list.append(None)
                continue

            # Get the final portfolio value of the year
            final_portfolio_value = year_data.iloc[-1]

            # Check if it's the first year or if we have data for the previous year
            if year == years[0]:  # First year, we don't have the previous year's value
                initial_portfolio_value = 100
                yearly_return = (final_portfolio_value / initial_portfolio_value - 1) * 100
            else:
                # Get the portfolio values for the previous year
                prev_year_data = summary_df[summary_df['Year'] == year - 1][portfolio_col]

                if not prev_year_data.empty:
                    prev_final_portfolio_value = prev_year_data.iloc[-1]
                    yearly_return = (final_portfolio_value / prev_final_portfolio_value - 1) * 100
                else:
                    yearly_return = None  # If there's no data for the previous year

            returns_list.append(yearly_return)

        # Add the yearly returns for the current count to the DataFrame
        yearly_returns[f'Top_{count}'] = returns_list

    # Add the Nifty 500 returns to the matrix
    returns_list_nifty_500 = []

    for year in years:
        # Get the Nifty 500 returns for the current year
        nifty_500_data = summary_df[summary_df['Year'] == year]['Portfolio_Returns_Nifty_500']

        if not nifty_500_data.empty:
            final_nifty_500_value = nifty_500_data.iloc[-1]

            if year == years[0]:
                initial_nifty_500_value = 100
                yearly_nifty_return = (final_nifty_500_value / initial_nifty_500_value - 1) * 100
            else:
                prev_nifty_500_data = summary_df[summary_df['Year'] == year - 1]['Portfolio_Returns_Nifty_500']
                if not prev_nifty_500_data.empty:
                    prev_final_nifty_500_value = prev_nifty_500_data.iloc[-1]
                    yearly_nifty_return = (final_nifty_500_value / prev_final_nifty_500_value - 1) * 100
                else:
                    yearly_nifty_return = None
        else:
            yearly_nifty_return = None

        returns_list_nifty_500.append(yearly_nifty_return)

    yearly_returns['Nifty_500'] = returns_list_nifty_500

    return yearly_returns


def max_drawdown(returns, initial_value = 100):
    # Append the initial value at the start of the series
    returns = pd.concat([pd.Series([initial_value]), returns.reset_index(drop=True)], ignore_index=True)


    # Calculate the running maximum
    running_max = returns.cummax()

    # Calculate the drawdown
    drawdown = ((returns - running_max) / running_max) * 100

    # Find the maximum drawdown
    max_dd = drawdown.min()

    return max_dd


def create_max_drawdown_matrix(summary_df, stock_counts=[5, 10, 15, 20, 25, 30], version='v1', initial_value=100):
    """
    Creates a DataFrame with years as rows and Top N max drawdowns (as well as Nifty 500 drawdowns) as columns.
    The max drawdowns are calculated based on the portfolio values from the summary DataFrame.

    :param summary_df: DataFrame created by create_summary_data_frame with portfolio values for each stock count.
    :param stock_counts: List of top N stock counts for which max drawdowns will be calculated.
    :param initial_value: Initial portfolio value used for calculating drawdowns.
    :return: DataFrame with max drawdowns.
    """
    # Initialize an empty DataFrame to store the max drawdowns
    max_drawdown_df = pd.DataFrame()

    # Extract the list of years from the summary DataFrame
    years = summary_df['Year'].unique()

    # Set the years as the index of the result DataFrame
    max_drawdown_df['Year'] = years
    max_drawdown_df.set_index('Year', inplace=True)

    # Loop over each stock count and calculate the max drawdown
    for count in stock_counts:
        portfolio_col = f'Portfolio_Returns_(Top_{count})_{version}'  # Column name for portfolio values

        # Initialize an empty list to store max drawdowns for each count
        drawdown_list = []

        for year in years:
            # Get the portfolio values for the current year
            year_data = summary_df[summary_df['Year'] == year][portfolio_col]

            if year_data.empty:
                drawdown_list.append(None)
                continue

            # Calculate max drawdown using the portfolio values for the year
            max_dd = max_drawdown(year_data, initial_value)
            drawdown_list.append(max_dd)

        # Add the max drawdowns for the current count to the DataFrame
        max_drawdown_df[f'Top_{count}'] = drawdown_list

    # Add the Nifty 500 max drawdown to the matrix
    drawdown_list_nifty_500 = []

    for year in years:
        # Get the Nifty 500 portfolio returns for the current year
        nifty_500_data = summary_df[summary_df['Year'] == year]['Portfolio_Returns_Nifty_500']

        if nifty_500_data.empty:
            drawdown_list_nifty_500.append(None)
            continue

        # Calculate max drawdown using the Nifty 500 data for the year
        max_dd_nifty_500 = max_drawdown(nifty_500_data, initial_value)
        drawdown_list_nifty_500.append(max_dd_nifty_500)

    # Add Nifty 500 max drawdown to the DataFrame
    max_drawdown_df['Nifty_500'] = drawdown_list_nifty_500

    return max_drawdown_df


def calculate_performance_metrics(yearly_returns_matrix, max_drawdown_matrix, risk_free_rate=0.07, include_last_year=False):
    """
    Function to create a matrix with performance metrics (Rp, Rf, SDp, Sharpe Ratio, and Calmar Ratio)
    for each portfolio. Both returns and drawdowns are assumed to be in percentage form (already multiplied by 100).

    :param yearly_returns_matrix: DataFrame containing yearly returns (in percentage) for each portfolio.
    :param max_drawdown_matrix: DataFrame containing maximum drawdown (in percentage) for each portfolio.
    :param risk_free_rate: The risk-free rate used for Sharpe ratio calculations (default is 3%).
    :param include_last_year: Boolean to determine whether the last year should be included in the calculations.
    :return: DataFrame containing performance metrics for each portfolio.
    """
    # Create a copy of the input DataFrames to avoid modifying the original data
    yearly_returns = yearly_returns_matrix.copy()
    max_drawdown = max_drawdown_matrix.copy()

    # If include_last_year is False, drop the last year from both DataFrames
    if not include_last_year:
        yearly_returns = yearly_returns.iloc[:-1, :]
        max_drawdown = max_drawdown.iloc[:-1, :]

    # Create an empty DataFrame to store the metrics
    performance_metrics_df = pd.DataFrame()

    # Iterate over the portfolios (Top N) in yearly returns
    for portfolio in yearly_returns.columns:
        # Extract yearly returns for the current portfolio (convert percentage to decimal)
        returns = yearly_returns[portfolio] / 100

        # Calculate Rp (Average yearly return) and SDp (Standard deviation of returns) in decimal
        Rp = returns.mean()  # Mean of returns
        SDp = returns.std()   # Standard deviation of returns

        # Get the max drawdown for the current portfolio (convert percentage to decimal)
        max_dd = min(max_drawdown[portfolio]) / 100

        # Calculate Sharpe Ratio: (Rp - Rf) / SDp
        sharpe_ratio = (Rp - risk_free_rate) / SDp if SDp != 0 else np.nan

        # Calculate Calmar Ratio: Rp / Max Drawdown
        calmar_ratio = (Rp - risk_free_rate) / abs(max_dd) if max_dd != 0 else np.nan

        # Store the calculated metrics in the DataFrame
        performance_metrics_df[portfolio] = [Rp * 100, risk_free_rate * 100, SDp * 100, sharpe_ratio, calmar_ratio]

    # Set the row index for the DataFrame
    performance_metrics_df.index = ['Rp (%)', 'Rf (%)', 'SDp (%)', 'Sharpe Ratio', 'Calmar Ratio']

    return performance_metrics_df


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')  # Convert DataFrame to a list of dicts
        elif isinstance(obj, pd.Series):
            return obj.to_list()  # Convert Series to a list
        elif pd.isna(obj):  # Handle NaN and None in pandas
            return None
        return super().default(obj)


def save_dict_to_json(data_dict, filename):
    with open(filename, 'w') as json_file:
        json.dump(data_dict, json_file, cls=CustomJSONEncoder, indent=4)
    
