import requests
import pandas as pd
import numpy as np


def get_earnings(symbol , api_key = 's1KV9x3qQ7NSH6JMRWUG6zAEvEpAsfrz'):
    """
    Returns historical earnings data for a given stock symbol
    """
    url = f'https://financialmodelingprep.com/stable/earnings?symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        earnings_data = response.json()
        return earnings_data
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        
        
def get_historical_eod_prices(symbol , start_date, end_date, api_key = 's1KV9x3qQ7NSH6JMRWUG6zAEvEpAsfrz'):
    """
    return historical prices, adjusted for dividends
    """
    params ={
        }
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted"
    params = {
        "symbol": symbol,
        "from": start_date,
        "to": end_date,
        "apikey": api_key
    }

    response = requests.get(url , params)

    if response.status_code == 200:
        price_data = response.json()
        return price_data
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        

def price_to_df(price_data):
    df = pd.DataFrame(price_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.set_index('date', inplace=True)
    # daily return
    df['return'] = df['adjClose'].pct_change()
    return df


# Earnings Data
def earnings_to_df(earnings_data):
    df = pd.DataFrame(earnings_data)
    df['date'] = pd.to_datetime(df['date'])
    # Only keep rows with estimated EPS
    df = df[df['epsEstimated'].notna()]
    df['eps_surprise'] = df['epsActual'] - df['epsEstimated']
    df = df.sort_values('date')
    return df[['date', 'epsActual', 'epsEstimated', 'eps_surprise']]


def compute_sue(earnings_df, lookback=8):
    sue_list = []
    for i in range(len(earnings_df)):
        if i < lookback:
            sue_list.append(None)
            continue
        past_surprises = earnings_df['eps_surprise'].iloc[i-lookback:i]
        sigma = past_surprises.std()
        actual = earnings_df['eps_surprise'].iloc[i]
        sue = actual / sigma if sigma != 0 else 0
        sue_list.append(sue)
    earnings_df['SUE'] = sue_list
    return earnings_df

def compute_expected_returns_mean(df, lookback=252):
    """
    Compute expected returns as rolling mean of past N periods.
    """
    df['expected_return'] = df['return'].rolling(lookback).mean()
    return df

def add_abnormal_returns(df, expected_return_col=None):
    """
    Add an 'AR' column for abnormal returns.

    Parameters:
        df : DataFrame with 'return' column
        expected_return_col : str, optional, name of expected return column

    Returns:
        df : DataFrame with 'AR' column added
    """
    if expected_return_col is None:
        df['AR'] = df['return']
    else:
        df['AR'] = df['return'] - df[expected_return_col]
    return df

def daily_to_weekly_ar(df):
    """
    Convert daily abnormal returns to weekly abnormal returns.
    Assumes df is indexed by date and has 'AR' column.
    """
    weekly_ar = df['AR'].resample('W-FRI').apply(lambda x: (1 + x).prod() - 1)
    weekly_df = weekly_ar.to_frame()
    weekly_df.rename(columns={'AR': 'weekly_AR'}, inplace=True)
    return weekly_df

def get_weekly_event_windows(earnings_df, weekly_df, weeks=6):
    """
    Returns a DataFrame with weekly abnormal returns for each earnings event.
    """
    all_windows = []

    for _, row in earnings_df.iterrows():
        ann_date = row['date']
        sue = row['SUE']

        # Get the weekly window after announcement
        # TODO: fix weekly windows, currently takes weeks of the year, instead of just 5 day windows
        window_df = weekly_df.loc[ann_date : ann_date + pd.Timedelta(weeks=weeks)].copy()
        if window_df.empty:
            continue

        window_df['week_number'] = range(1, len(window_df)+1)
        window_df['SUE'] = sue
        window_df['announcement_date'] = ann_date
        window_df['eps_surprise'] = row['eps_surprise']
        window_df['epsActual'] = row['epsActual']
        window_df['epsEstimated'] = row['epsEstimated']

        # Add cumulative abnormal return
        window_df['CAR'] = (1 + window_df['weekly_AR']).cumprod() - 1

        all_windows.append(window_df)

    event_df = pd.concat(all_windows)
    return event_df.reset_index()


def avg_car_by_decile(ticker , start_date, end_date):
    earnings_data = get_earnings(ticker)
    earnings_df = earnings_to_df(earnings_data)
    earnings_df = compute_sue(earnings_df)
    
    price_data = get_historical_eod_prices(ticker, start_date, end_date)
    price_df = price_to_df(price_data)
    price_df = compute_expected_returns_mean(price_df)
    price_df = add_abnormal_returns(price_df , 'expected_return')
    valid_df = price_df[price_df['expected_return'].notna()].copy()
    
    weekly_df = daily_to_weekly_ar(valid_df)
    event_df = get_weekly_event_windows(earnings_df, weekly_df, weeks=6)
    
    # add jitter to avoid conflicts in decile creation
    event_df['SUE_jitter'] = event_df['SUE'] + np.random.normal(0, 1e-10, size=len(event_df))
    event_df['SUE_decile'] = pd.qcut(event_df['SUE_jitter'], 10, labels=False)

    avg_car_dec = event_df.groupby(['SUE_decile','week_number'])['CAR'].mean().unstack()
    avg_weekly_ar_dec = event_df.groupby(['SUE_decile','week_number'])['weekly_AR'].mean().unstack()

    return avg_car_dec , avg_weekly_ar_dec


#tickers = ['AAPL', 'TSLA', 'AMZN', 'MSFT', 'NVDA', 'GOOGL', 'META', 'NFLX', 'JPM', 'V', 'BAC', 'AMD', 'PYPL', 'DIS', 'T', 'PFE', 'COST', 'INTC', 'KO', 'TGT', 'NKE', 'SPY', 'BA', 'BABA', 'XOM', 'WMT', 'GE', 'CSCO', 'VZ', 'JNJ', 'CVX', 'PLTR', 'SQ', 'SHOP', 'SBUX', 'SOFI', 'HOOD', 'RBLX', 'SNAP', 'AMD', 'UBER', 'FDX', 'ABBV', 'ETSY', 'MRNA', 'LMT', 'GM', 'F', 'RIVN', 'LCID', 'CCL', 'DAL', 'UAL', 'AAL', 'TSM', 'SONY', 'ET', 'NOK', 'MRO', 'COIN', 'RIVN', 'SIRI', 'SOFI', 'RIOT', 'CPRX', 'PYPL', 'TGT', 'VWO', 'SPYG', 'NOK', 'ROKU', 'HOOD', 'VIAC', 'ATVI', 'BIDU', 'DOCU', 'ZM', 'PINS', 'TLRY', 'WBA', 'VIAC', 'MGM', 'NFLX', 'NIO', 'C', 'GS', 'WFC', 'ADBE', 'PEP', 'UNH', 'CARR', 'FUBO', 'HCA', 'TWTR', 'BILI', 'SIRI', 'VIAC', 'FUBO', 'RKT']
#tickers = list(set(tickers))
#print('amount of stocks:', len(tickers))
#start_date = '2015-01-01'
#end_date = '2024-12-31'

#all_avg_cars = []
#all_avg_weekly_ars = []

#for ticker in tickers:
#    print(f"Processing {ticker}...")
#    try:
#        avg_car_dec , avg_weekly_ar_dec = avg_car_by_decile(ticker, start_date, end_date)
#        all_avg_cars.append(avg_car_dec)
#        all_avg_weekly_ars.append(avg_weekly_ar_dec)
#    except Exception as e:
#        print(f"Failed for {ticker}: {e}")

# Stack all dataframes and compute the average across stocks
#combined_avg_car = pd.concat(all_avg_cars)
#mean_avg_car = combined_avg_car.groupby('SUE_decile').mean()

#combined_avg_weekly_ar = pd.concat(all_avg_weekly_ars)
#mean_avg_weekly_ar = combined_avg_weekly_ar.groupby('SUE_decile').mean()