import requests
import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns

price_data = pd.read_csv('SP500_price_data_combined.csv')
earnings_data = pd.read_excel('SP500_data.xlsx')
price_data['Date'] = pd.to_datetime(price_data['Date'])
earnings_data['Date'] = pd.to_datetime(earnings_data['Date'])

earnings_data.columns = ['ticker' , 'epsActual', 'epsEstimated', 'date']
price_data.columns = ['date' , 'close' , 'ticker']

spy_close = pd.read_excel("spy_close 2000_2025_11_1.xlsx")
spy_close['Date'] = pd.to_datetime(spy_close['Date'])
spy_close = spy_close.sort_values('Date').reset_index(drop=True)

# Daily returns
spy_close['spy_return'] = spy_close['Close'].pct_change()



tickers = set(price_data['ticker']) & set(earnings_data['ticker'])
dataframes = []


tickers = list(tickers)

window_days = 30

earnings_windows = defaultdict(list)

for num , ticker in enumerate(tickers):
    print(f"processing pt1 ticker {num}/{len(tickers)}")
    price_df = price_data[price_data['ticker'] == ticker].copy()
    earnings_df = earnings_data[earnings_data['ticker'] == ticker].copy()
    price_df['date'] = pd.to_datetime(price_df['date']).dt.tz_localize(None).dt.date
    earnings_df['date'] = pd.to_datetime(earnings_df['date']).dt.date
    earnings_df['Earnings'] = 1

    

    if len(earnings_df) > 20:
        combined_df = price_df.reset_index(drop=True)
        combined_df['Earnings'] = 0  # initialize column to avoid missing entries

        for _, earn_row in earnings_df.iterrows():
            earn_date = earn_row['date']
            combined_df.loc[combined_df['date'] == earn_date, 'Earnings'] = 1

        

        combined_df['daily return'] = combined_df['close'].pct_change()
        combined_df['2day interval'] = np.where(combined_df['Earnings'] == 1, (1+combined_df['daily return'].shift(-1)) * (1 + combined_df['daily return']) - 1 , 0)

        earnings_dates = combined_df.loc[combined_df['Earnings'] == 1, 'date'].tolist()
        
        for earn_date in earnings_dates:
            # Get the index of the earnings date
            idx = combined_df.index[combined_df['date'] == earn_date][0]
           
            # Slice the window (earn_date + next 30 trading days)
            window_df = combined_df.iloc[idx: idx + 2 + window_days].copy()
            earnings_windows[ticker].append(window_df)
            
       # data[ticker] = combined_df




window_df = earnings_windows['FDS.N'][0]


def compute_weekly_abnormal(window_df, spy_df, start_skip=2, week_days=5, num_weeks=6):
    """
    Compute weekly cumulative stock, SPY, and abnormal returns for a single earnings window.

    Parameters:
    -----------
    window_df : pd.DataFrame
        One earnings window with columns ['date', 'close', 'daily return', ...]
    spy_df : pd.DataFrame
        SPY daily returns, must have columns ['Date', 'spy_return']
    start_skip : int
        Number of days to skip after earnings before week 1
    week_days : int
        Number of trading days per week
    num_weeks : int
        Number of weeks to compute

    Returns:
    --------
    weekly_cum_df : pd.DataFrame
        Weekly cumulative returns with columns:
        ['week', 'stock_cum_return', 'spy_cum_return', 'abnormal_return']
    """
    window_df = window_df.copy()
    window_df['date'] = pd.to_datetime(window_df['date'])
    spy_df['Date'] = pd.to_datetime(spy_df['Date'])
    
    # Merge SPY returns
    merged = window_df.merge(
        spy_df[['Date', 'spy_return']],
        left_on='date',
        right_on='Date',
        how='left'
    ).drop(columns=['Date'])
    
    print(merged)
    # Daily abnormal return
    merged['AR'] = merged['daily return'] - merged['spy_return']
    
    # Save first 2day interval value for later
    first_2day = merged['2day interval'].iloc[0]
    
    # Skip first `start_skip` days
    merged = merged.iloc[start_skip:].copy()
    
    # Assign week number
    # Number of trading days in the merged window (after skipping)
    n_days = len(merged)
    
    # Repeat week numbers for each set of `week_days`, up to `num_weeks`
    week_numbers = np.repeat(np.arange(1, num_weeks+1), week_days)[:n_days]
    merged['week'] = week_numbers

    # Compute weekly cumulative returns
    weekly_cum = merged.groupby('week').agg({
        'daily return': lambda x: (1 + x).prod() - 1,
        'spy_return': lambda x: (1 + x).prod() - 1,
        'AR': lambda x: (1 + x).prod() - 1
    }).reset_index()
    
    # Rename columns
    weekly_cum.rename(columns={
        'daily return': 'stock_cum_return',
        'spy_return': 'spy_cum_return',
        'AR': 'abnormal_cum_return'
    }, inplace=True)
    
    
    weekly_cum['AR_weekly'] = weekly_cum['stock_cum_return'] - weekly_cum['spy_cum_return']
    
    weekly_cum_results = weekly_cum[['week' , 'AR_weekly']]
    return weekly_cum_results, first_2day, merged


weekly_cum , first_2day , merged = compute_weekly_abnormal(window_df, spy_close)
window_df
merged

print(weekly_cum)


all_results = defaultdict(list)

for ticker in tickers:  # loop all tickers
    print(f'processing pt2 ticker {num}/{len(tickers)}')

    for window_df in earnings_windows[ticker]:
        weekly_cum, first_2day, merged = compute_weekly_abnormal(window_df, spy_close)
        all_results[ticker].append({
            'weekly_AR': weekly_cum,
            'first_2day': first_2day
        })
        
        
def visualize_results(all_results): 
    """
    Visualizes results in a heatmap and a line graph
    """    
    events_list = []
    
    for ticker in all_results:
        for i, event in enumerate(all_results[ticker]):
            weekly_ar = event['weekly_AR'].copy()
            weekly_ar['ticker'] = ticker
            weekly_ar['event_id'] = i
            # Add the first 2-day return for this event
            weekly_ar['first_2day'] = event['first_2day']
            events_list.append(weekly_ar)
    
    event_df = pd.concat(events_list, ignore_index=True)
    
    # 2. Add tiny jitter to first_2day to avoid duplicate edges
    event_df['first2_jitter'] = event_df['first_2day'] + np.random.normal(0, 1e-10, size=len(event_df))
    
    # 3. Assign deciles based on first_2day interval
    event_df['first2_decile'] = pd.qcut(event_df['first2_jitter'], 10, labels=False)
    
    # 4. Compute average weekly abnormal return by decile
    avg_weekly_ar_dec = event_df.groupby(['first2_decile', 'week'])['AR_weekly'].mean().unstack()
    
    avg_weekly_ar_dec = avg_weekly_ar_dec.sort_index(axis=1)
    
    # Reset index to make plotting easier
    plot_df = avg_weekly_ar_dec.reset_index().melt(id_vars='first2_decile', var_name='Week', value_name='Avg_AR')
    plot_df['Week'] = plot_df['Week'].astype(int)  # Ensure Week is numeric
    
    # Set seaborn style
    sns.set(style="whitegrid", palette="tab10", font_scale=1.1)
    
    plt.figure(figsize=(12, 6))
    
    # Lineplot of each decile
    sns.lineplot(data=plot_df, x='Week', y='Avg_AR', hue='first2_decile', marker='o', palette='tab10')
    
    plt.title('Weekly Abnormal Returns by First 2-Day Return Deciles')
    plt.xlabel('Week after Earnings Announcement')
    plt.ylabel('Average Weekly Abnormal Return')
    plt.legend(title='Decile (first 2-day return)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
    
    heatmap_data = avg_weekly_ar_dec.copy()
    heatmap_data.index = heatmap_data.index.astype(str)  # deciles as strings for nice labels
    heatmap_data.columns = [f'Week {w}' for w in heatmap_data.columns]
    
    plt.figure(figsize=(10, 6))
    sns.set(font_scale=1.1)
    sns.heatmap(heatmap_data, annot=True, fmt=".4f", cmap="RdYlGn", cbar_kws={'label': 'Average Weekly AR'})
    
    plt.title("Weekly Abnormal Returns by First 2-Day Return Decile")
    plt.ylabel("Decile (first 2-day return)")
    plt.xlabel("Week after Earnings Announcement")
    plt.tight_layout()
    plt.show()
    