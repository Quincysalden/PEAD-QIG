sp500_members = pd.read_csv(r"C:\Users\20213053\OneDrive - TU Eindhoven\Documents\QIG\PEAD LOCAL\sp500_members_by_year_2000_2024.csv")

start_date = '2000-01-01'
end_date = '2025-12-31'


all_tickers = sp500_members['ticker'].unique()

tickers_v1 = list(all_tickers[0:3])

tickers_v1.append('AAPL.O')

df1 = import_earn_dates(tickers_v1, start_date, end_date)



print(all_tickers)


# %%

from tqdm import tqdm
import lseg.data as ld
import pandas as pd

YOUR_APP_KEY = "6a72e6bde7e642cb8843ad44049a3b07dd73e5cd"

#suffixes = ['', '.O', '.N', '.L', '.PA']  # first try as-is
suffixes = ['', '.O', '.N', '.L', '.PA', '.TO', '.HK', '.MI', '.VX']

def fetch_with_suffixes(ticker, start_date, end_date):
    """Try a ticker with different suffixes until it returns non-empty events."""
    for suf in suffixes:
        t = ticker + suf
        try:
            df = ld.get_data(
                universe=[t],
                fields=['TR.EventType', 'TR.EventType.date'],
                parameters={'SDate': start_date, 'EDate': end_date, 'EventType': 'RES'}
            )
            if df is not None and not df.empty:
                return df  # success
        except Exception:
            continue
    return None  # all failed

def import_earn_dates_auto(tickers, start_date, end_date, app_key=YOUR_APP_KEY):
    failed = []
    all_events = []

    ld.open_session(app_key=app_key)

    # Loop with tqdm progress bar
    for t in tqdm(tickers, desc="Fetching earnings dates"):
        df = fetch_with_suffixes(t, start_date, end_date)
        if df is not None:
            all_events.append(df)
        else:
            failed.append(t)

    ld.close_session()

    # Combine only non-empty results
    df_all = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    return df_all, failed

# Make sure tickers are a Python list, not a NumPy array
tickers50 = list(all_tickers[:50])

# Run the auto-import function with tqdm
df_all, failed = import_earn_dates_auto(all_tickers, start_date, end_date)

print(f"Failed tickers: {failed}")


# %%

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import lseg.data as ld
import pandas as pd

suffixes = ['', '.O', '.N', '.L', '.PA']

def fetch_with_suffixes(ticker, start_date, end_date):
    for suf in suffixes:
        t = ticker + suf
        try:
            df = ld.get_data(
                universe=[t],
                fields=['TR.EventType', 'TR.EventType.date'],
                parameters={'SDate': start_date, 'EDate': end_date, 'EventType': 'RES'}
            )
            if df is not None and not df.empty:
                return df
        except:
            continue
    return None

def fetch_parallel(tickers, start_date, end_date, app_key=YOUR_APP_KEY, max_workers=5):
    ld.open_session(app_key=app_key)
    all_events = []
    failed = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(fetch_with_suffixes, t, start_date, end_date): t for t in tickers}
        for future in tqdm(as_completed(future_to_ticker), total=len(future_to_ticker), desc="Fetching earnings dates"):
            t = future_to_ticker[future]
            try:
                df = future.result()
                if df is not None:
                    all_events.append(df)
                else:
                    failed.append(t)
            except:
                failed.append(t)

    ld.close_session()
    df_all = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    return df_all, failed



df_all, failed = fetch_parallel(all_tickers, start_date, end_date , max_workers = 10)

if False:
    df_all.to_csv(r"C:\Users\20213053\OneDrive - TU Eindhoven\Documents\QIG\PEAD LOCAL\earnings_data_1_12\earnings_raw_v1.csv", index=False)


df_filt = df_all[df_all['Date'].isna() == False]
df_filt['Date'] = pd.to_datetime(df_filt['Date'])
df_filt['year'] = df_filt['Date'].dt.year
df_filt['ticker'] = df_filt['Instrument'].str.split('.').str[0]

sp500_set = set(zip(sp500_members['ticker'], sp500_members['year']))
mask = df_filt.apply(lambda row: (row['ticker'], row['year']) in sp500_set, axis=1)

df_final = df_filt[mask]

if False:
    df_final.to_csv(r"C:\Users\20213053\OneDrive - TU Eindhoven\Documents\QIG\PEAD LOCAL\earnings_data_1_12\earnings_final_v1.csv", index=False)

