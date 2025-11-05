import eikon as ek
import pandas as pd
import numpy as np
import pandas as pd
from collections import Counter

# Set your Eikon API key
ek.set_app_key('6a72e6bde7e642cb8843ad44049a3b07dd73e5cd')

instrument = "AAPL.O"   # equivalent to $C$3
start_date = "2000-01-01"  # equivalent to $A$2
end_date = "2024-12-31"    # equivalent to $A$3

def get_closing_price_data(instrument, start_date, end_date):
    """
    Fetches daily closing price data for a given instrument between start_date and end_date.
    Returns a DataFrame with columns [Date, Close, Instrument].
    """
    try:
        df = ek.get_timeseries(
            instrument,   # singular argument name
            fields='CLOSE',          # can also use ['OPEN','HIGH','LOW','CLOSE','VOLUME']
            start_date=start_date,
            end_date=end_date,
            interval='daily'
        )
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index().rename(columns={'CLOSE': 'Close'})
        df['Instrument'] = instrument
        return df

    except Exception as e:
        print(f"❌ Error fetching price for {instrument}: {e}")
        return pd.DataFrame()

def clean_ric(ric):
    """Standardize RIC endings that sometimes come from index constituents."""
    # Replace invalid suffixes with valid ones
    if ric.endswith(".OQ"):
        return ric.replace(".OQ", ".O")
    if ric.endswith(".NQ"):
        return ric.replace(".NQ", ".N")
    return ric

def fix_failed_ric(ric):
    """Return a corrected RIC if the original one failed."""
    if ric.endswith(".N"):
        # NYSE tickers sometimes work as .N or .K
        return ric.replace(".N", ".K")  # try NYSE alternative
    elif ric.endswith(".O"):
        return ric  # usually fine for NASDAQ
    elif ric.endswith(".Z"):
        return None  # skip derivatives/indexes
    return ric


def get_earnings_data(instrument, start_date, end_date):
    fields = [
        "TR.EPSActValue",       # EPS actual
        "TR.EPSMeanEstimate",   # EPS mean estimate
        "TR.EventType(EventType=RES).date"          # Event type (we will filter for earnings)
    ]
    
    # Pull data
    df, err = ek.get_data(
        instruments=[instrument],
        fields=fields,
        parameters={
            "EventType": "RES",   # Earnings Release
            "SDate": start_date,
            "EDate": end_date,
            "FRQ": "Q"            # Quarterly
        }
    )
    return df

def get_index_tickers(index):
    """
    Returns a list of tickers for a given chain/index.
    Works with 0#.SPX, 0#.RUT, 0#.RMCC, 0#.RUA
    """
    df, err = ek.get_data(index, ["TR.RIC", "TR.CommonName"])
    if err:
        print(f"Error for {index}: {err}")
        return []
    df = df[df["Instrument"] != index]  # Remove the index itself
    tickers = df["Instrument"].dropna().unique().tolist()
    print(f"{index}: {len(tickers)} tickers")
    return tickers
# %%


# Example usage
index_sp500 = "0#.SPX"
index_russel_midcap = "0#.RMCC"
index_russel_2000 = "0#.RUT"
index_russel_3000 = "0#.RUA"

tickers_sp500 = get_index_tickers(index_sp500)
tickers_russel_midcap = get_index_tickers(index_russel_midcap)
tickers_russel_2000 = get_index_tickers(index_russel_2000)
tickers_russel_3000 = get_index_tickers(index_russel_3000)

# Example: find “midcap only” (unique across all three lists)
all_items = tickers_sp500 + tickers_russel_2000 + tickers_russel_3000
midcap_tickers = [item for item, count in Counter(all_items).items() if count == 1]

df_list = []
failed_tickers = []

for num, ticker in enumerate(tickers_sp500, start=1):
    print(f"Processing ticker {num}/{len(tickers_sp500)}: {ticker}")
    try:
        df = get_earnings_data(ticker, start_date, end_date)
        
        # Skip empty DataFrames
        if df is None or df.empty:
            print(f"⚠️  No data for {ticker}, skipping.")
            failed_tickers.append(ticker)
            continue
        
        df_list.append(df)

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        failed_tickers.append(ticker)
        continue  # move to the next ticker

# Concatenate all successfully retrieved data
if df_list:
    all_earnings_df = pd.concat(df_list, ignore_index=True)
    all_earnings_df.to_excel("SP500_data.xlsx", index=False)
    print(f"\n✅ Done! Saved data for {len(df_list)} tickers to SP500_data.xlsx")
else:
    print("\n⚠️ No data retrieved.")
    

# Save list of failed tickers for inspection
if failed_tickers:
    pd.Series(failed_tickers, name="Failed Tickers").to_csv("failed_tickers.csv", index=False)
    print(f"⚠️ {len(failed_tickers)} tickers failed. Saved to failed_tickers.csv")


def generate_ticker_options(ticker):
    """
    Yield all reasonable RIC options for a ticker.
    Skips obviously invalid ones (.Z, .WI).
    """
    ticker = ticker.strip()
    if ticker.endswith((".Z", ".WI")):
        return
    
    # original
    yield ticker

    # Common suffix alternatives
    suffixes = ['.N', '.K', '.O', '.OQ']
    base = ticker.split('.')[0]
    
    for s in suffixes:
        candidate = f"{base}{s}"
        if candidate != ticker:
            yield candidate
