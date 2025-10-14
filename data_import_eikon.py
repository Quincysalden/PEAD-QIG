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
for ticker in tickers_sp500[0:6]:
    df = get_earnings_data(ticker , start_date, end_date)
    df_list.append(df)
    
all_earnings_df = pd.concat(df_list, ignore_index=True)

all_earnings_df.to_excel("test.xlsx")
    
