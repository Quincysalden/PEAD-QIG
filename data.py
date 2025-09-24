from pead import *

tickers = ['AAPL', 'TSLA', 'AMZN', 'MSFT', 'NVDA', 'GOOGL', 'META', 'NFLX', 'JPM', 'V', 'BAC', 'AMD', 'PYPL', 'DIS', 'T', 'PFE', 'COST', 'INTC', 'KO', 'TGT', 'NKE', 'SPY', 'BA', 'BABA', 'XOM', 'WMT', 'GE', 'CSCO', 'VZ', 'JNJ', 'CVX', 'PLTR', 'SQ', 'SHOP', 'SBUX', 'SOFI', 'HOOD', 'RBLX', 'SNAP', 'AMD', 'UBER', 'FDX', 'ABBV', 'ETSY', 'MRNA', 'LMT', 'GM', 'F', 'RIVN', 'LCID', 'CCL', 'DAL', 'UAL', 'AAL', 'TSM', 'SONY', 'ET', 'NOK', 'MRO', 'COIN', 'RIVN', 'SIRI', 'SOFI', 'RIOT', 'CPRX', 'PYPL', 'TGT', 'VWO', 'SPYG', 'NOK', 'ROKU', 'HOOD', 'VIAC', 'ATVI', 'BIDU', 'DOCU', 'ZM', 'PINS', 'TLRY', 'WBA', 'VIAC', 'MGM', 'NFLX', 'NIO', 'C', 'GS', 'WFC', 'ADBE', 'PEP', 'UNH', 'CARR', 'FUBO', 'HCA', 'TWTR', 'BILI', 'SIRI', 'VIAC', 'FUBO', 'RKT']
tickers = list(set(tickers))
start_date = '2015-01-01'
end_date = '2024-12-31'

def save_all_data(data, filename_prefix="market_data"):
    """
    Save both price and earnings data for multiple tickers into CSVs.
    
    data: dict of the form
        { "TICKER": {"prices": price_data, "earnings": earnings_data} }
    filename_prefix: prefix for the saved CSV files
    """
    all_price_dfs = []
    all_earnings_dfs = []

    for ticker, info in data.items():
        price_df = price_to_df(info['prices']).copy()
        price_df['ticker'] = ticker
        all_price_dfs.append(price_df)

        earnings_df = earnings_to_df(info['earnings']).copy()
        earnings_df['ticker'] = ticker
        all_earnings_dfs.append(earnings_df)

    prices_df = pd.concat(all_price_dfs)
    earnings_df = pd.concat(all_earnings_dfs)

    prices_df.to_csv(f"{filename_prefix}_prices.csv")
    earnings_df.to_csv(f"{filename_prefix}_earnings.csv")
    print("Data saved successfully!")

def load_all_data(filename_prefix="market_data"):
    """
    Load saved price and earnings data and return as dict of DataFrames.
    """
    prices_df = pd.read_csv(f"{filename_prefix}_prices.csv", parse_dates=['date'], index_col='date')
    earnings_df = pd.read_csv(f"{filename_prefix}_earnings.csv", parse_dates=['date'])

    return {"prices": prices_df, "earnings": earnings_df}

data = {}

for ticker in tickers:
    print(f'Loading {ticker}')
    try:
        earnings_data = get_earnings(ticker)
        price_data = get_historical_eod_prices(ticker, start_date, end_date)
        
        data[ticker] = {
            "prices": price_data,
            "earnings": earnings_data
        }
    except Exception as e:
        print(f"Failed for {ticker}: {e}")
# Now you can save it
save_all_data(data, filename_prefix="my_stocks")


tickers_to_remove = [ticker for ticker, info in data.items() if not info.get('earnings')]

print(f"Removing tickers without earnings: {tickers_to_remove}")

# Remove them from the data
for ticker in tickers_to_remove:
    del data[ticker]
    
save_all_data(data, filename_prefix="my_stocks")
