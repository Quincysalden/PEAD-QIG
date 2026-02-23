import pandas as pd
import lseg.data as ld
import os
import sys
import warnings
import re

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

print("--- STARTING ---")

# ==========================================
# 1. FILE FINDER
# ==========================================
# This section locates your input files regardless of where you run the script from.
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()

print(f"\n1. Locating Files in: {script_dir}")

def get_file_path(candidates):
    """Helper function to search for files in script dir and parent dir."""
    search_dirs = [script_dir, os.path.dirname(script_dir)]
    for d in search_dirs:
        for name in candidates:
            full_path = os.path.join(d, name)
            if os.path.exists(full_path):
                return full_path
    return None

path_price = get_file_path(['PRICECLOSE.csv', 'PRICECLOSE.xlsx'])
path_earn = get_file_path(['PRICEEARNINGS.xlsx - Sheet1.csv', 'PRICEEARNINGS.csv', 'PRICEEARNINGS.xlsx'])

if not path_price or not path_earn:
    print("[CRITICAL ERROR] Could not find input files.")
    sys.exit()

print(f"   - Price File: {os.path.basename(path_price)}")
print(f"   - Earn. File: {os.path.basename(path_earn)}")

# ==========================================
# 2. CONNECT TO LSEG
# ==========================================
# This establishes a connection to the LSEG (Refinitiv) Workspace app on your desktop.
# It is required to fetch the missing data and company names.
print("\n2. Connecting to LSEG...")
try:
    ld.open_session()
    print("   [SUCCESS] LSEG Session Active.")
except Exception as e:
    print(f"   [ERROR] Connection failed: {e}")
    print("   (Script will continue but cannot fetch new names/data)")

# ==========================================
# 3. LOAD & CLEAN DATA
# ==========================================
print("\n3. Loading & Cleaning Data...")

# Load Prices: This reads your main price history file.
# low_memory=False is used because large CSVs often have mixed data types.
df_price = pd.read_csv(path_price, index_col=0, low_memory=False)

# --- FIX FOR DATE ERROR ---
# This block fixes the issue where dates had timezones like "+00:00" causing crashes.
print("   - Normalizing date formats...")
df_price.index = df_price.index.astype(str) # Ensure all dates are strings first
df_price.index = df_price.index.str.replace(r'\+00:00$', '', regex=True) # Remove timezone suffix
# Convert to proper datetime objects. errors='coerce' turns bad dates into NaT (Not a Time)
df_price.index = pd.to_datetime(df_price.index, errors='coerce').normalize()
# Remove any rows where the date couldn't be parsed
df_price = df_price[df_price.index.notnull()]

print(f"   - Loaded Prices: {len(df_price.columns)} companies across {len(df_price)} days.")

# Load Earnings: This reads your earnings file to find out which companies you need.
if path_earn.endswith('.xlsx'):
    df_earn = pd.read_excel(path_earn)
else:
    df_earn = pd.read_csv(path_earn)

# --- TICKER EXTRACTION POINT (CRITICAL STEP) ---
# This extracts the unique list of tickers from the 'Instrument' column of your earnings file.
# This list acts as the "Master Checklist" of companies you want to analyze.
needed_tickers = [x for x in df_earn['Instrument'].unique() if isinstance(x, str)]
print(f"   [INFO] Extracted {len(needed_tickers)} unique tickers from Earnings File to reconcile.")

# ==========================================
# 4. RECONCILIATION (MISSING DATA)
# ==========================================
print("\n4. Checking for Missing Companies...")

# Map Tickers -> ISINs
# We convert the tickers (e.g., 'MAS') to ISINs (e.g., 'US...') because your price file uses ISINs.
print("   - Converting Tickers to ISINs...")
ticker_to_isin = {}
batch_size = 1000

for i in range(0, len(needed_tickers), batch_size):
    batch = needed_tickers[i:i+batch_size]
    try:
        # Ask LSEG for the ISIN for each ticker in the batch
        data = ld.get_data(universe=batch, fields=['TR.ISIN'])
        if isinstance(data, pd.DataFrame):
            for idx, row in data.iterrows():
                if row['TR.ISIN']:
                    ticker_to_isin[row['Instrument']] = row['TR.ISIN']
    except:
        pass

# Check gaps: Identify which ISINs from our checklist are missing from the price file.
existing_isins = set(df_price.columns)
missing_tickers = [t for t, i in ticker_to_isin.items() if i not in existing_isins]

# Fetch Missing Data
# If we found missing companies, we download their price history now.
if missing_tickers:
    print(f"   - Found {len(missing_tickers)} missing companies. Downloading history...")
    try:
        new_prices = ld.get_history(
            universe=missing_tickers,
            fields=['TR.PriceClose'],
            interval='1d',
            start='2000-01-01'
        )
        if not new_prices.empty:
            # Rename columns from Ticker -> ISIN so they match the main file
            new_prices.rename(columns={t: ticker_to_isin.get(t, t) for t in new_prices.columns}, inplace=True)
            # Ensure dates match the main file format
            new_prices.index = pd.to_datetime(new_prices.index).normalize()
            # Merge the new data into the main price dataframe
            df_price = df_price.join(new_prices, how='outer')
            print("   - Merged new data successfully.")
    except Exception as e:
        print(f"   [Warning] Could not fetch missing data: {e}")

# Final Sort
# We sort by date to ensure the timeline is correct.
# NOTE: Forward fill (.ffill()) is NOT applied to prices here, as requested.
df_price = df_price.sort_index()

# ==========================================
# 5. STRUCTURE & GET NAMES
# ==========================================
print("\n5. Structuring Final Dataset...")

# Reset index: Moves 'Date' from the index to a regular column
df_long = df_price.reset_index()
df_long = df_long.rename(columns={df_long.columns[0]: 'Date'})

# Melt (Wide -> Long): Pivots the data from a wide grid to a long list.
# Before: Date | AAPL | MSFT
# After:  Date | Ticker | Price
print("   - Pivoting data (this may take a moment)...")
df_long = df_long.melt(id_vars='Date', var_name='ISIN', value_name='Price')
df_long = df_long.dropna(subset=['Price'])

# Fetch Names for ALL ISINs
# We ask LSEG for the human-readable name (e.g., 'Apple Inc') for every ISIN.
print("   - Fetching Constituent Names from LSEG...")
unique_isins = df_long['ISIN'].unique().tolist()
isin_to_name = {}

for i in range(0, len(unique_isins), batch_size):
    batch = unique_isins[i:i+batch_size]
    try:
        name_data = ld.get_data(universe=batch, fields=['TR.CommonName'])
        if isinstance(name_data, pd.DataFrame):
            for idx, row in name_data.iterrows():
                isin_to_name[row['Instrument']] = row['Company Common Name']
    except:
        pass

# Apply Names: Create a new column with the readable name.
df_long['Company_Name'] = df_long['ISIN'].map(isin_to_name).fillna(df_long['ISIN'])

# ==========================================
# 6. SAVE
# ==========================================
output_path = os.path.join(script_dir, 'FIXEDPRICEDATA.csv')
print(f"\n6. Saving to: {output_path}")

# Reorder columns for the final output
df_long = df_long[['Date', 'Company_Name', 'ISIN', 'Price']]
df_long.to_csv(output_path, index=False)

# ==========================================
# 7. FINAL REPORT
# ==========================================
print("\n" + "="*50)
print("              FINAL DATA REPORT")
print("="*50)

total_rows = len(df_long)
total_companies = df_long['ISIN'].nunique()
start_date = df_long['Date'].min().strftime('%Y-%m-%d')
end_date = df_long['Date'].max().strftime('%Y-%m-%d')

print(f"Total Data Points:     {total_rows:,}")
print(f"Total Constituents:    {total_companies}")
print(f"Time Period:           {start_date} to {end_date}")
print("-" * 50)
print("COLUMN DEFINITIONS:")
print("[#1] Date:         Trading day (normalized).")
print("[#2] Company_Name: Official name fetched from LSEG.")
print("[#3] ISIN:         Unique ID matching Earnings file.")
print("[#4] Price:        Adjusted Closing Price (Raw, No Forward Fill).")
print("="*50)
print("\n[SUCCESS] Finished.")