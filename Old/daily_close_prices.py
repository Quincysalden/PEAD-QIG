"""
LSEG Daily Close Prices (RAW / Unadjusted)
==========================================
Output columns (CSV):
  - Ticker
  - Date
  - Close

Terminal output: short summary only.
"""

import lseg.data as ld
import pandas as pd
import time
from datetime import date, timedelta


# -----------------------------
# CONFIG
# -----------------------------
CHUNK_SIZE = 25
SLEEP_S = 0.10
OUTPUT_CSV = "daily_close_prices.csv"

DAYS_BACK = 30
TICKERS = ["AAPL.O", "MSFT.O", "NVDA.O"]


# -----------------------------
# HELPERS
# -----------------------------
def _chunk_list(xs, chunk_size):
    for i in range(0, len(xs), chunk_size):
        yield xs[i:i + chunk_size]


def fetch_daily_close_raw(
    tickers,
    start_date: date,
    end_date: date,
    chunk_size: int = 25,
    sleep_s: float = 0.10,
):
    """
    Returns DataFrame with columns:
      Ticker, Date, Close
    """
    if isinstance(tickers, str):
        tickers = [tickers]
    tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "Date", "Close"])

    fields = [
        "TR.PriceClose.date",
        "TR.PriceClose",
    ]

    params = {
        "SDate": start_date.isoformat(),
        "EDate": end_date.isoformat(),
        "Frq": "D",
    }

    parts = []
    for tick_chunk in _chunk_list(tickers, chunk_size):
        try:
            df = ld.get_data(universe=tick_chunk, fields=fields, parameters=params)
            if df is not None and not df.empty:
                df = df.copy()
                df = df.iloc[:, :3]
                df.columns = ["Ticker", "Date", "Close"]
                parts.append(df)
        except Exception:
            pass
        time.sleep(sleep_s)

    if not parts:
        return pd.DataFrame(columns=["Ticker", "Date", "Close"])

    out = pd.concat(parts, ignore_index=True)

    # Clean
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
    out.dropna(subset=["Ticker", "Date", "Close"], inplace=True)

    # One row per (Ticker, Date)
    out.sort_values(["Ticker", "Date"], inplace=True)
    out.drop_duplicates(subset=["Ticker", "Date"], keep="last", inplace=True)

    # Newest first per ticker
    out.sort_values(["Ticker", "Date"], ascending=[True, False], inplace=True)
    out.reset_index(drop=True, inplace=True)

    return out


# -----------------------------
# RUN SCRIPT
# -----------------------------
def main():
    tickers = [t.strip() for t in TICKERS if isinstance(t, str) and t.strip()]
    tickers_requested = len(tickers)

    end_dt = date.today()
    start_dt = end_dt - timedelta(days=DAYS_BACK)

    ld.open_session()

    df = fetch_daily_close_raw(
        tickers=tickers,
        start_date=start_dt,
        end_date=end_dt,
        chunk_size=CHUNK_SIZE,
        sleep_s=SLEEP_S,
    )

    df.to_csv(OUTPUT_CSV, index=False)

    tickers_returned = df["Ticker"].nunique() if not df.empty else 0
    rows = len(df)

    if rows > 0:
        dmin = df["Date"].min()
        dmax = df["Date"].max()
        coverage = f"{dmin} → {dmax}"
    else:
        coverage = "n/a"

    print(f"- Tickers requested: {tickers_requested}")
    print(f"- Tickers returned:  {tickers_returned}")
    print(f"- Rows (daily):      {rows}")
    print(f"- Date coverage:     {coverage}")
    print(f"- CSV saved as:      {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
