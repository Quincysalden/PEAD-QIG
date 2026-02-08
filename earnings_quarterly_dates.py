"""
LSEG Earnings Dates(Quarterly) with Fiscal Period Labels
=============================================================
Output columns (CSV):
  - Ticker
  - EarningsDateTime (exact timestamp, includes date+time)
  - FiscalPeriod     (e.g., FY2026 Q1)

Terminal output: short summary only.
"""

import lseg.data as ld
import pandas as pd
import time
from datetime import date


# -----------------------------
# CONFIG 
# -----------------------------
CHUNK_SIZE = 25
SLEEP_S = 0.15
OUTPUT_CSV = "earnings_quarterly_dates.csv"

QUARTERS_BACK = 9
TICKERS = [
    "AAPL.O", "MSFT.O", "AMZN.O", "NVDA.O", "META.O",
    "GOOGL.O", "TSLA.O",
    "JPM.N", "JNJ.N", "XOM.N", "PG.N", "KO.N", "WMT.N", "V.N"
]


# -----------------------------
# HELPERS
# -----------------------------
def _chunk_list(xs, chunk_size):
    for i in range(0, len(xs), chunk_size):
        yield xs[i:i + chunk_size]


def _standardize_fperiod(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()

    import re
    m = re.search(r"FY\s*(\d{4})\s*Q\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"FY\s*(\d{4})\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"(\d{4})\s*Q\s*([1-4])", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(1)} Q{m.group(2)}"

    m = re.search(r"FQ\s*([1-4])\s*(\d{4})", s, flags=re.IGNORECASE)
    if m:
        return f"FY{m.group(2)} Q{m.group(1)}"

    return s


def _extract_fy(fp: str):
    if not fp:
        return None
    import re
    m = re.search(r"FY(\d{4})\s*Q[1-4]$", fp, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def fetch_quarterly_earnings_dates(
    tickers,
    quarters_back=80,
    chunk_size=25,
    sleep_s=0.15,
    drop_future=True,
):
    """
    Returns DataFrame with columns (final):
      Ticker, EarningsDateTime, FiscalPeriod
    """
    if isinstance(tickers, str):
        tickers = [tickers]
    tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "EarningsDateTime", "FiscalPeriod"])

    eps_expr = f"TR.EPSActValue(SDate=0,EDate=-{quarters_back-1},Period=FQ0,Frq=FQ)"
    fields = [
        f"{eps_expr}.Date",
        f"{eps_expr}.fperiod",
    ]

    parts = []
    for tick_chunk in _chunk_list(tickers, chunk_size):
        try:
            df = ld.get_data(universe=tick_chunk, fields=fields)
            if df is not None and not df.empty:
                df = df.copy()
                df.columns = ["Ticker", "EarningsDateTime", "FiscalPeriod"]
                parts.append(df)
        except Exception:
            pass
        time.sleep(sleep_s)

    if not parts:
        return pd.DataFrame(columns=["Ticker", "EarningsDateTime", "FiscalPeriod"])

    out = pd.concat(parts, ignore_index=True)

    # Clean
    out["EarningsDateTime"] = pd.to_datetime(out["EarningsDateTime"], errors="coerce")
    out["FiscalPeriod"] = out["FiscalPeriod"].apply(_standardize_fperiod)
    out.dropna(subset=["Ticker", "EarningsDateTime", "FiscalPeriod"], inplace=True)

    # Keep only FY#### Q# labels
    out = out[out["FiscalPeriod"].str.match(r"^FY\d{4}\sQ[1-4]$", na=False)]

    # Drop future (use date component internally)
    if drop_future:
        out = out[out["EarningsDateTime"].dt.date <= date.today()]


    # De-dupe to one row per (Ticker, FiscalPeriod)
    out.sort_values(["Ticker", "FiscalPeriod", "EarningsDateTime"], ascending=[True, True, False], inplace=True)
    out.drop_duplicates(subset=["Ticker", "FiscalPeriod"], keep="first", inplace=True)

    # Final sort: newest first per ticker
    out.sort_values(["Ticker", "EarningsDateTime"], ascending=[True, False], inplace=True)
    out.reset_index(drop=True, inplace=True)

    return out


# -----------------------------
# RUN SCRIPT
# -----------------------------
def main():
    tickers = [t.strip() for t in TICKERS if isinstance(t, str) and t.strip()]
    tickers_requested = len(tickers)

    ld.open_session()

    df = fetch_quarterly_earnings_dates(
        tickers=tickers,
        quarters_back=QUARTERS_BACK,
        chunk_size=CHUNK_SIZE,
        sleep_s=SLEEP_S,
        drop_future=True,
    )

    df.to_csv(OUTPUT_CSV, index=False)

    # Short terminal summary only
    tickers_returned = df["Ticker"].nunique() if not df.empty else 0
    rows = len(df)

    if rows > 0:
        dmin = df["EarningsDateTime"].dt.date.min()
        dmax = df["EarningsDateTime"].dt.date.max()
        coverage = f"{dmin} \u2192 {dmax}"
    else:
        coverage = "n/a"

    print(f"- Tickers requested: {tickers_requested}")
    print(f"- Tickers returned:  {tickers_returned}")
    print(f"- Rows (events):     {rows}")
    print(f"- Date coverage:     {coverage}")
    print(f"- CSV saved as:      {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
