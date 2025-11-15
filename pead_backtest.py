import requests
import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

## Data imports

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


def flatten_all_results(all_results, max_weeks=6):
    """
    Flatten the nested all_results dictionary into a single DataFrame.
    Each row = one earnings event for one ticker.

    Parameters
    ----------
    all_results : dict
        Output of group_results()
        Format: {ticker: [{'earn_date': date, 'weekly_AR': df, 'first_2day': x, 'quarter': q, 'year': y}, ...]}
    max_weeks : int, default=6
        Maximum number of weeks of abnormal returns to include in flattened table.

    Returns
    -------
    pd.DataFrame
        Columns: ['ticker', 'earn_date', 'first_2day', 'quarter', 'year', 'week1', 'week2', ...]
    """
    records = []

    for ticker, events in all_results.items():
        for e in events:
            weekly_ar = e['weekly_AR']['AR_weekly'].tolist()
            # Pad or truncate so each event has consistent week columns
            weekly_ar = (weekly_ar + [np.nan] * max_weeks)[:max_weeks]

            records.append({
                'ticker': ticker,
                'earn_date': e.get('earn_date'),
                'first_2day': e.get('first_2day'),
                'quarter': e.get('quarter'),
                'year': e.get('year'),
                **{f'week{i+1}': weekly_ar[i] for i in range(max_weeks)}
            })

    df = pd.DataFrame(records)

    # Optional: Add a total cumulative AR column
    df['total_AR'] = df[[f'week{i+1}' for i in range(max_weeks)]].sum(axis=1, skipna=True)
    return df


def add_buy_signals_to_prices(price_data, flat_results, pct_threshold=0.01, lookback_events=300):
    """
    Generate threshold-based buy signals and merge into price data.

    Parameters:
    - price_data: DataFrame with ['date', 'ticker', ...] containing price information
    - flat_results: DataFrame with ['ticker', 'earn_date', 'first_2day']
    - pct_threshold: float, top fraction to trigger a buy (default 0.01 for top 1%)
    - lookback_events: int, number of previous events to compute threshold

    Returns:
    - merged: price_data DataFrame with added 'buy_signal' column
    """
    # --- Step 1: Generate buy signals ---
    df = flat_results.copy()
    
    # Keep relevant columns and drop NaNs
    df = df[['ticker', 'earn_date', 'first_2day']].sort_values('earn_date')
    df = df[df['first_2day'].notna()]

    # Compute rolling threshold
    d_thresh_list = []
    for i in range(len(df)):
        if i < lookback_events:
            d_thresh_list.append(np.nan)
        else:
            recent = df.iloc[i - lookback_events:i]
            threshold = np.nanpercentile(recent['first_2day'], 100 * (1 - pct_threshold))
            d_thresh_list.append(threshold)
    df['d_thresh'] = d_thresh_list

    # Generate buy signal and buy date
    df['buy_signal'] = np.where(df['first_2day'] > df['d_thresh'], 1, 0)
    df['buy_date'] = df['earn_date'] + pd.Timedelta(days=1)

    # --- Step 2: Merge with price data ---
    merged = price_data.merge(
        df[['ticker', 'buy_date', 'buy_signal']],
        left_on=['ticker', 'date'],
        right_on=['ticker', 'buy_date'],
        how='left'
    )

    merged['buy_signal'] = merged['buy_signal'].fillna(0).astype(int)
    merged = merged.sort_values(['date', 'ticker']).reset_index(drop=True)

    return merged


def backtest_daily_returns(merged, max_positions=5, hold_days=30, initial_cash=1.0):
    """
    Backtest using daily returns and daily rebalancing.
    
    Parameters:
    - merged: DataFrame with ['date', 'ticker', 'close', 'buy_signal']
    - max_positions: max simultaneous holdings
    - hold_days: minimum days to hold each position
    - initial_cash: starting portfolio value
    """
    merged = merged.sort_values(['date', 'ticker'])
    dates = merged['date'].drop_duplicates().sort_values().to_numpy()
    
    portfolio = []  # list of dicts: ticker, buy_date, held_days, eligible_to_sell
    daily_values = []  # list of dicts: date, portfolio_value
    portfolio_value = initial_cash
    yesterday_data = None
    
    for i, date in enumerate(dates):
        today_data = merged[merged['date'] == date].set_index('ticker')
        
        # --- Step 1: Compute daily return if portfolio is not empty ---
        if portfolio and yesterday_data is not None:
            N = len(portfolio)
            weights = {p['ticker']: 1/N for p in portfolio}
            r_portfolio = 0.0
            for pos in portfolio:
                ticker = pos['ticker']
                if ticker in yesterday_data.index:
                    prev_close = yesterday_data.loc[ticker, 'close']
                    today_close = today_data.loc[ticker, 'close']
                    r = (today_close / prev_close) - 1
                    r_portfolio += weights[ticker] * r
            portfolio_value *= (1 + r_portfolio)
        
        daily_values.append({'date': date, 'portfolio_value': portfolio_value})
        
        # --- Step 2: Update position eligibility ---
        for pos in portfolio:
            pos['held_days'] = (date - pos['buy_date']).astype('timedelta64[D]').astype(int)
            pos['eligible_to_sell'] = pos['held_days'] >= hold_days
        
        # --- Step 3: Process today's buy signals ---
        buy_signals = today_data[today_data['buy_signal'] == 1]
        for ticker, signal in buy_signals.iterrows():
            # Skip if already holding
            if any(p['ticker'] == ticker for p in portfolio):
                continue
            
            # If portfolio full, sell oldest eligible position
            if len(portfolio) >= max_positions:
                eligible = [p for p in portfolio if p['eligible_to_sell']]
                if eligible:
                    oldest = min(eligible, key=lambda x: x['buy_date'])
                    portfolio.remove(oldest)
                else:
                    continue  # cannot buy new position
            
            # Buy the new stock
            portfolio.append({
                'ticker': ticker,
                'buy_date': date,
                'held_days': 0,
                'eligible_to_sell': False
            })
        
        # --- Step 4: Update yesterday_data ---
        yesterday_data = today_data.copy()
    
    return pd.DataFrame(daily_values)


# %%


def analyze_strategy_vs_spy(daily_portfolio, spy_close, log_scale=False, risk_free_rate=0.0):
    """
    Compare strategy daily equity vs SPY, plot, and compute metrics including Sharpe ratio,
    and display metrics on the plot.

    Parameters:
    - daily_portfolio: DataFrame with ['date', 'portfolio_value', 'num_positions']
    - spy_close: DataFrame with ['Date', 'Close']
    - log_scale: bool, if True, use logarithmic y-axis
    - risk_free_rate: annualized risk-free rate for Sharpe calculation

    Returns:
    - merged_df: DataFrame with strategy and SPY equity
    - metrics: dict with performance metrics
    """
    daily_portfolio['date'] = pd.to_datetime(daily_portfolio['date'])
    spy_close['date'] = pd.to_datetime(spy_close['Date'])
    
    # Active period
    first_active_idx = daily_portfolio[daily_portfolio['num_positions'] > 0].index[0]
    start_idx = max(first_active_idx - 1, 0)
    start_date = daily_portfolio['date'].iloc[start_idx]
    end_date = daily_portfolio['date'].iloc[-1]
    
    spy_filtered = spy_close[(spy_close['date'] >= start_date) & (spy_close['date'] <= end_date)].copy()
    spy_filtered['spy_equity'] = spy_filtered['Close'] / spy_filtered['Close'].iloc[0]
    
    merged_df = daily_portfolio.merge(
        spy_filtered[['date', 'spy_equity']],
        on='date',
        how='left'
    )
    
    # --- Compute metrics ---
    def compute_metrics(equity_series):
        total_return = equity_series.iloc[-1] / equity_series.iloc[0] - 1
        n_years = (merged_df['date'].iloc[-1] - merged_df['date'].iloc[0]).days / 365.25
        cagr = (equity_series.iloc[-1] / equity_series.iloc[0]) ** (1 / n_years) - 1
        running_max = equity_series.cummax()
        drawdowns = (equity_series - running_max) / running_max
        max_drawdown = drawdowns.min()
        
        daily_returns = equity_series.pct_change().dropna()
        volatility = daily_returns.std() * np.sqrt(252)
        sharpe = (daily_returns.mean() * 252 - risk_free_rate) / volatility if volatility != 0 else np.nan
        calmar = cagr / abs(max_drawdown) if max_drawdown != 0 else np.nan
        
        return {
            'total_return': total_return,
            'CAGR': cagr,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'sharpe_ratio': sharpe,
            'calmar_ratio': calmar
        }
    
    metrics = {
        'strategy': compute_metrics(merged_df['portfolio_value'].iloc[start_idx:]),
        'SPY': compute_metrics(merged_df['spy_equity'].iloc[start_idx:])
    }
    
    # --- Plot with metrics in legend ---
    strategy_label = (
        f"Strategy\n"
        f"Total: {metrics['strategy']['total_return']:.2f}, "
        f"CAGR: {metrics['strategy']['CAGR']:.2f}, "
        f"MaxDD: {metrics['strategy']['max_drawdown']:.2f}, "
        f"Sharpe: {metrics['strategy']['sharpe_ratio']:.2f}"
    )
    
    spy_label = (
        f"SPY\n"
        f"Total: {metrics['SPY']['total_return']:.2f}, "
        f"CAGR: {metrics['SPY']['CAGR']:.2f}, "
        f"MaxDD: {metrics['SPY']['max_drawdown']:.2f}, "
        f"Sharpe: {metrics['SPY']['sharpe_ratio']:.2f}"
    )
    
    plt.figure(figsize=(12, 6))
    plt.plot(
        merged_df['date'].iloc[start_idx:],
        merged_df['portfolio_value'].iloc[start_idx:] / merged_df['portfolio_value'].iloc[start_idx],
        label=strategy_label,
        color='blue'
    )
    plt.plot(
        merged_df['date'].iloc[start_idx:],
        merged_df['spy_equity'].iloc[start_idx:],
        label=spy_label,
        color='orange',
        linestyle='--'
    )
    plt.xlabel('Date')
    plt.ylabel('Cumulative Equity')
    plt.title('Strategy vs SPY')
    plt.legend(loc='upper left', fontsize=10)
    plt.grid(True)
    if log_scale:
        plt.yscale('log')
    plt.show()
    
    return merged_df, metrics





def plot_cash_fraction(merged_df, trim_start=0, trim_end=0, plot=True):
    """
    Calculate cash fraction, AUC, and average cash fraction.
    Optionally trims start/end of the data for plotting.

    Parameters
    ----------
    merged_df : pd.DataFrame
        Must contain columns ['date', 'cash', 'portfolio_value'].
    trim_start : int
        Number of rows to trim from start.
    trim_end : int
        Number of rows to trim from end.
    plot : bool
        Whether to plot the cash fraction curve.

    Returns
    -------
    auc_cash : float
        Area under the cash fraction curve (cash-days).
    avg_cash_fraction : float
        Average cash fraction over the period.
    trimmed_df : pd.DataFrame
        DataFrame used for plotting (trimmed if applicable).
    """
    # Compute cash fraction
    merged_df = merged_df.copy()
    merged_df['cash_fraction'] = merged_df['cash'] / merged_df['portfolio_value']

    # Trim if requested
    if trim_start > 0 or trim_end > 0:
        trimmed_df = merged_df.iloc[trim_start: len(merged_df) - trim_end]
    else:
        trimmed_df = merged_df

    # Plot
    if plot:
        plt.figure(figsize=(12, 6))
        plt.plot(trimmed_df['date'], trimmed_df['cash_fraction'], label='Cash Fraction', color='green')
        plt.xlabel('Date')
        plt.ylabel('Fraction of Portfolio in Cash')
        plt.title('Portfolio Cash Fraction (Trimmed Start/End)')
        plt.grid(True)
        plt.legend()
        plt.show()

    # Compute AUC
    auc_cash = np.trapz(merged_df['cash_fraction'], dx=1)

    # Average cash fraction
    avg_cash_fraction = auc_cash / len(merged_df)

    return auc_cash, avg_cash_fraction, trimmed_df


def backtest_cash_trading(
    merged,
    max_positions=5,
    hold_days=30,
    initial_cash=5000.0
):
    """
    Realistic daily signal-driven backtest.
    
    Parameters
    ----------
    merged : DataFrame
        Must contain columns ['date', 'ticker', 'close', 'buy_signal']
        'date' must be datetime64, data sorted by date is preferred.
    max_positions : int
        Maximum number of open positions.
    hold_days : int
        Minimum number of calendar days to hold before selling.
    initial_cash : float
        Starting cash balance.
    
    Returns
    -------
    daily_values : DataFrame
        Portfolio value each day.
    trades : DataFrame
        Log of buys and sells.
    """
    
    merged = merged.sort_values(['date', 'ticker']).reset_index(drop=True)
    dates = merged['date'].drop_duplicates().sort_values().to_numpy()

    # --- Portfolio state ---
    portfolio = {}   # ticker → {shares, buy_date, buy_price}
    cash = float(initial_cash)

    daily_values = []
    trades = []

    for date in dates:
        today_data = merged[merged['date'] == date].set_index('ticker')

        # ------------------------------------------------------------
        # 1) Mark-to-market portfolio
        # ------------------------------------------------------------
        market_value = 0.0
        for t, pos in portfolio.items():
            if t in today_data.index:
                mv = pos['shares'] * today_data.loc[t, 'close']
            else:
                mv = pos['shares'] * pos['buy_price']
            market_value += mv

        portfolio_value = cash + market_value

        daily_values.append({
            'date': date,
            'cash': cash,
            'market_value': market_value,
            'portfolio_value': portfolio_value,
            'num_positions': len(portfolio)
        })

        # ------------------------------------------------------------
        # 2) Determine which positions are sell-eligible
        # ------------------------------------------------------------
        eligible = {}
        for t, pos in portfolio.items():
            held_days = (pd.Timestamp(date) - pd.Timestamp(pos['buy_date'])).days
            eligible[t] = held_days >= hold_days

        # ------------------------------------------------------------
        # 3) Process today's buy signals (SEQUENTIALLY)
        # ------------------------------------------------------------
        signals = today_data[today_data['buy_signal'] == 1]

        for ticker, row in signals.sort_index().iterrows():
            price = row['close']

            # Already holding → skip
            if ticker in portfolio:
                continue

            # If full, free a slot by selling the oldest eligible
            if len(portfolio) >= max_positions:
                eligible_tickers = [t for t, ok in eligible.items() if ok]

                if eligible_tickers:
                    oldest = min(
                        eligible_tickers,
                        key=lambda t: portfolio[t]['buy_date']
                    )

                    # S E L L
                    sell_price = today_data.loc[oldest, 'close']
                    shares = portfolio[oldest]['shares']
                    notional = shares * sell_price

                    cash_before = cash
                    cash += notional

                    trades.append({
                        'date': date,
                        'ticker': oldest,
                        'action': 'SELL',
                        'shares': shares,
                        'price': sell_price,
                        'notional': notional,
                        'cash_before': cash_before,
                        'cash_after': cash,
                        'reason': 'sell_to_free_slot'
                    })

                    del portfolio[oldest]
                    del eligible[oldest]

                else:
                    # Cannot free a slot → cannot buy
                    continue

            # --------------------------------------------------------
            # BUY using equal-dollar slot sizing:
            # invest = portfolio_value / max_positions
            # --------------------------------------------------------
            market_value_after = 0.0
            for t, pos in portfolio.items():
                if t in today_data.index:
                    market_value_after += pos['shares'] * today_data.loc[t, 'close']
                else:
                    market_value_after += pos['shares'] * pos['buy_price']

            portfolio_value_after = cash + market_value_after
            target_dollars = portfolio_value_after / max_positions
            dollars_to_invest = min(target_dollars, cash)

            if dollars_to_invest <= 0:
                continue

            # Fractional shares allowed
            shares = dollars_to_invest / price
            notional = shares * price

            cash_before = cash
            cash -= notional

            portfolio[ticker] = {
                'shares': shares,
                'buy_date': date,
                'buy_price': price
            }

            trades.append({
                'date': date,
                'ticker': ticker,
                'action': 'BUY',
                'shares': shares,
                'price': price,
                'notional': notional,
                'cash_before': cash_before,
                'cash_after': cash,
                'reason': 'buy_signal'
            })

    # ------------------------------------------------------------
    # 4) Final liquidation on the last date
    # ------------------------------------------------------------
    last_date = dates[-1]
    last_prices = merged[merged['date'] == last_date].set_index('ticker')

    for t, pos in list(portfolio.items()):
        if t in last_prices.index:
            price = last_prices.loc[t, 'close']
        else:
            price = pos['buy_price']

        shares = pos['shares']
        notional = shares * price

        cash_before = cash
        cash += notional

        trades.append({
            'date': last_date,
            'ticker': t,
            'action': 'SELL',
            'shares': shares,
            'price': price,
            'notional': notional,
            'cash_before': cash_before,
            'cash_after': cash,
            'reason': 'liquidation_end'
        })

        del portfolio[t]

    # Final snapshot
    daily_values.append({
        'date': last_date,
        'cash': cash,
        'market_value': 0.0,
        'portfolio_value': cash,
        'num_positions': 0
    })

    return pd.DataFrame(daily_values), pd.DataFrame(trades)



def analyze_trades(trades_df):
    """
    Analyze trades to compute holding time, action ratios, cash usage, PnL metrics, and ticker concentration.

    Parameters:
    - trades_df: DataFrame with columns including:
        ['date', 'ticker', 'action', 'cash_before', 'cash_after', 'price', 'shares']

    Returns:
    - summary: dict with all trade-level statistics
    - holding_df: DataFrame with individual trade holding times and PnL
    """
    trades = trades_df.copy()
    trades['date'] = pd.to_datetime(trades['date'])
    trades['action'] = trades['action'].str.upper()
    
    # --- Pair BUY and SELL trades ---
    buys = trades[trades['action'] == 'BUY']
    sells = trades[trades['action'] == 'SELL']
    holding_times = []

    for ticker in buys['ticker'].unique():
        buy_rows = buys[buys['ticker'] == ticker].sort_values('date')
        sell_rows = sells[sells['ticker'] == ticker].sort_values('date')
        sell_list = sell_rows.to_dict('records')

        for _, buy in buy_rows.iterrows():
            # Find first sell after buy
            sell_idx = next((i for i, s in enumerate(sell_list) if s['date'] > buy['date']), None)
            if sell_idx is not None:
                sell = sell_list.pop(sell_idx)
                holding_days = (sell['date'] - buy['date']).days

                # Compute PnL
                pnl = (sell['price'] - buy['price']) * buy['shares']
                return_pct = pnl / (buy['price'] * buy['shares'])

                holding_times.append({
                    'ticker': ticker,
                    'buy_date': buy['date'],
                    'sell_date': sell['date'],
                    'holding_days': holding_days,
                    'cash_used': buy['cash_before'] - buy['cash_after'],
                    'pnl': pnl,
                    'return_pct': return_pct
                })

    holding_df = pd.DataFrame(holding_times)

    # --- Compute summary statistics ---
    summary = {}
    
    if not holding_df.empty:
        # Holding times
        summary['average_holding_days'] = holding_df['holding_days'].mean()
        summary['median_holding_days'] = holding_df['holding_days'].median()
        summary['max_holding_days'] = holding_df['holding_days'].max()
        summary['min_holding_days'] = holding_df['holding_days'].min()
        summary['total_trades'] = len(holding_df)
        summary['unique_tickers_traded'] = holding_df['ticker'].nunique()

        # Action ratios
        summary['total_buys'] = len(buys)
        summary['total_sells'] = len(sells)
        summary['buy_ratio'] = len(buys) / (len(buys) + len(sells))
        summary['sell_ratio'] = len(sells) / (len(buys) + len(sells))

        # Cash usage / position sizing
        summary['avg_cash_used'] = holding_df['cash_used'].mean()
        summary['max_cash_used'] = holding_df['cash_used'].max()
        summary['min_cash_used'] = holding_df['cash_used'].min()

        # Win/Loss metrics
        wins = holding_df[holding_df['pnl'] > 0]
        losses = holding_df[holding_df['pnl'] <= 0]
        summary['win_rate'] = len(wins) / len(holding_df)
        summary['avg_win'] = wins['pnl'].mean() if not wins.empty else 0
        summary['avg_loss'] = losses['pnl'].mean() if not losses.empty else 0
        summary['largest_win'] = wins['pnl'].max() if not wins.empty else 0
        summary['largest_loss'] = losses['pnl'].min() if not losses.empty else 0

        # --- Ticker concentration ---
        trade_counts = holding_df['ticker'].value_counts()
        summary['top_5_tickers_by_trades'] = trade_counts.head(5).to_dict()
        
        pnl_by_ticker = holding_df.groupby('ticker')['pnl'].sum().sort_values(ascending=False)
        summary['top_5_tickers_by_pnl'] = pnl_by_ticker.head(5).to_dict()

    else:
        summary = {
            'average_holding_days': None,
            'median_holding_days': None,
            'max_holding_days': None,
            'min_holding_days': None,
            'total_trades': 0,
            'unique_tickers_traded': 0,
            'total_buys': 0,
            'total_sells': 0,
            'buy_ratio': None,
            'sell_ratio': None,
            'avg_cash_used': None,
            'max_cash_used': None,
            'min_cash_used': None,
            'win_rate': None,
            'avg_win': None,
            'avg_loss': None,
            'largest_win': None,
            'largest_loss': None,
            'top_5_tickers_by_trades': None,
            'top_5_tickers_by_pnl': None
        }

    return summary, holding_df
# %%

### Data Prep

# Step 1: Create earnings windows
earnings_windows = create_earnings_windows(price_data, earnings_data, tickers)

# Step 2: Group results (compute abnormal returns etc.)
all_results = group_results(earnings_windows, tickers, spy_close)

# %%


### Full run

max_pos = 10
hold_days = 42
init_cash = 1
lookback_events=250
pct_threshold = 0.05

flat_results = flatten_all_results(all_results)
merged = add_buy_signals_to_prices(price_data, flat_results, pct_threshold=pct_threshold, lookback_events=lookback_events)

cash_portfolio, trades = backtest_cash_trading(merged, max_positions= max_pos, hold_days = hold_days, initial_cash=init_cash)

merged_df, metrics = analyze_strategy_vs_spy(cash_portfolio, spy_close, log_scale=False, risk_free_rate = 0.042)

print(metrics)

trades_summary, holding_df = analyze_trades(trades)

print(trades_summary)