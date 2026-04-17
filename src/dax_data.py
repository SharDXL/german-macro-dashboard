"""
dax_data.py
-----------
Fetches DAX and European equity data via yfinance.
Tracks sector performance to overlay on macro regime.

DAX Sector ETFs (available on yfinance):
  Sector ETFs are proxied via STOXX Europe 600 sector indices
  or German-listed ETFs where available.

KEY LEARNING: German equity market structure
  - DAX40 = 40 largest German companies (blue chip)
  - MDAX  = 50 mid-cap German companies
  - STOXX Europe 600 = 600 largest European companies (benchmark for pan-European)
  - Key sectors for Germany: Autos (VW, BMW, Mercedes), Industrials (Siemens),
    Chemicals (BASF), Financials (Deutsche Bank, Allianz), Healthcare (Bayer)

Regime-sector relationship (what outperforms when):
  Expansion:   Autos, Industrials, Financials, Materials
  Slowdown:    Healthcare, Consumer Staples, Quality Tech
  Contraction: Utilities, Gold, Short-duration bonds
  Recovery:    Early cyclicals, Autos, Materials, Small caps
"""

import yfinance as yf
import pandas as pd
from datetime import date, timedelta


# Key tickers to track
# DAX major constituents as regime proxies
TICKERS = {
    # Broad indices
    "DAX"         : "^GDAXI",        # DAX40 index
    "STOXX600"    : "^STOXX",        # STOXX Europe 600

    # German sector proxies (ETFs listed on Xetra/Frankfurt)
    # iShares STOXX Europe 600 sector ETFs
    "Autos"       : "EXV1.DE",       # iShares STOXX Auto & Parts
    "Banks"       : "EXV6.DE",       # iShares STOXX Banks
    "Industrials" : "EXH4.DE",       # iShares STOXX Industrials
    "Healthcare"  : "EXV4.DE",       # iShares STOXX Healthcare
    "Utilities"   : "EXV5.DE",       # iShares STOXX Utilities
    "Technology"  : "EXV3.DE",       # iShares STOXX Technology
    "Materials"   : "EXV7.DE",       # iShares STOXX Basic Resources

    # Key German individual stocks (for qualitative understanding)
    "Siemens"     : "SIE.DE",
    "BASF"        : "BAS.DE",
    "BMW"         : "BMW.DE",
    "Deutsche_Bank": "DBK.DE",
    "Allianz"     : "ALV.DE",
}

# Sector ETFs only (for regime overlay)
SECTOR_TICKERS = {k: v for k, v in TICKERS.items()
                  if k in ["Autos", "Banks", "Industrials", "Healthcare",
                            "Utilities", "Technology", "Materials"]}


def fetch_returns(start_date: str = "2022-01-01",
                  tickers: dict = None) -> pd.DataFrame:
    """
    Fetch daily price data and compute returns.

    Returns pd.DataFrame of daily close prices, columns = ticker names.
    """
    if tickers is None:
        tickers = TICKERS

    symbols = list(tickers.values())
    names   = list(tickers.keys())

    print(f"Downloading {len(symbols)} tickers from Yahoo Finance...")
    raw = yf.download(symbols, start=start_date, auto_adjust=True, progress=False)

    # Extract Close prices
    if "Close" in raw.columns.get_level_values(0):
        prices = raw["Close"]
    else:
        prices = raw  # single ticker fallback

    # Rename to friendly names
    rename_map = {v: k for k, v in tickers.items()}
    prices = prices.rename(columns=rename_map)

    return prices


def compute_performance(prices: pd.DataFrame,
                        window_months: int = 1) -> pd.DataFrame:
    """
    Compute rolling returns over various windows.

    Returns DataFrame with columns:
      name, price, return_1m, return_3m, return_6m, return_ytd
    """
    today = prices.index[-1]
    periods = {
        "return_1m"  : 21,
        "return_3m"  : 63,
        "return_6m"  : 126,
    }

    result = {}
    for name in prices.columns:
        s = prices[name].dropna()
        if len(s) == 0:
            continue

        row = {"price": round(s.iloc[-1], 2)}
        for label, days in periods.items():
            if len(s) > days:
                ret = (s.iloc[-1] / s.iloc[-1 - days] - 1) * 100
                row[label] = round(ret, 2)
            else:
                row[label] = None

        # YTD
        ytd_start = s[s.index.year == today.year]
        if len(ytd_start) > 0:
            ytd_ret = (s.iloc[-1] / ytd_start.iloc[0] - 1) * 100
            row["return_ytd"] = round(ytd_ret, 2)
        else:
            row["return_ytd"] = None

        result[name] = row

    return pd.DataFrame(result).T


def get_sector_regime_alignment(prices: pd.DataFrame) -> dict:
    """
    Compare sector performance over last 3 months to identify
    which regime the market is 'pricing in'.

    Logic: cyclicals (Autos, Banks, Industrials) outperforming defensives
    (Utilities, Healthcare) suggests market is pricing in expansion.
    The reverse suggests contraction expectations.
    """
    perf = compute_performance(prices)

    cyclicals   = ["Autos", "Banks", "Industrials", "Materials"]
    defensives  = ["Utilities", "Healthcare"]

    avail_c = [s for s in cyclicals if s in perf.index and perf.loc[s, "return_3m"] is not None]
    avail_d = [s for s in defensives if s in perf.index and perf.loc[s, "return_3m"] is not None]

    if not avail_c or not avail_d:
        return {"market_regime_signal": "insufficient data"}

    cyclical_avg  = perf.loc[avail_c, "return_3m"].mean()
    defensive_avg = perf.loc[avail_d, "return_3m"].mean()
    spread = cyclical_avg - defensive_avg

    if spread > 5:
        signal = "Cyclical outperformance → market pricing Expansion"
    elif spread > 1:
        signal = "Mild cyclical outperformance → Slowdown/Early Recovery"
    elif spread > -3:
        signal = "Neutral → mixed macro signal"
    else:
        signal = "Defensive outperformance → market pricing Contraction/Risk-off"

    return {
        "cyclical_avg_3m"    : round(cyclical_avg, 2),
        "defensive_avg_3m"   : round(defensive_avg, 2),
        "cyclical_vs_defensive_spread": round(spread, 2),
        "market_regime_signal": signal,
    }


if __name__ == "__main__":
    print("Fetching DAX/sector data...")
    prices = fetch_returns(start_date="2023-01-01", tickers=SECTOR_TICKERS)
    print(f"\nPrices shape: {prices.shape}")

    perf = compute_performance(prices)
    print("\nSector Performance:")
    print(perf.to_string())

    regime = get_sector_regime_alignment(prices)
    print(f"\nMarket Regime Signal: {regime}")

    prices.to_csv("../data/processed/dax_sector_prices.csv")
    print("\nSaved → data/processed/dax_sector_prices.csv")
