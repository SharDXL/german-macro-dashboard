"""
zew_data.py
-----------
Loads and processes ZEW Economic Sentiment data.

The ZEW Indicator of Economic Sentiment surveys ~350 financial experts
on their 6-month expectations for the German economy.
Published: 2nd Tuesday of each month.

KEY INSIGHT:
  - Values range roughly from -100 to +100
  - Zero = neutral (equal pessimists and optimists)
  - Below -20 = significant pessimism (contraction signal)
  - Above +30 = significant optimism (expansion signal)
  - MOMENTUM matters: a reading improving from -30 to -10 is bullish
    even though it's still negative

Two series:
  - Expectations (6-month forward outlook) — the headline
  - Current Conditions (Lage) — current assessment

Data source (FRED):
  ZSXECP01DEM659N = ZEW Economic Sentiment for Germany
  CSCICP03DEM665S = ZEW Current Conditions

Free FRED API: https://fred.stlouisfed.org/docs/api/api_key.html
"""

import pandas as pd
import requests
import os


FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "zew_expectations"  : "ZSXECP01DEM659N",
    "zew_current"       : "CSCICP03DEM665S",
}


def fetch_zew_from_fred(api_key: str, start_date: str = "2019-01-01") -> pd.DataFrame:
    """
    Fetch ZEW data from FRED.

    Returns pd.DataFrame indexed by date with columns:
      zew_expectations, zew_current
    """
    dfs = []
    for name, series_id in FRED_SERIES.items():
        params = {
            "series_id"        : series_id,
            "api_key"          : api_key,
            "file_type"        : "json",
            "observation_start": start_date,
        }
        resp = requests.get(FRED_BASE, params=params, timeout=15)
        resp.raise_for_status()

        data = resp.json()["observations"]
        df = pd.DataFrame(data)[["date", "value"]].copy()
        df.columns = ["date", name]
        df["date"] = pd.to_datetime(df["date"])
        df[name] = pd.to_numeric(df[name], errors="coerce")
        df = df.set_index("date")
        dfs.append(df)
        print(f"  ✓ FRED: {name} ({len(df)} obs)")

    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.join(df, how="outer")

    return merged.sort_index().dropna()


def compute_zew_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived signals:
      - zew_momentum_3m : 3-month change in expectations
      - zew_divergence  : expectations minus current (forward vs. present gap)
      - zew_trend       : 'improving' / 'deteriorating' / 'stable'
    """
    df = df.copy()
    df["zew_momentum_3m"] = df["zew_expectations"].diff(3)
    df["zew_divergence"]  = df["zew_expectations"] - df["zew_current"]

    def trend(row):
        if row["zew_momentum_3m"] > 5:
            return "improving"
        elif row["zew_momentum_3m"] < -5:
            return "deteriorating"
        else:
            return "stable"

    df["zew_trend"] = df.apply(trend, axis=1)
    return df


def get_zew_regime_score(df: pd.DataFrame, as_of: str = None) -> dict:
    """
    Return ZEW contribution to regime classifier (0–25 scale).

    Scoring:
      Expectations level: >30 → 10pts | 10–30 → 7pts | 0–10 → 4pts | <0 → 1pt
      Momentum:           improving → 10pts | stable → 5pts | deteriorating → 0pts
      Divergence:         exp > current by >10 → 5pts | else → 2pts | exp < current → 0pts

    Max score: 25
    """
    df = compute_zew_signals(df)
    row = df.iloc[-1] if as_of is None else df.loc[:as_of].iloc[-1]

    # Level score
    zew = row["zew_expectations"]
    if zew > 30:
        level_score = 10
    elif zew >= 10:
        level_score = 7
    elif zew >= 0:
        level_score = 4
    else:
        level_score = 1

    # Momentum score
    momentum_score = {"improving": 10, "stable": 5, "deteriorating": 0}.get(row["zew_trend"], 5)

    # Divergence score
    div = row["zew_divergence"]
    div_score = 5 if div > 10 else (2 if div >= 0 else 0)

    total = level_score + momentum_score + div_score

    return {
        "zew_score"          : total,
        "zew_expectations"   : round(zew, 1),
        "zew_trend"          : row["zew_trend"],
        "zew_divergence"     : round(div, 1),
        "as_of"              : str(row.name.date()),
    }


if __name__ == "__main__":
    API_KEY = "YOUR_FRED_API_KEY_HERE"

    if API_KEY != "YOUR_FRED_API_KEY_HERE":
        print("Fetching ZEW data from FRED...")
        df = fetch_zew_from_fred(API_KEY, start_date="2019-01-01")
        df = compute_zew_signals(df)
        print(df.tail(6).to_string())

        score = get_zew_regime_score(df)
        print(f"\nZEW Regime Score: {score}")

        df.to_csv("../data/processed/zew_data.csv")
        print("Saved → data/processed/zew_data.csv")
    else:
        print("Set your FRED API key. Get it free at: https://fred.stlouisfed.org/docs/api/api_key.html")
