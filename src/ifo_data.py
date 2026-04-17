"""
ifo_data.py
-----------
Loads and processes IFO Business Climate Index data.

The IFO Business Climate is Germany's most-watched leading indicator.
Published monthly by the ifo Institute (Munich).

Three components:
  - Business Climate (headline): geometric mean of current + expectations
  - Current Business Situation (Lage)
  - Business Expectations (Erwartungen)

KEY INSIGHT FOR ANALYSIS:
  - Climate > 100 = above long-run average (expansion territory)
  - Expectations > Current → recovery likely (forward-looking optimism)
  - Expectations < Current → slowdown likely (pessimism about future)
  - The GAP between expectations and current conditions is a leading signal

Data source:
  Manual CSV download from: https://www.ifo.de/en/survey/ifo-business-climate-index
  File: ifo_bci_data.csv (place in ../data/raw/)

  Alternatively, the St. Louis FRED API hosts the IFO series:
    BSCICP03DEM665S = IFO Business Climate (normalised)
    Use FRED key at: https://fred.stlouisfed.org/docs/api/fred/
"""

import pandas as pd
import os
import requests
from io import StringIO


FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs for IFO components (normalised, OECD CLI-adjusted)
FRED_SERIES = {
    "ifo_climate"      : "BSCICP03DEM665S",  # IFO Business Climate
    "ifo_current"      : "BSXNCP03DEM665S",  # Current Conditions
    "ifo_expectations" : "BSXPCP03DEM665S",  # Expectations
}


def fetch_ifo_from_fred(api_key: str, start_date: str = "2019-01-01") -> pd.DataFrame:
    """
    Fetch IFO components from FRED (free API key required).

    Get your free key at: https://fred.stlouisfed.org/docs/api/api_key.html
    Takes ~30 seconds to register.

    Parameters
    ----------
    api_key  : str  - Your FRED API key
    start_date : str - 'YYYY-MM-DD'

    Returns
    -------
    pd.DataFrame with columns: date, ifo_climate, ifo_current, ifo_expectations
    """
    dfs = []
    for name, series_id in FRED_SERIES.items():
        params = {
            "series_id"       : series_id,
            "api_key"         : api_key,
            "file_type"       : "json",
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


def load_ifo_csv(filepath: str = None) -> pd.DataFrame:
    """
    Load IFO data from manually downloaded CSV.

    Expected columns (from ifo.de download):
      Date | Business Climate | Current Situation | Expectations

    Parameters
    ----------
    filepath : str  - Path to CSV file. Defaults to ../data/raw/ifo_bci_data.csv
    """
    if filepath is None:
        filepath = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "ifo_bci_data.csv")

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"IFO CSV not found at {filepath}.\n"
            "Download from: https://www.ifo.de/en/survey/ifo-business-climate-index\n"
            "Or use fetch_ifo_from_fred() with a free FRED API key."
        )

    df = pd.read_csv(filepath, parse_dates=["Date"])
    df = df.rename(columns={
        "Date"                : "date",
        "Business Climate"    : "ifo_climate",
        "Current Situation"   : "ifo_current",
        "Expectations"        : "ifo_expectations",
    })
    df = df.set_index("date").sort_index()
    return df


def compute_ifo_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived signals to IFO data:
      - expectations_gap: expectations minus current (positive = forward optimism)
      - climate_mom: month-on-month change in climate index
      - climate_direction: 'rising' / 'falling' / 'flat' based on 3m trend

    These signals feed into the regime classifier.
    """
    df = df.copy()

    df["expectations_gap"] = df["ifo_expectations"] - df["ifo_current"]
    df["climate_mom"] = df["ifo_climate"].diff()
    df["climate_3m_change"] = df["ifo_climate"].diff(3)

    def classify_direction(row):
        if row["climate_3m_change"] > 1.5:
            return "rising"
        elif row["climate_3m_change"] < -1.5:
            return "falling"
        else:
            return "flat"

    df["climate_direction"] = df.apply(classify_direction, axis=1)
    return df


def get_ifo_regime_score(df: pd.DataFrame, as_of: str = None) -> dict:
    """
    Return IFO contribution to regime classifier (0–25 scale).

    Scoring:
      Climate level:     >102 → 10pts | 100–102 → 7pts | 98–100 → 4pts | <98 → 1pt
      Expectations gap:  >2   → 10pts | 0–2 → 5pts | <0 → 0pts
      Direction:         rising → 5pts | flat → 2pts | falling → 0pts

    Max score: 25
    """
    df = compute_ifo_signals(df)
    row = df.iloc[-1] if as_of is None else df.loc[:as_of].iloc[-1]

    # Climate level score
    climate = row["ifo_climate"]
    if climate > 102:
        level_score = 10
    elif climate >= 100:
        level_score = 7
    elif climate >= 98:
        level_score = 4
    else:
        level_score = 1

    # Expectations gap score
    gap = row["expectations_gap"]
    gap_score = 10 if gap > 2 else (5 if gap >= 0 else 0)

    # Direction score
    direction_score = {"rising": 5, "flat": 2, "falling": 0}.get(row["climate_direction"], 2)

    total = level_score + gap_score + direction_score

    return {
        "ifo_score"         : total,
        "ifo_climate"       : round(climate, 2),
        "expectations_gap"  : round(gap, 2),
        "climate_direction" : row["climate_direction"],
        "as_of"             : str(row.name.date()),
    }


# ── Quick test ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Requires FRED API key — get free at fred.stlouisfed.org
    # Replace with your key to test
    API_KEY = "YOUR_FRED_API_KEY_HERE"

    if API_KEY != "YOUR_FRED_API_KEY_HERE":
        print("Fetching IFO data from FRED...")
        df = fetch_ifo_from_fred(API_KEY, start_date="2019-01-01")
        df = compute_ifo_signals(df)
        print(df.tail(6).to_string())

        score = get_ifo_regime_score(df)
        print(f"\nIFO Regime Score: {score}")

        df.to_csv("../data/processed/ifo_data.csv")
        print("Saved → data/processed/ifo_data.csv")
    else:
        print("Set your FRED API key to test. Get it free at: https://fred.stlouisfed.org/docs/api/api_key.html")
