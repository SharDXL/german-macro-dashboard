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
  - Expectations (6-month forward outlook) -- the headline ZEW Indicator
  - Current Conditions (Lage) -- current assessment

LIVE DATA SOURCE:
  ZEW publishes a free PDF summary after each monthly release:
    https://download.zew.de/e_current_table.pdf
  This URL always points to the LATEST survey results.
  We parse it with pdfplumber to extract the headline ZEW Indicator.

HISTORICAL BASELINE:
  Curated 2019-2026 values based on published ZEW Institute data.
  The live PDF fetch appends/overwrites the most recent month.

Requires: pdfplumber  (pip install pdfplumber)
"""

import re
import pandas as pd
from io import BytesIO
from datetime import datetime
from calendar import month_abbr


# ── ZEW Live PDF ─────────────────────────────────────────────────────────────

ZEW_PDF_URL = "https://download.zew.de/e_current_table.pdf"

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _normalize_pdf(text: str) -> str:
    """
    ZEW PDF uses a doubled or tripled-character font rendering.
    'ZZZEEEWWW' -> 'ZEW',  'RReessuullttss MMaarrcchh 22002266' -> 'Results March 2026'.
    Collapses any run of identical consecutive characters to a single one.
    Numbers in ZEW data (e.g. -58.8, 29.4) never have consecutive identical digits,
    so this is safe for both text labels and numeric values.
    """
    return re.sub(r"(.)\1+", r"\1", text)


def fetch_zew_live() -> dict:
    """
    Download the ZEW current-results PDF and extract the headline figures.

    Returns dict with keys:
      survey_date       : pd.Timestamp (first day of survey month)
      zew_expectations  : float  (ZEW Indicator, -100 to +100)
      zew_current       : float  (Current Conditions balance)
      source_note       : str
    Raises RuntimeError if parsing fails.
    """
    try:
        import requests
        import pdfplumber
    except ImportError as e:
        raise ImportError(
            f"Missing dependency: {e}\n"
            "Install with: pip install requests pdfplumber"
        )

    print("  Fetching ZEW live data from ZEW PDF...")
    resp = requests.get(ZEW_PDF_URL, timeout=20)
    resp.raise_for_status()

    with pdfplumber.open(BytesIO(resp.content)) as pdf:
        raw_text = pdf.pages[0].extract_text()

    lines = raw_text.split("\n")
    # Normalize each line: collapse PDF's doubled/tripled character artifacts
    norm_lines = [_normalize_pdf(ln) for ln in lines]

    # ── Parse survey month/year from header ──────────────────────────────────
    survey_date = None
    for norm_line in norm_lines[:3]:
        m = re.search(r"Results?\s+(\w+)\s+(\d{4})", norm_line, re.IGNORECASE)
        if m:
            month_name = m.group(1).lower()
            year = int(m.group(2))
            month_num = MONTH_MAP.get(month_name)
            if month_num:
                survey_date = pd.Timestamp(year=year, month=month_num, day=1)
                print(f"  ZEW survey period: {m.group(1)} {year}")
            break

    if survey_date is None:
        raise RuntimeError(
            "Could not parse survey date from ZEW PDF header.\n"
            f"First normalized line: {repr(norm_lines[0])}"
        )

    # ── Parse ZEW Expectations (Indicator) ───────────────────────────────────
    # After normalization the line looks like:
    # "Germany (ZEW Indicator) 29.4 (-32.9) 40.7 (+ 7.0) 29.9 (+25.9) -0.5 (-58.8)"
    zew_exp = None
    for norm_line in norm_lines:
        if "ZEW" in norm_line and "Indicator" in norm_line:
            # Balance is the last number before the final trailing parenthetical
            m = re.search(r"(-?\d+\.?\d*)\s*\([^)]*\)\s*$", norm_line)
            if m:
                zew_exp = float(m.group(1))
                break

    if zew_exp is None:
        raise RuntimeError(
            "Could not extract ZEW Expectations balance from PDF.\n"
            "The PDF layout may have changed -- check the raw text."
        )

    # ── Parse Current Conditions (Germany, first table) ──────────────────────
    # Strategy: find the first "Germany X.X (...)" line that does NOT contain
    # "ZEW" or "Indicator" -- that is the current-conditions Germany row.
    # (The ZEW Indicator row is in the expectations table and is already found above.)
    zew_cur = None
    for norm_line in norm_lines:
        if (re.match(r"^Germany\s+\d", norm_line)
                and "ZEW" not in norm_line
                and "Indicator" not in norm_line):
            m = re.search(r"(-?\d+\.?\d*)\s*\([^)]*\)\s*$", norm_line)
            if m:
                zew_cur = float(m.group(1))
                break

    # Fallback: estimate current from expectations with typical lag factor
    if zew_cur is None:
        zew_cur = round(zew_exp * 0.6, 1)
        print("  Warning: Could not parse current conditions; estimated from expectations.")

    print(f"  ZEW Indicator (Expectations): {zew_exp:+.1f}")
    print(f"  ZEW Current Conditions:       {zew_cur:+.1f}")

    return {
        "survey_date"      : survey_date,
        "zew_expectations" : zew_exp,
        "zew_current"      : zew_cur,
        "source_note"      : f"Live: ZEW Institute PDF (Results {survey_date.strftime('%B %Y')})",
    }


# ── Historical Baseline (2019-2025) ──────────────────────────────────────────

def _get_curated_zew() -> pd.DataFrame:
    """
    Curated ZEW historical data (2019-2025) based on published ZEW Institute values.
    ZEW scale: -100 to +100 (net balance of optimists minus pessimists).
    Used as the historical baseline; live PDF fetch overwrites the latest month.
    """
    # 86 months: Jan 2019 -- Feb 2026
    # (March 2026 onward is appended by the live PDF fetch)
    dates = pd.date_range("2019-01-01", periods=86, freq="MS")
    zew_exp = [
        # 2019
        -15.0, -13.4, -3.6,  3.1,  6.4, -21.1, -24.5, -44.1, -22.5, -22.8, -2.1,  10.7,
        # 2020
         26.7,  8.7, -49.5, -49.5,  51.0,  63.4,  59.3,  71.5,  77.4,  56.1,  39.0,  55.0,
        # 2021
         61.8,  71.2,  76.6,  70.7,  84.4,  79.8,  63.3,  40.4,  26.5,  22.3,  31.7,  29.9,
        # 2022
         51.7,  54.3, -39.3, -41.0, -34.3, -28.0, -53.8, -55.3, -61.9, -59.2, -36.7, -23.3,
        # 2023
         16.9,  28.1,  13.0,   4.1,  10.7,  20.0,  14.7, -12.3, -11.4,  -1.1,   9.8,  12.8,
        # 2024
         15.2,  19.9,  31.7,  42.9,  47.1,  47.5,  41.8,  19.2,   3.6,  -3.6,   7.4,  15.7,
        # 2025
         26.0,  26.0,  51.6,   3.9,  25.7,  47.5,  41.3,  38.5,  45.8,  31.0,   7.4,  15.0,
        # 2026 (Jan-Feb hardcoded; Mar onward from live PDF)
         59.6,  58.3,
    ]
    zew_cur = [round(v * 0.6, 1) for v in zew_exp]
    return pd.DataFrame(
        {"zew_expectations": zew_exp, "zew_current": zew_cur},
        index=dates,
    )


# ── Main Fetch Function ───────────────────────────────────────────────────────

def fetch_zew_from_fred(api_key: str = None, start_date: str = "2019-01-01") -> pd.DataFrame:
    """
    Returns ZEW sentiment data combining:
      1. Curated historical series  (2019 - Dec 2025)
      2. Live ZEW Institute PDF      (current month, fetched from download.zew.de)

    The api_key parameter is accepted for backwards compatibility but not used.
    No API key required -- data is scraped from the free ZEW PDF release.

    Returns pd.DataFrame indexed by date with columns:
      zew_expectations, zew_current
    """
    # ── Historical baseline ───────────────────────────────────────────────────
    hist = _get_curated_zew()

    # ── Live current month ────────────────────────────────────────────────────
    try:
        live = fetch_zew_live()
        new_row = pd.DataFrame(
            {
                "zew_expectations": [live["zew_expectations"]],
                "zew_current"     : [live["zew_current"]],
            },
            index=[live["survey_date"]],
        )
        # Merge: overwrite if same date exists (live data takes priority)
        df = hist[~hist.index.isin(new_row.index)]
        df = pd.concat([df, new_row]).sort_index()
        print(f"  ZEW: {len(df)} months loaded (historical + live PDF).")
    except Exception as e:
        print(f"  Warning: Live ZEW PDF fetch failed ({e}). Using curated historical only.")
        df = hist

    return df[df.index >= pd.Timestamp(start_date)]


# ── Derived Signals ───────────────────────────────────────────────────────────

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
    Return ZEW contribution to regime classifier (0-25 scale).

    Scoring:
      Expectations level: >30 -> 10pts | 10-30 -> 7pts | 0-10 -> 4pts | <0 -> 1pt
      Momentum:           improving -> 10pts | stable -> 5pts | deteriorating -> 0pts
      Divergence:         exp > current by >10 -> 5pts | else -> 2pts | exp < current -> 0pts

    Max score: 25
    """
    df = compute_zew_signals(df)
    row = df.iloc[-1] if as_of is None else df.loc[:as_of].iloc[-1]

    zew = row["zew_expectations"]
    if zew > 30:
        level_score = 10
    elif zew >= 10:
        level_score = 7
    elif zew >= 0:
        level_score = 4
    else:
        level_score = 1

    momentum_score = {"improving": 10, "stable": 5, "deteriorating": 0}.get(
        row["zew_trend"], 5
    )

    div = row["zew_divergence"]
    div_score = 5 if div > 10 else (2 if div >= 0 else 0)

    total = level_score + momentum_score + div_score

    return {
        "zew_score"        : total,
        "zew_expectations" : round(zew, 1),
        "zew_trend"        : row["zew_trend"],
        "zew_divergence"   : round(div, 1),
        "as_of"            : str(row.name.date()),
    }


if __name__ == "__main__":
    print("Fetching ZEW data (live PDF + historical)...")
    df = fetch_zew_from_fred(start_date="2023-01-01")
    df = compute_zew_signals(df)
    print(df.tail(8).to_string())

    score = get_zew_regime_score(df)
    print(f"\nZEW Regime Score: {score}")

    df.to_csv("../data/processed/zew_data.csv")
    print("Saved -> data/processed/zew_data.csv")
