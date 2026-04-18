"""
ecb_macro.py
------------
Fetches macro data from the ECB Statistical Data Warehouse (SDW).
Free public REST API — no key required.

Series fetched:
  - ECB deposit facility rate  : FM.B.U2.EUR.4F.KR.DFR.LEV
  - 2Y Bund yield              : YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y
  - 5Y Bund yield              : YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y
  - 10Y Bund yield             : YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y
  - Euro Area HICP (headline)  : ICP.M.U2.N.000000.4.ANR
  - German HICP                : ICP.M.DE.N.000000.4.ANR

Docs: https://data-api.ecb.europa.eu/
"""

import requests
import pandas as pd
from io import StringIO
from datetime import date, timedelta


ECB_BASE = "https://data-api.ecb.europa.eu/service/data"

SERIES = {
    # Rates
    "ecb_deposit_rate" : "FM/B.U2.EUR.4F.KR.DFR.LEV",
    "bund_2y"          : "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    "bund_5y"          : "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y",
    "bund_10y"         : "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
    # Inflation
    "hicp_ea"          : "ICP/M.U2.N.000000.4.ANR",   # Euro Area HICP YoY
    "hicp_de"          : "ICP/M.DE.N.000000.4.ANR",   # German HICP YoY
}


def fetch_series(series_key: str, start_date: str = "2019-01-01",
                 end_date: str = None) -> pd.DataFrame:
    """
    Fetch a time series from ECB SDW REST API.

    Returns pd.DataFrame with columns ['date', 'value'].
    """
    if end_date is None:
        end_date = date.today().isoformat()

    url = f"{ECB_BASE}/{series_key}"
    params = {"startPeriod": start_date, "endPeriod": end_date, "format": "csvdata"}
    headers = {"Accept": "text/csv"}

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))

    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        raise ValueError(f"Unexpected columns in ECB response: {df.columns.tolist()}")

    result = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
    result.columns = ["date", "value"]
    result["date"] = pd.to_datetime(result["date"])
    result["value"] = pd.to_numeric(result["value"], errors="coerce")
    return result.dropna().sort_values("date").reset_index(drop=True)


def fetch_all_macro(start_date: str = "2019-01-01") -> pd.DataFrame:
    """
    Fetch all ECB macro series and return wide-format DataFrame indexed by date.
    Note: Rate data is daily, CPI is monthly — joining produces NaN on rate rows.
    Use .resample('MS').last() to get monthly snapshots.
    """
    dfs = []
    for name, key in SERIES.items():
        try:
            df = fetch_series(key, start_date=start_date)
            df = df.rename(columns={"value": name}).set_index("date")
            dfs.append(df)
            print(f"  ✓ {name}: {len(df)} obs")
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    if not dfs:
        raise RuntimeError("No ECB data fetched.")

    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.join(df, how="outer")

    return merged.sort_index()


def get_monthly_macro(start_date: str = "2019-01-01") -> pd.DataFrame:
    """
    Return monthly macro snapshot (end-of-month values).
    Good for merging with IFO and ZEW monthly data.
    """
    df = fetch_all_macro(start_date)
    monthly = df.resample("MS").last()
    return monthly


def get_yield_curve_shape(df: pd.DataFrame = None) -> dict:
    """
    Return current yield curve characteristics.
    2s10s spread < 0 = inverted (historically signals slowdown).
    """
    if df is None:
        df = fetch_all_macro(start_date=(date.today() - timedelta(days=30)).isoformat())

    latest = df.dropna(how="all").iloc[-1]
    result = {
        "bund_2y"   : round(latest.get("bund_2y", float("nan")), 3),
        "bund_5y"   : round(latest.get("bund_5y", float("nan")), 3),
        "bund_10y"  : round(latest.get("bund_10y", float("nan")), 3),
        "spread_2s10s" : round(latest.get("bund_10y", 0) - latest.get("bund_2y", 0), 3),
        "ecb_rate"  : round(latest.get("ecb_deposit_rate", float("nan")), 3),
    }

    # Curve regime
    if result["spread_2s10s"] < -0.25:
        result["curve_regime"] = "Inverted (caution)"
    elif result["spread_2s10s"] < 0.25:
        result["curve_regime"] = "Flat"
    else:
        result["curve_regime"] = "Normal (steepening)"

    return result


if __name__ == "__main__":
    print("Fetching ECB macro data...")
    df = get_monthly_macro(start_date="2022-01-01")
    print(f"\nShape: {df.shape}")
    print("\nLatest 6 months:")
    print(df.tail(6).to_string())

    curve = get_yield_curve_shape(df)
    print(f"\nYield Curve: {curve}")

    df.to_csv("../data/processed/ecb_macro.csv")
    print("\nSaved → data/processed/ecb_macro.csv")
