"""
run_regime.py
--------------
Standalone entry point that refreshes ECB, ZEW, and DAX sector data live,
recomputes the composite regime score, and writes the result to
outputs/weekly_commentary.txt. Run manually or on a schedule (see
.github/workflows/refresh.yml).

IFO Business Climate is the one component NOT refreshed automatically here --
it requires a free FRED API key (https://fred.stlouisfed.org/docs/api/api_key.html)
or a manual CSV download from ifo.de (see src/ifo_data.py docstring). Until a
FRED_API_KEY secret is wired in, the last confirmed IFO reading is carried
forward and clearly labelled as such in the output below.
"""
import sys, os, warnings, json
from datetime import date

sys.path.insert(0, "src")
warnings.filterwarnings("ignore")

import ecb_macro
import zew_data
import dax_data
import regime_classifier

LAST_KNOWN_IFO = {
    "ifo_score": 11,
    "ifo_climate": 98.95,
    "expectations_gap": 1.5,
    "climate_direction": "flat (last confirmed reading)",
    "as_of": "2026-04-18 (STALE -- needs FRED API key or manual CSV refresh, see src/ifo_data.py)",
}


def main():
    print("Fetching ECB/Bund data...")
    ecb_df = ecb_macro.fetch_all_macro()
    ecb_score = regime_classifier.get_ecb_regime_score(ecb_df)

    print("Fetching ZEW (live PDF)...")
    zew_df = zew_data.fetch_zew_from_fred()
    zew_score = zew_data.get_zew_regime_score(zew_df)

    ifo_score = LAST_KNOWN_IFO
    if os.environ.get("FRED_API_KEY"):
        try:
            import ifo_data
            ifo_df = ifo_data.fetch_ifo_from_fred(os.environ["FRED_API_KEY"])
            ifo_score = ifo_data.get_ifo_regime_score(ifo_df)
        except Exception as e:
            print(f"FRED IFO fetch failed ({e}), falling back to last known reading.")

    print("Fetching DAX sector performance...")
    prices = dax_data.fetch_returns(start_date="2024-01-01", tickers=dax_data.SECTOR_TICKERS)
    sector_signal = dax_data.get_sector_regime_alignment(prices)
    equity_score = regime_classifier.get_equity_regime_score(sector_signal)

    result = regime_classifier.classify_regime(ifo_score, zew_score, ecb_score, equity_score)
    regime_classifier.print_regime_summary(result)

    ecb_rate = ecb_df["ecb_deposit_rate"].dropna()
    ecb_rate_latest = round(ecb_rate.iloc[-1], 2) if len(ecb_rate) else None
    spread = ecb_score.get("curve_spread")

    lines = [
        "GERMAN MACRO REGIME DASHBOARD",
        f"Generated: {date.today():%d %B %Y}",
        "=" * 30,
        "",
        f"REGIME: {result['regime'].upper()}",
        f"Composite Score: {result['composite_score']}/100",
        f"Confidence: {result['regime_confidence']}",
        "",
        "SUMMARY:",
        result["regime_description"],
        "",
        "COMPONENT SCORES:",
        f"  IFO (Business Climate): {ifo_score['ifo_score']}/25  [{'LIVE' if not os.environ.get('FRED_API_KEY') is None and ifo_score is not LAST_KNOWN_IFO else 'STALE - see note above'}]",
        f"  ZEW (Financial Sentiment): {zew_score['zew_score']}/25  [LIVE - {zew_score.get('as_of')}]",
        f"  ECB (Rates/Inflation): {ecb_score['ecb_score']}/25  [LIVE]",
        f"  Equity (Sector Rotation): {equity_score['equity_score']}/25  [LIVE]",
        "",
        "KEY DATA POINTS:",
        f"  IFO Climate Index:      {ifo_score.get('ifo_climate')}",
        f"  ZEW Expectations:       {zew_score.get('zew_expectations')}",
        f"  ECB Deposit Rate:       {ecb_rate_latest}%",
        f"  Bund 2s10s spread:      {spread}",
        f"  Cyclical vs Defensive (3m): {sector_signal.get('cyclical_vs_defensive_spread')}pp -- {sector_signal.get('market_regime_signal')}",
    ]

    os.makedirs("outputs", exist_ok=True)
    with open("outputs/weekly_commentary.txt", "w") as f:
        f.write("\n".join(lines) + "\n")

    with open("outputs/latest_regime.json", "w") as f:
        json.dump(result, f, indent=2, default=str)

    print("\nSaved outputs/weekly_commentary.txt and outputs/latest_regime.json")


if __name__ == "__main__":
    main()
