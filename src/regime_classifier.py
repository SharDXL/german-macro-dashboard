"""
regime_classifier.py
--------------------
Combines IFO, ZEW, ECB macro, and equity market signals into
a single four-phase German economic regime classification.

Regime Framework (based on classic economic cycle):

  EXPANSION   → High activity, rising confidence, ECB tightening or neutral
  SLOWDOWN    → Confidence falling from high levels, PMI softening
  CONTRACTION → Confidence low, PMI sub-50, yield curve flat/inverted
  RECOVERY    → Confidence bottoming, ZEW inflecting, early cyclicals bid

Composite Score (0–100):
  IFO score    (0–25): level, expectations gap, direction
  ZEW score    (0–25): level, momentum, divergence
  ECB score    (0–25): yield curve shape, rate cycle position
  Equity score (0–25): cyclical vs defensive performance

Regime thresholds:
  75–100 → Expansion
  50–74  → Slowdown
  25–49  → Contraction
  0–24   → Recovery
"""

import pandas as pd
import numpy as np
from datetime import date


# ── ECB score helper (no external dependency) ──────────────────────────────

def get_ecb_regime_score(ecb_df: pd.DataFrame) -> dict:
    """
    Score ECB/rates data for regime classifier (0–25 scale).

    Uses:
      - 2s10s spread (curve shape)
      - ECB deposit rate level and recent direction

    Scoring:
      Curve shape:     normal steep >0.5 → 10pts | flat 0–0.5 → 6pts | inverted → 2pts
      Rate direction:  cutting cycle → 8pts | on hold → 5pts | hiking → 2pts
      Inflation:       HICP < 2.5% → 7pts | 2.5–4% → 4pts | >4% → 1pt

    Max: 25
    """
    if ecb_df is None or ecb_df.empty:
        return {"ecb_score": 12, "note": "No ECB data — using neutral score"}

    latest = ecb_df.dropna(how="all").iloc[-1]

    # 2s10s spread
    spread = latest.get("bund_10y", np.nan) - latest.get("bund_2y", np.nan)
    if pd.isna(spread):
        curve_score = 5
    elif spread > 0.5:
        curve_score = 10
    elif spread >= 0:
        curve_score = 6
    else:
        curve_score = 2  # inverted

    # Rate direction (compare to 6 months ago)
    rate_series = ecb_df["ecb_deposit_rate"].dropna()
    if len(rate_series) > 120:
        ecb_rate_now = rate_series.iloc[-1]
        ecb_rate_6m  = rate_series.iloc[-120]
        if pd.isna(ecb_rate_now) or pd.isna(ecb_rate_6m):
            rate_score = 5
        elif ecb_rate_now < ecb_rate_6m - 0.1:
            rate_score = 8   # cutting → supportive
        elif ecb_rate_now > ecb_rate_6m + 0.1:
            rate_score = 2   # hiking → restrictive
        else:
            rate_score = 5   # on hold
    elif len(rate_series) > 1:
        ecb_rate_now = rate_series.iloc[-1]
        ecb_rate_6m  = rate_series.iloc[0]
        if ecb_rate_now < ecb_rate_6m - 0.1:
            rate_score = 8
        elif ecb_rate_now > ecb_rate_6m + 0.1:
            rate_score = 2
        else:
            rate_score = 5
    else:
        rate_score = 5

    # Inflation (HICP)
    hicp = latest.get("hicp_ea", latest.get("hicp_de", np.nan))
    if pd.isna(hicp):
        inflation_score = 4
    elif hicp < 2.5:
        inflation_score = 7
    elif hicp <= 4.0:
        inflation_score = 4
    else:
        inflation_score = 1

    total = curve_score + rate_score + inflation_score

    return {
        "ecb_score"    : total,
        "curve_spread" : round(spread, 3) if not pd.isna(spread) else None,
        "ecb_rate"     : round(latest.get("ecb_deposit_rate", np.nan), 2),
        "hicp_ea"      : round(hicp, 1) if not pd.isna(hicp) else None,
    }


def get_equity_regime_score(sector_signal: dict) -> dict:
    """
    Score equity market signal for regime classifier (0–25 scale).

    Uses cyclical vs. defensive spread from dax_data.get_sector_regime_alignment().

    Scoring:
      Cyclical spread > 5  → 25 (market pricing expansion)
      Cyclical spread 1–5  → 18
      Cyclical spread -3–1 → 12
      Cyclical spread < -3 → 5  (defensive, risk-off)
    """
    if not sector_signal or "cyclical_vs_defensive_spread" not in sector_signal:
        return {"equity_score": 12, "note": "No equity data — using neutral score"}

    spread = sector_signal["cyclical_vs_defensive_spread"]
    if spread > 5:
        score = 25
    elif spread >= 1:
        score = 18
    elif spread >= -3:
        score = 12
    else:
        score = 5

    return {
        "equity_score"  : score,
        "cyclical_spread": spread,
        "market_signal" : sector_signal.get("market_regime_signal", ""),
    }


# ── Main classifier ─────────────────────────────────────────────────────────

def classify_regime(
    ifo_score_dict: dict,
    zew_score_dict: dict,
    ecb_score_dict: dict,
    equity_score_dict: dict,
) -> dict:
    """
    Combine four component scores into a single regime classification.

    Parameters (all dicts from respective module score functions):
      ifo_score_dict    : from ifo_data.get_ifo_regime_score()
      zew_score_dict    : from zew_data.get_zew_regime_score()
      ecb_score_dict    : from regime_classifier.get_ecb_regime_score()
      equity_score_dict : from regime_classifier.get_equity_regime_score()

    Returns dict with:
      composite_score, regime, regime_confidence, component_scores,
      regime_description, sector_playbook
    """
    ifo_s    = ifo_score_dict.get("ifo_score", 12)
    zew_s    = zew_score_dict.get("zew_score", 12)
    ecb_s    = ecb_score_dict.get("ecb_score", 12)
    equity_s = equity_score_dict.get("equity_score", 12)

    composite = ifo_s + zew_s + ecb_s + equity_s

    # Regime thresholds
    if composite >= 75:
        regime = "Expansion"
        color  = "green"
        description = (
            "German economy operating above trend. Business confidence high, "
            "PMI expansionary, equity cyclicals leading. Focus: growth assets, "
            "cyclicals, credit. Watch for overheating signals."
        )
        sector_playbook = {
            "overweight" : ["Autos", "Banks", "Industrials", "Materials"],
            "underweight": ["Utilities", "Healthcare"],
            "bond_view"  : "Bearish duration — expect yields to rise or stay high",
        }
    elif composite >= 50:
        regime = "Slowdown"
        color  = "yellow"
        description = (
            "Growth decelerating. IFO declining, ZEW softening. PMI approaching 50. "
            "Defensive rotation beginning. ECB may be at or near peak rates."
        )
        sector_playbook = {
            "overweight" : ["Healthcare", "Consumer Staples", "Technology (quality)"],
            "underweight": ["Autos", "Banks", "Materials"],
            "bond_view"  : "Duration neutral — beginning to add as rates peak",
        }
    elif composite >= 25:
        regime = "Contraction"
        color  = "red"
        description = (
            "German economy contracting. IFO below 98, ZEW deeply negative, "
            "PMI sub-50. Risk-off. ECB likely cutting or about to cut. "
            "Defensives and bonds outperforming."
        )
        sector_playbook = {
            "overweight" : ["Utilities", "Healthcare", "Long-duration bonds"],
            "underweight": ["Autos", "Banks", "Industrials"],
            "bond_view"  : "Bullish duration — add Bunds as ECB cuts",
        }
    else:
        regime = "Recovery"
        color  = "blue"
        description = (
            "Economy bottoming. ZEW improving from deeply negative, IFO stabilising. "
            "ECB in cutting cycle. Early cyclicals starting to outperform. "
            "Best entry point for risk assets."
        )
        sector_playbook = {
            "overweight" : ["Autos", "Industrials", "Materials", "Small caps"],
            "underweight": ["Utilities"],
            "bond_view"  : "Neutral — peak exposure reached, shift to equity",
        }

    # Confidence: how far from nearest threshold
    thresholds = [75, 50, 25]
    distances  = [abs(composite - t) for t in thresholds]
    min_dist   = min(distances)
    confidence = "High" if min_dist > 8 else ("Moderate" if min_dist > 3 else "Low (near threshold)")

    return {
        "composite_score"    : composite,
        "regime"             : regime,
        "regime_color"       : color,
        "regime_confidence"  : confidence,
        "regime_description" : description,
        "sector_playbook"    : sector_playbook,
        "component_scores"   : {
            "ifo"    : ifo_s,
            "zew"    : zew_s,
            "ecb"    : ecb_s,
            "equity" : equity_s,
        },
        "as_of": str(date.today()),
    }


def print_regime_summary(result: dict) -> None:
    """Pretty-print the regime classification."""
    bar = "=" * 60
    print(f"\n{bar}")
    print(f"  GERMAN MACRO REGIME DASHBOARD")
    print(f"  As of: {result['as_of']}")
    print(bar)
    print(f"\n  REGIME:     {result['regime'].upper()}  [{result['composite_score']}/100]")
    print(f"  Confidence: {result['regime_confidence']}")
    print(f"\n  {result['regime_description']}")
    print(f"\n  Component Scores:")
    for name, score in result["component_scores"].items():
        bar_len = int(score / 25 * 20)
        print(f"    {name.upper():8s}: {score:2d}/25  {'█' * bar_len}{'░' * (20 - bar_len)}")
    print(f"\n  Sector Playbook:")
    pb = result["sector_playbook"]
    print(f"    Overweight : {', '.join(pb['overweight'])}")
    print(f"    Underweight: {', '.join(pb['underweight'])}")
    print(f"    Bonds      : {pb['bond_view']}")
    print(f"\n{bar}\n")


# ── Example (runs without live data for testing) ────────────────────────────
if __name__ == "__main__":
    # Simulate current Germany macro (Apr 2026 approximation)
    # IFO ~98, ZEW ~10 (recovering but cautious), ECB cutting, equity neutral
    mock_ifo    = {"ifo_score": 13, "ifo_climate": 98.1, "expectations_gap": 0.5, "climate_direction": "flat"}
    mock_zew    = {"zew_score": 14, "zew_expectations": 11.3, "zew_trend": "improving", "zew_divergence": 8.2}
    mock_ecb    = {"ecb_score": 16, "curve_spread": 0.4, "ecb_rate": 2.5, "hicp_ea": 2.3}
    mock_equity = {"equity_score": 13, "cyclical_spread": 0.5, "market_signal": "Neutral"}

    result = classify_regime(mock_ifo, mock_zew, mock_ecb, mock_equity)
    print_regime_summary(result)
