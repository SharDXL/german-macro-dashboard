# German Macro Regime Dashboard

**Author:** Shardul Pundir | Economics Graduate · CFA Level I  
**Stack:** Python · pandas · requests · Plotly · Jupyter  
**Data:** ZEW (live PDF) · ECB SDW API · yfinance · IFO (manual/FRED)

**Data last refreshed:** 06 July 2026 — ZEW, ECB, and equity-sector legs update automatically every Monday via GitHub Actions (`.github/workflows/refresh.yml`); IFO carries forward the last confirmed reading until a free FRED API key is added (see Data Sources below).

---

## What This Is

A Python-based macro intelligence dashboard that tracks the German economic cycle in real time and maps it to equity and bond market behavior. Built to understand — not just observe — how Germany's economy moves.

The dashboard integrates four data streams into the composite score:
- **IFO Business Climate Index** — Germany's premier leading indicator (current conditions + expectations)
- **ZEW Economic Sentiment** — ~350 financial experts' 6-month outlook, scraped live from ZEW's own published PDF each month
- **ECB rates & yield curve** — deposit rate, 2Y/5Y/10Y Bund yields, and Euro Area/German HICP inflation, via the ECB's free public SDW API
- **DAX sector rotation** — cyclical (Autos, Banks, Industrials, Materials) vs. defensive (Utilities, Healthcare) 3-month performance spread, via yfinance

These are mapped to a **four-phase economic regime classifier**:
```
Expansion   → IFO high, ZEW positive, ECB curve normal/rates falling, cyclicals leading
Slowdown    → IFO declining, ZEW turning negative, ECB near peak rates
Contraction → IFO low, ZEW deeply negative, curve flat/inverted, defensives leading
Recovery    → IFO bottoming, ZEW recovering, ECB cutting, cyclicals re-emerging
```

Overlaid with DAX sector performance to show which sectors outperform in each regime.

---

## Project Structure

```
german-macro-dashboard/
├── src/
│   ├── ifo_data.py          # IFO Business Climate (CSV download from ifo.de)
│   ├── zew_data.py          # ZEW Economic Sentiment (CSV from zew.de)
│   ├── ecb_macro.py         # ECB SDW API — CPI, yield curve, deposit rate
│   ├── pmi_data.py          # PMI data (S&P Global / FRED)
│   ├── dax_data.py          # DAX sector ETF performance via yfinance
│   └── regime_classifier.py # Regime logic + composite score
├── notebooks/
│   └── macro_dashboard.ipynb  # Main interactive dashboard
├── data/
│   ├── raw/                 # Downloaded CSVs (gitignored)
│   └── processed/           # Cleaned DataFrames
├── outputs/                 # Saved charts / monthly commentary
├── requirements.txt
└── README.md
```

---

## Key Economic Indicators — Reference Guide

| Indicator | Source | Release | What to Watch |
|-----------|--------|---------|---------------|
| IFO Business Climate | ifo.de | Monthly (last Mon/Tue) | Gap between Expectations & Current: widening = turning point |
| ZEW Economic Sentiment | zew.de | Monthly (2nd Tue) | Below 0 = negative expectations; sharp moves = inflection signal |
| German CPI | Destatis / ECB | Monthly | Core vs. headline divergence; ECB reaction function |
| PMI Manufacturing | S&P Global | Monthly (1st Tue) | 50 = breakeven; sub-48 = contraction; >52 = expansion |
| PMI Services | S&P Global | Monthly (1st Wed) | Services-led economy post-COVID; watch divergence from Mfg |
| 10Y Bund Yield | ECB SDW | Daily | Real yields, ECB policy expectations, credit spread benchmark |
| ECB Deposit Rate | ECB SDW | Per meeting | Key rate driving EUR fixed income markets |

---

## Regime Classification Logic

```python
# Composite Score (0-100), each of the four components scored 0-25

ifo_score    = f(climate_level, expectations_gap, 3m_direction)
zew_score    = f(expectations_level, 3m_momentum, expectations_vs_current_divergence)
ecb_score    = f(2s10s_curve_shape, 6m_rate_direction, HICP_inflation)
equity_score = f(cyclical_vs_defensive_3m_return_spread)

composite = ifo_score + zew_score + ecb_score + equity_score

# Regime (actual implemented thresholds, src/regime_classifier.py)
if composite >= 75:   regime = "Expansion"
elif composite >= 50: regime = "Slowdown"
elif composite >= 25: regime = "Contraction"
else:                 regime = "Recovery"
```

---

## DAX Sector Regime Map

| Regime | Outperforming Sectors | Underperforming |
|--------|-----------------------|-----------------|
| Expansion | Industrials, Autos, Financials | Utilities, Healthcare |
| Slowdown | Healthcare, Consumer Staples | Industrials, Tech |
| Contraction | Utilities, Gold, Bonds | Autos, Banks |
| Recovery | Autos, Industrials, Materials | Utilities |

---

## Data Sources (All Free)

| Data | Source | Access Method | Auto-refreshed? |
|------|--------|---------------|------------------|
| IFO Index | [ifo.de](https://www.ifo.de/en/survey/ifo-business-climate-index) | Manual CSV, or free FRED API key | Not yet — add `FRED_API_KEY` as a repo secret to enable |
| ZEW Sentiment | [zew.de](https://download.zew.de/e_current_table.pdf) | Live PDF scrape (no key needed) | Yes, weekly |
| ECB rates + inflation | [ECB SDW API](https://data-api.ecb.europa.eu) | REST API (free, no key) | Yes, weekly |
| DAX sectors | [yfinance](https://pypi.org/project/yfinance/) | Python library | Yes, weekly |

---

## CV Line

> Built a German macro regime dashboard in Python integrating IFO, ZEW, ECB yield curve, and DAX sector data into a four-phase economic cycle classifier, with three of four data feeds refreshing automatically via a scheduled GitHub Action.

---

## Key Findings (as of latest refresh, 06 July 2026)

- **Current read: Slowdown, 69/100, moderate confidence** — up from a borderline Slowdown/Contraction read (53-58/100) in April.
- **The most interesting signal is a divergence, not the headline number:** ZEW expectations jumped from -0.5 to +10.5 in two months, and cyclical equities are outperforming defensives by 9.9 percentage points over 3 months — both point toward improving sentiment. But ZEW's *current conditions* reading is still -81.0, deeply depressed. Real-money positioning (equities) and forward survey sentiment (ZEW expectations) are both turning up well ahead of the on-the-ground data — a classic early-cycle pattern worth watching rather than trusting at face value.
- **Caveat:** the IFO leg of this score is stale (last confirmed April reading) pending a FRED API key — see the Data Sources table. The other three legs are live as of each weekly run.

---

## Automated Refresh

`run_regime.py` re-pulls ZEW, ECB, and DAX sector data, recomputes the composite score, and writes `outputs/weekly_commentary.txt` + `outputs/latest_regime.json`. A GitHub Action (`.github/workflows/refresh.yml`) runs this every Monday and commits any changes automatically, so the numbers in this repo don't silently go stale between visits.
