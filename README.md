# German Macro Regime Dashboard

**Author:** Shardul Pundir | MSc Finance @ WHU Otto Beisheim (Sep 2026)  
**Stack:** Python · pandas · requests · Plotly · Jupyter  
**Data:** IFO Institute · ZEW · ECB SDW API · Destatis · yfinance

---

## What This Is

A Python-based macro intelligence dashboard that tracks the German economic cycle in real time and maps it to equity and bond market behavior. Built to understand — not just observe — how Germany's economy moves.

The dashboard integrates five key data streams:
- **IFO Business Climate Index** — Germany's premier leading indicator (current conditions + expectations)
- **ZEW Economic Sentiment** — institutional investor expectations for the next 6 months
- **Inflation (CPI)** — German and Euro Area headline/core, sourced from Destatis and ECB
- **PMI Manufacturing & Services** — real-time activity signals
- **ECB Yield Curve** — 2Y/5Y/10Y Bund yields + ECB deposit rate

These are mapped to a **four-phase economic regime classifier**:
```
Expansion   → IFO high, ZEW positive, PMI > 50
Slowdown    → IFO declining, ZEW turning negative, PMI softening
Contraction → IFO low, ZEW deeply negative, PMI < 50
Recovery    → IFO bottoming, ZEW recovering, PMI turning up
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
# Composite Score (0–100)
# Each indicator scored 0–25, summed

ifo_score   = f(ifo_climate_index, ifo_expectations)
zew_score   = f(zew_sentiment_level, momentum)
pmi_score   = f(pmi_composite, direction)
yield_score = f(bund_10y_level, curve_shape)

composite = ifo_score + zew_score + pmi_score + yield_score

# Regime
if composite >= 70:   regime = "Expansion"
elif composite >= 50: regime = "Slowdown"
elif composite >= 30: regime = "Contraction"
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

| Data | Source | Access Method |
|------|--------|---------------|
| IFO Index | [ifo.de](https://www.ifo.de/en/survey/ifo-business-climate-index) | CSV download |
| ZEW Sentiment | [zew.de](https://www.zew.de/en/publications/zew-indicator-of-economic-sentiment/) | CSV download |
| ECB rates + CPI | [ECB SDW API](https://sdw-wsrest.ecb.europa.eu) | REST API (free, no key) |
| PMI Germany | [FRED](https://fred.stlouisfed.org) | FRED API (free key) |
| DAX sectors | [yfinance](https://pypi.org/project/yfinance/) | Python library |

---

## CV Line

> Built a German macro regime dashboard in Python integrating IFO, ZEW, PMI, ECB yield curve, and CPI data into a four-phase economic cycle classifier with DAX sector overlay. Used to track Germany's economic environment weekly in preparation for WHU MSc Finance.

---

## Timeline

| Milestone | Target |
|-----------|--------|
| Data modules complete | Week 1 |
| Regime classifier built | Week 2 |
| Jupyter dashboard live | Week 3 |
| First monthly macro commentary | Week 4 |
| GitHub launch | May 2026 |
