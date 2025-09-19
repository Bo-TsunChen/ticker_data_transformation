# Ticker Data Transformation

Two PySpark commands convert the sample ticker feed into daily observations and mid-quarter forecasts. The pipelines match the exploratory notebook logic so they can be run repeatably from the terminal.

## 1. Environment Setup
```bash
# create / activate a virtual environment
python -m venv .venv311
source .venv311/bin/activate        # adjust for your shell/OS

# install Python dependencies
pip install pyspark pandas plotly    # pandas/plotly only needed for plotting

# install Java (PySpark needs a JRE/JDK)
# macOS (Homebrew)
brew install openjdk@11
export JAVA_HOME="$(/usr/libexec/java_home -v11)"

# macOS (manual download)
# download OpenJDK pkg from https://adoptium.net, install it, then:
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home"

# Linux (apt example)
sudo apt-get install openjdk-11-jdk
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Windows (PowerShell example)
# install OpenJDK 11 from https://adoptium.net and set JAVA_HOME
env JDK_PATH="C:\Program Files\Eclipse Adoptium\jdk-11" ; \
  setx JAVA_HOME $Env:JDK_PATH ; \
  setx PATH "$Env:JDK_PATH\bin;$Env:PATH"
```

## 2. Inputs & Outputs
- Source feed: `data/index.csv`
- Daily output: `output/dailyindex.csv`
- Quarterly midpoint output: `output/quarterlyindex.csv`

## 3. Daily Conversion Pipeline
Fans each ticker/index to day-level rows while preserving period totals.
```bash
python src/pipeline_daily.py \
  --input data/index.csv \
  --output output/dailyindex.csv
```
Console progress: `[daily] reading source CSV...` → `[daily] expanding...` → `[daily] writing output...` → (optional) `[daily] rendering cumulative plot...` → `[daily] completed.`

## 4. Quarterly Forecast Pipeline
Consumes the day-level feed and blends run-rate/seasonal forecasts. Mid-quarter is the point two calendar months into a quarter.
```bash
python src/pipeline_quarterly.py \
  --input output/dailyindex.csv \
  --output output/quarterlyindex.csv
```
Console progress: `[quarterly] loading day-level feed...` → `[quarterly] computing daily forecasts...` → `[quarterly] selecting mid-quarter snapshots...` → `[quarterly] writing midpoint output...` → (optional) `[quarterly] writing full timeline...` → `[quarterly] completed.`

> Run the daily pipeline first so `output/dailyindex.csv` exists (or supply a day-level feed with the same schema).

## 5. Assumptions & Data Requirements
### Data Quality
- Feed covers every calendar day used; any gaps were handled upstream.
- `VALUE` and `CUMULATIVEVALUE` reconcile with official quarter totals; no duplicates.
- Column definitions remain stable year over year; data types do not change midstream.
- Updated data arrives daily (or at the frequency required for forecasting).

### Forecasting
- Year one falls back to simple extrapolation (no seasonal history yet); later years have enough data for seasonal proportions.
- Seasonal patterns are assumed to be stable across matching quarters (e.g., Q2 year over year).
- Simple extrapolation assumes the quarter-to-date daily rate persists.
- Hybrid forecast blends run-rate/seasonal estimates; default weight is `0.7` / `0.3` but configurable.
- Pipelines emit two CSVs plus this README so the process is reproducible.
- Forecast error is `(forecast - actual) / actual`, rounded to four decimals.

## 6. Future Enhancements
- Add schema/data-quality validation before running the pipelines.
- Provide backtesting metrics (MAPE, RMSE) by ticker/index.
- Introduce a unified CLI with `daily`/`quarterly` subcommands and dry-run support.
- Add automated tests for `src/utils.py` helpers.

## 7. Challenges
- Mixed cadence in index.csv forced you to detect each ticker/index’s lowest duration and expand it to daily rows without breaking totals.
- Mid-quarter forecasts hinge on seasonality; limited history means the hybrid model must fall back to simple run-rate when seasonal curves aren’t reliable.
- The pipelines assume a clean feed (no missing VALUE/CUMULATIVEVALUE, stable schema) because there’s no validation layer yet.
- PySpark requires a local JRE/JDK; documenting the cross-platform setup was necessary so others can run the code.

## 8. Limitations
- Unexpected shocks (policy changes, market events) are not modelled.
- Accuracy hinges on seasonal stability year over year.
- Mid-quarter estimates carry uncertainty; downstream consumers should treat them as estimates (add error bands if needed).

## 9. Troubleshooting
| Issue | Likely Fix |
| --- | --- |
| `ModuleNotFoundError: pyspark` | Install requirements in the active virtual env (`pip install pyspark`). |
| Excess Spark WARN logs | Log level defaults to `ERROR`; ensure no custom config overrides it. |
| Missing outputs | Writers create directories automatically; check permissions if files are absent. |

## 10. Repository Layout
```
.
├── data/
│   └── index.csv
├── output/
│   ├── dailyindex.csv
│   └── quarterlyindex.csv
├── requirements.txt      # optional Python dependencies (pyspark, pandas, plotly)
└── src/
    ├── pipeline_daily.py
    ├── pipeline_quarterly.py
    └── utils.py
```

Happy transforming!
