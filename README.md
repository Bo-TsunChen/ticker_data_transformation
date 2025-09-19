# Ticker Data Transformation

Two PySpark CLIs turn the sample ticker feed into daily observations and mid-quarter forecasts. The scripts are distilled from the exploratory notebooks so they can be run repeatably from the terminal.

## 1. Environment Setup
```bash
python -m venv .venv311
source .venv311/bin/activate        # adjust for your shell/OS
pip install pyspark pandas plotly    # pandas/plotly only needed for plotting
```

## 2. Input & Outputs
- **Source**: `data/index.csv` (multiple tickers, durations, cumulative values).
- **Daily output**: `output/dailyindex.csv`
- **Quarterly output**: `output/quarterlyindex.csv`
- Optional full forecast timeline: `output/quarterly_full.csv`

## 3. Daily Conversion Pipeline
Converts the lowest available cadence per ticker/index into a contiguous day-level series and keeps totals consistent.
```bash
python pipeline.py \
  --input data/index.csv \
  --output output/dailyindex.csv \
  [--plot] \
  [--app-name DailyTransformation]
```
What you will see on the console:
- `[daily] reading source CSV...`
- `[daily] expanding to day-level rows...`
- `[daily] writing output ...`
- `[daily] rendering cumulative plot...` (only when `--plot` is supplied)
- `[daily] completed.`

## 4. Quarterly Forecast Pipeline
Consumes the day-level feed and generates a hybrid run-rate + seasonal forecast. Mid-quarter is defined as two calendar months into the quarter.
```bash
python src/pipeline_quarterly.py \
  --input output/dailyindex.csv \
  --output output/quarterlyindex.csv \
  [--full-output output/quarterly_full.csv] \
  [--weight 0.7] \
  [--app-name QuarterlyForecast]
```
Console progress:
- `[quarterly] loading day-level feed...`
- `[quarterly] computing daily forecasts...`
- `[quarterly] selecting mid-quarter snapshots...`
- `[quarterly] writing midpoint output ...`
- `[quarterly] writing full timeline ...` (when `--full-output` is supplied)
- `[quarterly] completed.`

## 5. Assumptions & Data Requirements
- Spark runs locally (`local[*]`); no external cluster configuration is required.
- Required columns: `TICKER`, `INDEXNAME`, `DURATION`, `PERIODEND`, `VALUE`, `CUMULATIVEVALUE`.
- Daily feed must contain valid `Day` rows with numeric `VALUE`/`CUMULATIVEVALUE`.
- Forecast weight defaults to `0.7` run-rate / `0.3` seasonal; adjust using `--weight`.
- Forecast error is reported as `(forecast - actual) / actual`, rounded to four decimals.

## 6. Optional Enhancements
- Add schema & data-quality validation before running the pipelines.
- Surface forecast backtesting metrics (MAPE, RMSE) for each ticker/index.
- Wrap both commands under a single CLI entry point with `daily`/`quarterly` subcommands and dry-run support.
- Add automated tests around `src/utils.py` helpers to guard regressions.

## 7. Troubleshooting
| Issue | Likely Fix |
| --- | --- |
| `ModuleNotFoundError: pyspark` | Install requirements in the active virtual env (`pip install pyspark`). |
| Spark prints lots of WARN logs | Default level is forced to `ERROR`; if you still see noise, ensure no custom configs override it. |
| Output directories missing | The writers create directories automatically; check filesystem permissions if files are absent. |

## 8. Repository Layout
```
.
├── data/                # sample index feed
├── output/              # generated CSVs
├── src/
│   ├── pipeline_daily.py
│   ├── pipeline_quarterly.py
│   └── utils.py         # shared Spark helpers
├── runner_daily.ipynb   # exploratory notebook
├── runner_quarterly.ipynb
└── pipeline.py          # compatibility wrapper for daily CLI
```

Happy transforming!
