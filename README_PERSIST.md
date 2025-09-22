# XLSX Persistence Layer

This module adds a lightweight XLSX-based persistence layer for FX mid rates fetched by the existing SAFE pipeline. It stores records under `~/AutoFlow/store` by default and keeps operations idempotent so daily jobs can be replayed safely.

## Requirements
- Python 3.13
- `pandas`, `openpyxl`, `PyYAML`, `decimal` (standard library)
- Write permission to `~/AutoFlow/{store,tmp,logs}` (or a custom `--root` supplied to the CLI)

## Key Files
- `autoflow_persist/stores/rates_store.py`: Implements init/import/upsert/query for `rates_store.xlsx`.
- `autoflow_persist/utils/excel_io.py`: Handles atomic XLSX writes and cooperative locking.
- `tools/rates_cli.py`: Typer CLI for initializing the store, importing CSV, upserting, and querying.
- `tools/persist_health.py`: Aggregated health check for permissions, dependencies, and locks.
- `rates_store.xlsx` columns: base_currency, quote_currency, rate_mid, rate_date, fetch_date, source, fallback_strategy, year, month, download_url, created_at, updated_at.

## CLI Examples
```bash
# Initialize the rates/pdf/xlsx stores under the default root
python -m tools.rates_cli init

# Import historical CSV and tag a download URL placeholder
python -m tools.rates_cli import-csv --path data/rates_2025.csv --base USD --quote CNY --source safe_portal --fallback forward --download-url ""

# Upsert one rate (download_url kept for future DingPan uploads)
python -m tools.rates_cli upsert --base USD --quote CNY --rate 7.1052 --rate-date 2025-09-04 --fetch-date 2025-09-01 --source safe_portal --fallback forward --download-url ""

# Query a date range for USD/CNY and preview 5 rows
python -m tools.rates_cli query --pair USD/CNY --from 2025-09-01 --to 2025-09-30

# Run health diagnostics for all stores
python -m tools.persist_health run
```

## Behaviour Notes
- All writes use a temporary file + atomic rename to avoid partial corruption.
- A simple lock file (`rates_store.xlsx.lock`) guards against concurrent writers; if you see “Workbook appears locked”, remove the lock file after confirming no job is running.
- `download_url` is persisted but not uploaded anywhere yet; it is a placeholder column for future DingPan integrations.
- Decimal values are normalized to four decimals when stored; internal calculations keep full `Decimal` precision.
- CSV imports understand the existing SAFE headers (`年份`, `月份`, `查询日期`, etc.) and perform upserts keyed by `(base, quote, rate_date)`.

## Troubleshooting
- Missing dependencies (`ModuleNotFoundError: openpyxl`): install the listed requirements.
- Permission errors when writing `~/AutoFlow/store`: run the CLI with a writable `--root` (e.g. a tmp directory) or adjust filesystem permissions.
- `Workbook appears locked`: ensure no other process is using the file, then delete the `.lock` file if safe. For cross-host coordination consider upgrading to a database such as SQLite (not included here).
- Excel file opened in another program: close the workbook before rerunning the CLI; Windows keeps hard locks that will surface as “Access denied”.

## Testing
Use pytest to validate the workflow end to end:
```bash
pytest tests/test_rates_store.py
```
