"""
Persistence facade exposing XLSX-backed stores.
"""

from .stores.rates_store import (
    RatesStore,
    bulk_import_csv as bulk_import_rates_csv,
    init_rates_store,
    query_rates,
    rates_healthcheck,
    upsert_rate,
)
from .stores.pdf_store import PDFStore, init_pdf_store
from .stores.xlsx_store import XLSXStore, init_xlsx_store

__all__ = [
    "RatesStore",
    "PDFStore",
    "XLSXStore",
    "init_rates_store",
    "init_pdf_store",
    "init_xlsx_store",
    "upsert_rate",
    "bulk_import_rates_csv",
    "query_rates",
    "rates_healthcheck",
]
