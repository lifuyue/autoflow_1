from __future__ import annotations

import time
from datetime import date
from decimal import Decimal
from pathlib import Path

from autoflow_persist.schemas.rates import RatesQuery, RatesRecord
from autoflow_persist.stores.rates_store import (
    RatesStore,
    bulk_import_csv,
    init_rates_store,
    query_rates,
    upsert_rate,
)


def _build_record() -> RatesRecord:
    return RatesRecord(
        base_currency="USD",
        quote_currency="CNY",
        rate_mid=Decimal("7.1052"),
        rate_date=date(2025, 9, 4),
        fetch_date=date(2025, 9, 1),
        source="safe_portal",
        fallback_strategy="forward",
        download_url="",
    )


def test_rates_store_workflow(tmp_path: Path) -> None:
    root = tmp_path / "persist"

    store_path = init_rates_store(root)
    assert store_path.exists()

    record = _build_record()
    upsert_rate(record, root=root)

    initial_df = query_rates(RatesQuery(base_currency="USD", quote_currency="CNY"), root=root)
    assert len(initial_df) == 1
    row = initial_df.iloc[0]
    assert row["rate_mid"] == Decimal("7.1052")
    created_at = row["created_at"]
    updated_at = row["updated_at"]

    time.sleep(1)
    upsert_rate(record, root=root, download_url="https://example.com/rate.xlsx")
    second_df = query_rates(RatesQuery(base_currency="USD", quote_currency="CNY"), root=root)
    assert len(second_df) == 1
    second_row = second_df.iloc[0]
    assert second_row["rate_mid"] == Decimal("7.1052")
    assert second_row["created_at"] == created_at
    assert second_row["updated_at"] >= updated_at
    assert second_row["download_url"] == "https://example.com/rate.xlsx"

    csv_path = tmp_path / "rates.csv"
    csv_path.write_text(
        "年份,月份,中间价,查询日期,来源日期,数据源,回退策略\n"
        "2025,09,7.2000,2025-09-04,2025-09-04,safe_portal,forward\n"
        "2025,10,7.1888,2025-10-08,2025-10-08,safe_portal,forward\n",
        encoding="utf-8",
    )

    imported = bulk_import_csv(
        csv_path,
        base="USD",
        quote="CNY",
        source="safe_portal",
        fallback="forward",
        root=root,
        download_url=None,
    )
    assert imported == 2

    query = RatesQuery(base_currency="USD", quote_currency="CNY", start_date=date(2025, 9, 1), end_date=date(2025, 9, 30))
    september_df = query_rates(query, root=root)
    assert len(september_df) == 1
    assert september_df.iloc[0]["rate_mid"] == Decimal("7.2000")

    october_df = query_rates(RatesQuery(base_currency="USD", quote_currency="CNY", start_date=date(2025, 10, 1)), root=root)
    assert len(october_df) == 1
    assert october_df.iloc[0]["rate_mid"] == Decimal("7.1888")
