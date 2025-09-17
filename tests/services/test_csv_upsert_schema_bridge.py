"""Regression coverage for CSV upsert canonicalisation."""

from __future__ import annotations

import csv

from autoflow.services.fees_fetcher.monthly_builder import OUTPUT_HEADER, upsert_csv


def test_upsert_csv_preserves_all_months_with_mixed_headers(tmp_path) -> None:
    csv_path = tmp_path / "monthly_rates.csv"

    existing_rows = [
        OUTPUT_HEADER,
        ["2023", "01", "6.9000", "2023-01-03", "2023-01-03", "safe_portal", "none"],
        ["2023", "02", "6.8500", "2023-02-02", "2023-02-02", "safe_portal", "none"],
        ["2023", "03", "6.7800", "2023-03-01", "2023-03-01", "safe_portal", "forward"],
    ]

    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(existing_rows)

    upsert_csv(
        csv_path,
        [
            {
                "year": "2023",
                "month": "04",
                "mid_rate": "6.6000",
                "query_date": "2023-04-03",
                "source_date": "2023-04-03",
                "rate_source": "safe_portal",
            },
            {
                "年份": "2023",
                "月份": "05",
                "中间价": "6.5500",
                "查询日期": "2023-05-05",
                "来源日期": "2023-05-05",
                "数据源": "safe_portal",
                "回退策略": "forward",
            },
        ],
    )

    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == OUTPUT_HEADER
        persisted_rows = list(reader)

    assert len(persisted_rows) == 5

    month_keys = [(row["年份"], row["月份"]) for row in persisted_rows]
    assert month_keys == [
        ("2023", "01"),
        ("2023", "02"),
        ("2023", "03"),
        ("2023", "04"),
        ("2023", "05"),
    ]

    rows_by_key = {key: row for key, row in zip(month_keys, persisted_rows)}
    assert rows_by_key[("2023", "04")]["回退策略"] == "none"
    assert rows_by_key[("2023", "04")]["查询日期"] == "2023-04-03"
    assert rows_by_key[("2023", "04")]["中间价"] == "6.6000"
    assert rows_by_key[("2023", "05")]["回退策略"] == "forward"
    assert rows_by_key[("2023", "03")]["中间价"] == "6.7800"
