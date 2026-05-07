"""
Тесты для analyzer/main.py — pure-функции (load_raw, to_parquet).
validate_records зависит от Rust-крейта demographics_validator (binding'и),
поэтому тестируем отдельно через моки. aggregate_with_duckdb — smoke-тест
с реальным parquet.
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest


# ─── load_raw ───────────────────────────────────────────────────────────


def test_load_raw_reads_records(analyzer_main_mod, ndjson_file):
    fn = analyzer_main_mod.load_raw
    records = fn(ndjson_file)
    # фикстура содержит 8 (4 region × 2 year × 6 ind…) — но через ndjson_file
    # 4 регионa × 2 года × 6 показателей = 48 + 1 _agg_ = 49 строк, _agg_ должна
    # быть отфильтрована, итого 48
    assert len(records) == 48
    # не должно быть _agg_ записей
    assert not any(r["indicator"].startswith("_agg_") for r in records)


def test_load_raw_skips_empty_lines(analyzer_main_mod, tmp_path: Path):
    fn = analyzer_main_mod.load_raw
    p = tmp_path / "with_empty.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        f.write('{"region": "A", "indicator": "birth_rate", "year": 2023, '
                '"value": 10.0, "federal_district": "ЦФО"}\n')
        f.write("\n")
        f.write("   \n")
        f.write('{"region": "B", "indicator": "death_rate", "year": 2023, '
                '"value": 13.0, "federal_district": "ЦФО"}\n')
    records = fn(p)
    assert len(records) == 2


def test_load_raw_skips_agg_records(analyzer_main_mod, tmp_path: Path):
    fn = analyzer_main_mod.load_raw
    p = tmp_path / "with_agg.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        f.write('{"region": "X", "indicator": "_agg_birth_rate", '
                '"year": 0, "value": 11.0, "federal_district": "_aggregated_"}\n')
        f.write('{"region": "Y", "indicator": "birth_rate", '
                '"year": 2023, "value": 10.0, "federal_district": "ЦФО"}\n')
    records = fn(p)
    assert len(records) == 1
    assert records[0]["region"] == "Y"


# ─── to_parquet ─────────────────────────────────────────────────────────


def test_to_parquet_writes_zstd(analyzer_main_mod, tmp_path: Path, sample_records):
    fn = analyzer_main_mod.to_parquet
    out = tmp_path / "clean.parquet"
    fn(sample_records, out)
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == len(sample_records)


def test_to_parquet_empty_skip(analyzer_main_mod, tmp_path: Path, capsys):
    fn = analyzer_main_mod.to_parquet
    out = tmp_path / "empty.parquet"
    fn([], out)
    assert not out.exists()
    captured = capsys.readouterr()
    assert "пуст" in captured.out


# ─── validate_records через mock ────────────────────────────────────────


def test_validate_records_with_mock(analyzer_main_mod, sample_records, monkeypatch):
    """Подменяем Rust-валидатор на python mock — все валидны."""
    class MockDV:
        @staticmethod
        def version():
            return "0.1.0-mock"

        @staticmethod
        def validate_batch(records):
            return [{"valid": True, "errors": []} for _ in records]

    monkeypatch.setattr(analyzer_main_mod, "dv", MockDV)
    clean, rejected = analyzer_main_mod.validate_records(sample_records)
    assert len(clean) == len(sample_records)
    assert rejected == []


def test_validate_records_filters_invalid(analyzer_main_mod, sample_records, monkeypatch):
    """Mock возвращает половину записей как невалидные."""
    class MockDV:
        @staticmethod
        def version():
            return "0.1.0-mock"

        @staticmethod
        def validate_batch(records):
            results = []
            for i, _ in enumerate(records):
                if i % 2 == 0:
                    results.append({"valid": False, "errors": ["region: missing"]})
                else:
                    results.append({"valid": True, "errors": []})
            return results

    monkeypatch.setattr(analyzer_main_mod, "dv", MockDV)
    clean, rejected = analyzer_main_mod.validate_records(sample_records)
    assert len(clean) == len(sample_records) // 2
    assert len(rejected) == len(sample_records) // 2
    # rejected — это {record, errors}
    assert "errors" in rejected[0]
    assert "record" in rejected[0]


# ─── aggregate_with_duckdb ──────────────────────────────────────────────


def test_aggregate_with_duckdb_smoke(analyzer_main_mod, parquet_file, capsys):
    """Smoke-test: запускается без падения, что-то печатает."""
    fn = analyzer_main_mod.aggregate_with_duckdb
    fn(parquet_file)
    captured = capsys.readouterr()
    assert "федеральному" in captured.out or "округу" in captured.out
