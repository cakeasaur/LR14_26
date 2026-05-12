"""
Тесты для analyzer/main.py — pure-функции (load_raw, to_parquet,
validate_with_rust, aggregate_with_duckdb).

После рефактора d788c9a весь пайплайн работает на pl.DataFrame, а
не на list[dict]: load_raw возвращает DataFrame, to_parquet ожидает
DataFrame, validate_with_rust принимает DataFrame и возвращает
(DataFrame, list[rejected]). aggregate_with_duckdb теперь требует
второй аргумент — замеренное время Polars-агрегации для сравнения.
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest


# ─── load_raw ───────────────────────────────────────────────────────────


def test_load_raw_reads_records(analyzer_main_mod, ndjson_file):
    fn = analyzer_main_mod.load_raw
    df = fn(ndjson_file)
    # фикстура: 4 региона × 2 года × 6 показателей = 48 строк + 1 _agg_,
    # _agg_ отфильтрована load_raw'ом → 48
    assert df.height == 48
    # не должно быть _agg_ записей
    assert not df["indicator"].str.starts_with("_agg_").any()


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
    df = fn(p)
    assert df.height == 2


def test_load_raw_skips_agg_records(analyzer_main_mod, tmp_path: Path):
    fn = analyzer_main_mod.load_raw
    p = tmp_path / "with_agg.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        f.write('{"region": "X", "indicator": "_agg_birth_rate", '
                '"year": 0, "value": 11.0, "federal_district": "_aggregated_"}\n')
        f.write('{"region": "Y", "indicator": "birth_rate", '
                '"year": 2023, "value": 10.0, "federal_district": "ЦФО"}\n')
    df = fn(p)
    assert df.height == 1
    assert df.row(0, named=True)["region"] == "Y"


# ─── to_parquet ─────────────────────────────────────────────────────────


def test_to_parquet_writes_zstd(analyzer_main_mod, tmp_path: Path, sample_df):
    fn = analyzer_main_mod.to_parquet
    out = tmp_path / "clean.parquet"
    fn(sample_df, out)
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == sample_df.height


def test_to_parquet_empty_skip(analyzer_main_mod, tmp_path: Path, capsys):
    fn = analyzer_main_mod.to_parquet
    out = tmp_path / "empty.parquet"
    fn(pl.DataFrame(), out)
    assert not out.exists()
    captured = capsys.readouterr()
    assert "пуст" in captured.out


# ─── validate_with_rust через mock ──────────────────────────────────────


def test_validate_with_rust_all_valid(analyzer_main_mod, sample_df, monkeypatch):
    """Подменяем Rust-валидатор на python mock — все валидны."""
    class MockDV:
        @staticmethod
        def version():
            return "0.1.0-mock"

        @staticmethod
        def validate_batch(records):
            return [{"valid": True, "errors": []} for _ in records]

    monkeypatch.setattr(analyzer_main_mod, "dv", MockDV)
    monkeypatch.setattr(analyzer_main_mod, "_HAS_VALIDATOR", True)
    clean, rejected = analyzer_main_mod.validate_with_rust(sample_df)
    assert clean.height == sample_df.height
    assert rejected == []


def test_validate_with_rust_filters_invalid(analyzer_main_mod, sample_df, monkeypatch):
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
    monkeypatch.setattr(analyzer_main_mod, "_HAS_VALIDATOR", True)
    clean, rejected = analyzer_main_mod.validate_with_rust(sample_df)
    assert clean.height == sample_df.height // 2
    assert len(rejected) == sample_df.height - clean.height
    # rejected — это {record, errors}
    assert "errors" in rejected[0]
    assert "record" in rejected[0]


# ─── aggregate_with_duckdb ──────────────────────────────────────────────


def test_aggregate_with_duckdb_smoke(analyzer_main_mod, parquet_file, capsys):
    """Smoke-test: запускается без падения, что-то печатает.
    Второй аргумент — время Polars-агрегации (для строки сравнения)."""
    fn = analyzer_main_mod.aggregate_with_duckdb
    fn(parquet_file, polars_agg_ms=1.0)
    captured = capsys.readouterr()
    assert "DuckDB" in captured.out
