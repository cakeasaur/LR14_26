"""
Тесты для analyzer/kafka_consumer.py — без поднятого Kafka.
Все Kafka-зависимые функции (make_consumer, main) пропускаем.
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest


# ─── SlidingWindow ──────────────────────────────────────────────────────


def test_sliding_window_add_and_records(kafka_consumer_mod):
    SW = kafka_consumer_mod.SlidingWindow
    win = SW(window_seconds=60)
    win.add({"value": 1}, ts_ns=1_000_000_000)
    win.add({"value": 2}, ts_ns=2_000_000_000)

    assert len(win) == 2
    recs = win.records()
    assert recs == [{"value": 1}, {"value": 2}]


def test_sliding_window_evict_old(kafka_consumer_mod):
    SW = kafka_consumer_mod.SlidingWindow
    win = SW(window_seconds=10)
    # 100с назад
    old_ts = 100_000_000_000
    new_ts = 200_000_000_000
    win.add({"id": "old"}, ts_ns=old_ts)
    win.add({"id": "new"}, ts_ns=new_ts)

    # окно 10 секунд от now=new_ts → cutoff = new_ts - 10e9 = 190e9 > old_ts
    evicted = win.evict(now_ns=new_ts)
    assert evicted == 1
    assert len(win) == 1
    assert win.records() == [{"id": "new"}]


def test_sliding_window_evict_nothing_when_all_fresh(kafka_consumer_mod):
    SW = kafka_consumer_mod.SlidingWindow
    win = SW(window_seconds=60)
    base = 1_000_000_000_000
    win.add({"id": "a"}, ts_ns=base)
    win.add({"id": "b"}, ts_ns=base + 5_000_000_000)
    evicted = win.evict(now_ns=base + 30_000_000_000)
    assert evicted == 0
    assert len(win) == 2


def test_sliding_window_empty(kafka_consumer_mod):
    SW = kafka_consumer_mod.SlidingWindow
    win = SW(window_seconds=10)
    assert len(win) == 0
    assert win.evict(now_ns=10_000_000_000) == 0
    assert win.records() == []


# ─── aggregate_top5_birth_rate ──────────────────────────────────────────


def test_aggregate_top5_returns_top5(kafka_consumer_mod, sample_records):
    fn = kafka_consumer_mod.aggregate_top5_birth_rate
    result = fn(sample_records)
    assert isinstance(result, pl.DataFrame)
    # У нас 4 региона, top-5 → не больше 4
    assert result.height <= 5
    assert result.height == 4
    cols = set(result.columns)
    assert "region" in cols
    assert "avg_birth_rate" in cols
    # Сортировка убывающая по avg_birth_rate
    values = result["avg_birth_rate"].to_list()
    assert values == sorted(values, reverse=True)


def test_aggregate_top5_empty(kafka_consumer_mod):
    fn = kafka_consumer_mod.aggregate_top5_birth_rate
    assert fn([]).is_empty()


def test_aggregate_top5_no_birth_rate_records(kafka_consumer_mod):
    """Записи только по другим показателям → пустой результат."""
    fn = kafka_consumer_mod.aggregate_top5_birth_rate
    recs = [
        {"region": "Москва", "indicator": "death_rate", "value": 13.0,
         "year": 2023, "federal_district": "ЦФО"},
    ]
    out = fn(recs)
    assert out.is_empty()


def test_aggregate_top5_missing_columns(kafka_consumer_mod):
    """Записи без поля indicator/value → возвращает пустой DF без падения."""
    fn = kafka_consumer_mod.aggregate_top5_birth_rate
    recs = [{"region": "X"}]  # нет indicator/value
    out = fn(recs)
    assert out.is_empty()


# ─── flush_parquet ──────────────────────────────────────────────────────


def test_flush_parquet_writes_file(kafka_consumer_mod, tmp_path: Path, sample_records):
    fn = kafka_consumer_mod.flush_parquet
    out = fn(sample_records, tmp_path)
    assert out is not None
    assert out.exists()
    assert out.suffix == ".parquet"
    # Проверим что содержимое читаемо
    df = pl.read_parquet(out)
    assert df.height == len(sample_records)


def test_flush_parquet_empty_returns_none(kafka_consumer_mod, tmp_path: Path):
    fn = kafka_consumer_mod.flush_parquet
    assert fn([], tmp_path) is None


def test_flush_parquet_fallback_to_ndjson_on_error(
    kafka_consumer_mod, tmp_path: Path, monkeypatch
):
    """Если pl.write_parquet падает — записи сохраняются в .ndjson."""
    fn = kafka_consumer_mod.flush_parquet

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    out = fn([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], tmp_path)
    assert out is not None
    assert out.suffix == ".ndjson"
    assert out.exists()
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1, "b": "x"}


# ─── make_consumer (smoke без подключения) ──────────────────────────────


def test_make_consumer_returns_object(kafka_consumer_mod):
    """Просто проверяем что возвращает Consumer-объект (не подключается до subscribe)."""
    c = kafka_consumer_mod.make_consumer("localhost:9092", "test-group")
    # confluent_kafka.Consumer принимает конфиг и создаёт C-объект
    assert c is not None
    # Без вызова poll/subscribe — соединение не устанавливается
    c.close()
