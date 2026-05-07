"""
Тесты для collector_python/main.py — pure-функции, без поднятия event loop
кроме одного интеграционного теста через asyncio.run.
"""
from __future__ import annotations

import asyncio
import json
import random
from pathlib import Path

import pytest


# ─── parse_duration_seconds ─────────────────────────────────────────────


def test_parse_duration_seconds_simple(collector_py_mod):
    fn = collector_py_mod.parse_duration_seconds
    assert fn("5s") == 5.0
    assert fn("10ms") == 0.01
    assert fn("2m") == 120.0
    assert fn("1h") == 3600.0


def test_parse_duration_seconds_default_unit(collector_py_mod):
    """Без юнита — секунды."""
    fn = collector_py_mod.parse_duration_seconds
    assert fn("30") == 30.0


def test_parse_duration_seconds_decimal(collector_py_mod):
    fn = collector_py_mod.parse_duration_seconds
    assert fn("1.5s") == 1.5


def test_parse_duration_seconds_invalid(collector_py_mod):
    fn = collector_py_mod.parse_duration_seconds
    with pytest.raises(ValueError):
        fn("not-a-duration")
    with pytest.raises(ValueError):
        fn("")


# ─── generate_value ─────────────────────────────────────────────────────


def test_generate_value_birth_rate_in_range(collector_py_mod):
    rng = random.Random(42)
    Region = collector_py_mod.Region
    region = Region(name="Москва", federal_district="ЦФО")
    for year in range(2000, 2024):
        v = collector_py_mod.generate_value(region, "birth_rate", year, rng)
        assert 2.0 <= v <= 50.0


def test_generate_value_death_rate_in_range(collector_py_mod):
    rng = random.Random(42)
    Region = collector_py_mod.Region
    region = Region(name="Тверская область", federal_district="ЦФО")
    for year in range(2000, 2024):
        v = collector_py_mod.generate_value(region, "death_rate", year, rng)
        assert 2.0 <= v <= 50.0


def test_generate_value_life_expectancy_in_range(collector_py_mod):
    rng = random.Random(42)
    Region = collector_py_mod.Region
    region = Region(name="Дагестан", federal_district="СКФО")
    for year in range(2000, 2024):
        v = collector_py_mod.generate_value(region, "life_expectancy", year, rng)
        assert 40.0 <= v <= 85.0


def test_generate_value_population_uses_base(collector_py_mod):
    """Москва должна давать ~12.5M, не дефолт ~1.2M."""
    rng = random.Random(42)
    Region = collector_py_mod.Region
    region = Region(name="Москва", federal_district="ЦФО")
    v = collector_py_mod.generate_value(region, "population", 2020, rng)
    # Москва ≈ 12.5M, разброс sigma=800k → диапазон [10M, 15M]
    assert v > 10_000_000
    assert v < 15_000_000


def test_generate_value_population_default_for_unknown_region(collector_py_mod):
    """Неизвестный регион → дефолт ~1.2M."""
    rng = random.Random(42)
    Region = collector_py_mod.Region
    region = Region(name="Несуществующий регион", federal_district="??")
    # Усредним 100 прогонов чтобы убрать шум — должно быть в районе 1.2M
    vals = [
        collector_py_mod.generate_value(region, "population", 2020, rng)
        for _ in range(100)
    ]
    avg = sum(vals) / len(vals)
    assert 500_000 < avg < 2_500_000


# ─── Region dataclass ───────────────────────────────────────────────────


def test_region_dataclass_fields(collector_py_mod):
    r = collector_py_mod.Region(name="Москва", federal_district="ЦФО")
    assert r.name == "Москва"
    assert r.federal_district == "ЦФО"


# ─── интеграция: run_collector через asyncio ────────────────────────────


def test_run_collector_writes_expected_count(collector_py_mod, tmp_path: Path):
    """Полный пайплайн: 4 региона × 24 года × 6 показателей = 576."""
    regions_file = tmp_path / "regions.json"
    regions_file.write_text(json.dumps([
        {"name": "Москва", "federal_district": "ЦФО"},
        {"name": "Санкт-Петербург", "federal_district": "СЗФО"},
        {"name": "Татарстан", "federal_district": "ПФО"},
        {"name": "Краснодарский край", "federal_district": "ЮФО"},
    ]), encoding="utf-8")

    output = tmp_path / "raw_python.ndjson"
    metrics = asyncio.run(
        collector_py_mod.run_collector(
            regions_file=regions_file,
            output_path=output,
            workers=4,
            batch_size=20,
            flush_interval_s=1.0,
            api_delay_ms=0.0,
        )
    )
    assert metrics["regions"] == 4
    assert metrics["written"] == 4 * 24 * 6
    assert output.exists()
    lines = output.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 4 * 24 * 6
    # Проверим что строки — валидный JSON с нужными полями
    rec = json.loads(lines[0])
    for k in ("region", "federal_district", "year", "indicator", "value", "collected_at"):
        assert k in rec
