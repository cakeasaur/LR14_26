"""
Общие фикстуры для pytest. Импортируем модули проекта через importlib,
поскольку у нас несколько `main.py` в разных директориях
(`analyzer/main.py` и `collector_python/main.py`) — обычный
sys.path-импорт даст конфликт имён.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent

# Добавляем dashboard/ в path, поскольку app.py обычно импортируется как
# самостоятельный модуль (там нет дублей имён).
sys.path.insert(0, str(ROOT / "dashboard"))


# ─── Mock Rust-крейта demographics_validator ───────────────────────────
# analyzer/main.py делает `import demographics_validator as dv` на module-level
# и падает с SystemExit, если крейт не установлен. Тесты не должны зависеть
# от собранного maturin-крейта, поэтому подставляем минимальную заглушку.
if "demographics_validator" not in sys.modules:
    _mock = ModuleType("demographics_validator")
    _mock.version = lambda: "0.1.0-mock"  # type: ignore[attr-defined]
    _mock.validate_record = lambda rec: {"valid": True, "errors": []}  # type: ignore[attr-defined]
    _mock.validate_batch = lambda recs: [  # type: ignore[attr-defined]
        {"valid": True, "errors": []} for _ in recs
    ]
    sys.modules["demographics_validator"] = _mock


def load_module(name: str, path: Path) -> ModuleType:
    """Загружает модуль по абсолютному пути под уникальным именем."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ─── модули проекта ─────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def kafka_consumer_mod() -> ModuleType:
    return load_module(
        "demographics_kafka_consumer",
        ROOT / "analyzer" / "kafka_consumer.py",
    )


@pytest.fixture(scope="session")
def dashboard_app_mod() -> ModuleType:
    # dashboard/app.py — есть top-level код в `if __name__ == "__main__":`,
    # но его не выполнит import.
    return load_module("demographics_dashboard_app", ROOT / "dashboard" / "app.py")


@pytest.fixture(scope="session")
def collector_py_mod() -> ModuleType:
    return load_module(
        "demographics_collector_py", ROOT / "collector_python" / "main.py"
    )


@pytest.fixture(scope="session")
def analyzer_main_mod() -> ModuleType:
    return load_module("demographics_analyzer_main", ROOT / "analyzer" / "main.py")


# ─── тестовые данные ────────────────────────────────────────────────────


@pytest.fixture
def sample_records() -> list[dict[str, Any]]:
    """Минимальный, но реалистичный набор записей: 4 региона, 2 года, 6 показателей."""
    records: list[dict[str, Any]] = []
    regions = [
        ("Москва", "ЦФО"),
        ("Санкт-Петербург", "СЗФО"),
        ("Татарстан", "ПФО"),
        ("Краснодарский край", "ЮФО"),
    ]
    indicators_with_values = {
        "population": [12_500_000, 5_400_000, 3_900_000, 5_700_000],
        "birth_rate": [10.0, 9.5, 12.0, 11.0],
        "death_rate": [13.0, 14.0, 12.5, 13.5],
        "natural_growth": [-3.0, -4.5, -0.5, -2.5],
        "migration_growth": [2.0, 1.0, 0.5, 1.5],
        "life_expectancy": [78.0, 76.5, 75.0, 73.0],
    }
    for year in (2022, 2023):
        for i, (region, fd) in enumerate(regions):
            for ind, vals in indicators_with_values.items():
                records.append({
                    "region": region,
                    "federal_district": fd,
                    "year": year,
                    "indicator": ind,
                    "value": vals[i] + (year - 2022) * 0.1,
                    "collected_at": "2024-01-01T00:00:00Z",
                })
    return records


@pytest.fixture
def sample_df(sample_records: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(sample_records)


@pytest.fixture
def ndjson_file(tmp_path: Path, sample_records: list[dict]) -> Path:
    p = tmp_path / "raw.ndjson"
    with open(p, "w", encoding="utf-8") as f:
        for r in sample_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        # одну _agg_ запись добавим для проверки фильтрации
        f.write(json.dumps({
            "region": "Москва", "federal_district": "_aggregated_",
            "year": 0, "indicator": "_agg_birth_rate",
            "value": 11.0, "collected_at": "2024-01-01T00:00:00Z",
        }, ensure_ascii=False) + "\n")
        # пустая строка
        f.write("\n")
    return p


@pytest.fixture
def parquet_file(tmp_path: Path, sample_df: pl.DataFrame) -> Path:
    p = tmp_path / "clean.parquet"
    sample_df.write_parquet(p, compression="zstd")
    return p
