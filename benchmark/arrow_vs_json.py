"""
Бенчмарк: сравнение Arrow Flight vs JSON-файл при чтении 10 000 записей.
Сохраняет результаты в benchmark/results.md.
"""
from __future__ import annotations

import json
import os
import time
import tracemalloc
from pathlib import Path

import polars as pl

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "analyzer"))
from arrow_client import fetch_demographics  # noqa: E402


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DATA_DIR = ROOT / "data"
RESULTS_MD = HERE / "results.md"

TARGET_ROWS = 10_000


def benchmark_json(path: Path, limit: int) -> dict:
    """Читает первые `limit` записей из NDJSON в polars.DataFrame."""
    tracemalloc.start()
    start = time.perf_counter()

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            rows.append(json.loads(line))

    df = pl.DataFrame(rows)

    elapsed_ms = (time.perf_counter() - start) * 1000
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    file_size_kb = os.path.getsize(path) / 1024  # размер всего файла
    return {
        "method": "JSON файл",
        "rows": df.height,
        "time_ms": round(elapsed_ms, 2),
        "size_kb": round(file_size_kb, 2),
        "memory_mb": round(peak / 1024 / 1024, 2),
    }


def benchmark_arrow() -> dict:
    """Получает 10к записей через Arrow Flight."""
    tracemalloc.start()
    df, m = fetch_demographics(
        address="grpc://localhost:8815",
        # без фильтра по году — берём всё, потом обрежем
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        "method": "Arrow Flight",
        "rows": min(df.height, TARGET_ROWS),
        "time_ms": m["transfer_ms"],
        "size_kb": round(m["arrow_bytes"] / 1024, 2),
        "memory_mb": round(peak / 1024 / 1024, 2),
    }


def render_markdown(rows: list[dict]) -> str:
    lines = [
        "# Бенчмарк: Arrow Flight vs JSON",
        "",
        f"Цель: {TARGET_ROWS} записей",
        "",
        "| Метод        | Время (мс) | Размер (KB) | Память (MB) | Строк |",
        "|--------------|-----------:|------------:|------------:|------:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['method']:<12} | {r['time_ms']:>10} | "
            f"{r['size_kb']:>11} | {r['memory_mb']:>11} | {r['rows']:>5} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    raw_path = DATA_DIR / "raw.ndjson"
    if not raw_path.exists():
        print(f"⚠ {raw_path} не найден. Сначала запусти сборщик.")
        return

    results = []

    print("→ JSON файл…")
    try:
        results.append(benchmark_json(raw_path, TARGET_ROWS))
    except Exception as e:
        print(f"  Ошибка JSON: {e}")

    print("→ Arrow Flight…")
    try:
        results.append(benchmark_arrow())
    except Exception as e:
        print(f"  Ошибка Arrow: {e}")
        print("  Убедись, что Go-сервер запущен:")
        print("    cd collector && go run . --flight-only --flight-source=../data/raw.ndjson")

    md = render_markdown(results)
    print("\n" + md)
    RESULTS_MD.write_text(md, encoding="utf-8")
    print(f"\n✓ Результаты сохранены в {RESULTS_MD}")


if __name__ == "__main__":
    main()
