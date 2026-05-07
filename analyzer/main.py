"""
analyzer/main.py — главный пайплайн обработки демографических данных.

Последовательность:
  1. Загрузить NDJSON (raw.ndjson) в polars.DataFrame
  2. Прогнать каждую запись через Rust-валидатор demographics_validator
  3. Отбросить невалидные, залогировать причины
  4. Сохранить чистый датасет в Parquet (с DuckDB-агрегациями)
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import polars as pl

try:
    import demographics_validator as dv
except ImportError as e:
    raise SystemExit(
        "❌ Rust-крейт demographics_validator не установлен.\n"
        "   Запусти из папки validator/:\n"
        "       maturin develop --release\n"
        f"   ({e})"
    )

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def load_raw(path: Path) -> list[dict]:
    """Читает NDJSON в список словарей."""
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            # пропускаем агрегированные служебные записи
            if rec.get("indicator", "").startswith("_agg_"):
                continue
            records.append(rec)
    return records


def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Прогоняет батч через Rust-валидатор. Возвращает (clean, rejected).
    rejected содержит {record, errors}.
    """
    print(f"  → версия валидатора: {dv.version()}")
    start = time.perf_counter()
    results = dv.validate_batch(records)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"  → валидация {len(records)} записей: {elapsed_ms:.1f} мс "
          f"({len(records) / max(elapsed_ms, 1) * 1000:.0f} записей/сек)")

    clean: list[dict] = []
    rejected: list[dict] = []
    error_counter: Counter[str] = Counter()

    for rec, res in zip(records, results):
        if res["valid"]:
            clean.append(rec)
        else:
            rejected.append({"record": rec, "errors": res["errors"]})
            for e in res["errors"]:
                # обрезаем числа из сообщения, чтобы агрегировать причины
                key = e.split(":")[0]
                error_counter[key] += 1

    if rejected:
        print(f"  ⚠ отклонено {len(rejected)} из {len(records)} записей")
        for cat, n in error_counter.most_common(10):
            print(f"      {cat}: {n}")
    else:
        print(f"  ✓ все {len(records)} записей валидны")

    return clean, rejected


def to_parquet(records: list[dict], out_path: Path) -> None:
    if not records:
        print("  ⚠ пустой датасет, пропускаем сохранение Parquet")
        return
    df = pl.DataFrame(records)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path, compression="zstd")
    print(f"  ✓ Parquet сохранён: {out_path} ({df.height} строк)")


def aggregate_with_duckdb(parquet_path: Path) -> None:
    """Несколько агрегаций через DuckDB (на лету, без переноса в БД)."""
    try:
        import duckdb
    except ImportError:
        print("  (duckdb не установлен — пропускаем агрегации)")
        return

    con = duckdb.connect()
    print("\n— Среднее по показателю и федеральному округу (последние 5 лет) —")
    print(con.execute(f"""
        SELECT federal_district, indicator,
               ROUND(AVG(value), 2) AS avg_val,
               COUNT(*)             AS n
        FROM read_parquet('{parquet_path.as_posix()}')
        WHERE year >= 2019
        GROUP BY federal_district, indicator
        ORDER BY federal_district, indicator
        LIMIT 20
    """).fetchdf())

    print("\n— Топ-10 регионов по продолжительности жизни (2023) —")
    print(con.execute(f"""
        SELECT region, ROUND(value, 1) AS life_expectancy
        FROM read_parquet('{parquet_path.as_posix()}')
        WHERE indicator = 'life_expectancy' AND year = 2023
        ORDER BY value DESC
        LIMIT 10
    """).fetchdf())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DATA_DIR / "raw.ndjson"))
    parser.add_argument("--output", default=str(DATA_DIR / "clean.parquet"))
    parser.add_argument("--rejected", default=str(DATA_DIR / "rejected.ndjson"))
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"❌ Входной файл не найден: {in_path}")

    print(f"→ Загрузка {in_path}…")
    records = load_raw(in_path)
    print(f"  загружено {len(records)} записей")

    print("\n→ Валидация (Rust crate)…")
    clean, rejected = validate_records(records)

    out_path = Path(args.output)
    print(f"\n→ Сохранение чистого датасета…")
    to_parquet(clean, out_path)

    if rejected:
        rej_path = Path(args.rejected)
        rej_path.parent.mkdir(parents=True, exist_ok=True)
        with open(rej_path, "w", encoding="utf-8") as f:
            for r in rejected:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  ✓ отклонённые записи: {rej_path}")

    if out_path.exists():
        aggregate_with_duckdb(out_path)


if __name__ == "__main__":
    main()
