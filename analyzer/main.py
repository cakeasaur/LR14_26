"""
analyzer/main.py — главный пайплайн обработки демографических данных.

Последовательность:
  1. Загрузить NDJSON (raw.ndjson) в polars.DataFrame через pl.read_ndjson
  2. Очистить и провалидировать данные средствами Polars
  3. Опционально прогнать через Rust-валидатор demographics_validator
  4. Агрегировать по (federal_district, indicator) через Polars GROUP BY
  5. Сохранить чистый датасет в Parquet
  6. Выполнить SQL-анализ через DuckDB, замерить время vs Polars
  7. Сохранить 2 графика в файлы
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import polars as pl
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"

try:
    import demographics_validator as dv
    _HAS_VALIDATOR = True
except ImportError:
    _HAS_VALIDATOR = False


# ── 1. Загрузка ──────────────────────────────────────────────────────────

def load_raw(path: Path) -> pl.DataFrame:
    """Читает NDJSON в DataFrame через pl.read_ndjson (нативный путь Polars)."""
    df = pl.read_ndjson(path)
    # отбрасываем служебные агрегированные записи
    if "indicator" in df.columns:
        df = df.filter(~pl.col("indicator").str.starts_with("_agg_"))
    return df


# ── 2. Очистка и валидация (Polars) ──────────────────────────────────────

def clean_with_polars(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Очистка средствами Polars: дубликаты, null, диапазоны, типы."""
    before = df.height

    df = (
        df
        # удаляем строки с null в обязательных полях
        .drop_nulls(subset=["region", "indicator", "value", "year"])
        # удаляем дубликаты по естественному ключу
        .unique(subset=["region", "indicator", "year"], keep="first")
        # фильтруем физически невозможные значения
        .filter(pl.col("year").is_between(1990, 2030))
        .filter(pl.col("value").is_finite())
        # приводим типы
        .with_columns([
            pl.col("year").cast(pl.Int32),
            pl.col("value").cast(pl.Float64),
        ])
    )

    dropped = before - df.height
    if dropped:
        print(f"  Polars: отброшено {dropped} строк (дубли/null/выбросы)")
    else:
        print(f"  Polars: все {before} строк прошли очистку")

    return df, dropped


# ── 3. Опциональная Rust-валидация ───────────────────────────────────────

def validate_with_rust(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict]]:
    """Прогоняет батч через Rust-крейт (если установлен). Возвращает (clean_df, rejected)."""
    if not _HAS_VALIDATOR:
        print("  (demographics_validator не установлен — пропускаем Rust-валидацию)")
        return df, []

    print(f"  → версия валидатора: {dv.version()}")
    records = df.to_dicts()

    start = time.perf_counter()
    results = dv.validate_batch(records)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"  → Rust: {len(records)} записей за {elapsed_ms:.1f} мс "
          f"({len(records) / max(elapsed_ms, 1) * 1000:.0f} зап/сек)")

    clean_records: list[dict] = []
    rejected: list[dict] = []
    error_counter: Counter[str] = Counter()

    for rec, res in zip(records, results):
        if res["valid"]:
            clean_records.append(rec)
        else:
            rejected.append({"record": rec, "errors": res["errors"]})
            for e in res["errors"]:
                error_counter[e.split(":")[0]] += 1

    if rejected:
        print(f"  ⚠ Rust отклонил {len(rejected)} из {len(records)} записей")
        for cat, n in error_counter.most_common(5):
            print(f"      {cat}: {n}")
    else:
        print(f"  ✓ Rust: все {len(records)} валидны")

    return pl.DataFrame(clean_records), rejected


# ── 4. Агрегация через Polars GROUP BY ───────────────────────────────────

def aggregate_with_polars(df: pl.DataFrame) -> pl.DataFrame:
    """GROUP BY (federal_district, indicator): SUM, AVG, MIN, MAX, COUNT."""
    t0 = time.perf_counter()
    agg = (
        df
        .group_by(["federal_district", "indicator"])
        .agg([
            pl.col("value").sum().alias("sum"),
            pl.col("value").mean().round(2).alias("avg"),
            pl.col("value").min().alias("min"),
            pl.col("value").max().alias("max"),
            pl.len().alias("count"),
        ])
        .sort(["federal_district", "indicator"])
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"\n— Агрегация Polars GROUP BY ({elapsed_ms:.1f} мс) —")
    print(agg)
    return agg


# ── 5. Сохранение в Parquet ───────────────────────────────────────────────

def to_parquet(df: pl.DataFrame, out_path: Path) -> None:
    if not df.height:
        print("  ⚠ пустой датасет, пропускаем сохранение Parquet")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path, compression="zstd")
    print(f"  ✓ Parquet: {out_path} ({df.height} строк)")


# ── 6. Анализ через DuckDB ────────────────────────────────────────────────

def aggregate_with_duckdb(parquet_path: Path, polars_agg_ms: float) -> None:
    try:
        import duckdb
    except ImportError:
        print("  (duckdb не установлен — пропускаем)")
        return

    con = duckdb.connect()
    # con.read_parquet() — стабильный API, возвращает Relation без SQL-инъекции
    rel = con.read_parquet(str(parquet_path))
    con.register("demo", rel)

    t0 = time.perf_counter()

    print("\n— DuckDB: среднее по показателю и округу (2019–2023) —")
    print(con.execute("""
        SELECT federal_district, indicator,
               ROUND(AVG(value), 2) AS avg_val,
               COUNT(*)             AS n
        FROM demo
        WHERE year >= 2019
        GROUP BY federal_district, indicator
        ORDER BY federal_district, indicator
        LIMIT 20
    """).fetchdf())

    print("\n— DuckDB: топ-10 регионов по продолжительности жизни (2023) —")
    print(con.execute("""
        SELECT region, ROUND(value, 1) AS life_expectancy
        FROM demo
        WHERE indicator = 'life_expectancy' AND year = 2023
        ORDER BY value DESC
        LIMIT 10
    """).fetchdf())

    duckdb_ms = (time.perf_counter() - t0) * 1000
    print(f"\n— Сравнение производительности —")
    print(f"  Polars GROUP BY : {polars_agg_ms:.1f} мс")
    print(f"  DuckDB (2 запроса): {duckdb_ms:.1f} мс")


# ── 7. Визуализации ───────────────────────────────────────────────────────

def save_charts(df: pl.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # График 1: временной ряд средней продолжительности жизни по ФО
    le = (
        df.filter(pl.col("indicator") == "life_expectancy")
        .group_by(["federal_district", "year"])
        .agg(pl.col("value").mean().alias("avg"))
        .sort("year")
    )
    if le.height:
        fig, ax = plt.subplots(figsize=(12, 6))
        for district, group in le.to_pandas().groupby("federal_district"):
            ax.plot(group["year"], group["avg"], marker="o", markersize=3, label=district)
        ax.set_title("Средняя ожидаемая продолжительность жизни по федеральным округам")
        ax.set_xlabel("Год")
        ax.set_ylabel("Лет")
        ax.legend(fontsize=7, loc="lower right")
        ax.grid(True, alpha=0.3)
        path1 = out_dir / "life_expectancy_dynamics.png"
        fig.savefig(path1, dpi=120, bbox_inches="tight")
        plt.close(fig)
        print(f"  ✓ график 1: {path1}")

    # График 2: гистограмма распределения рождаемости
    br = df.filter(pl.col("indicator") == "birth_rate")["value"]
    if br.len():
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(br.to_numpy(), bins=40, color="steelblue", edgecolor="white")
        ax.set_title("Распределение рождаемости по регионам (все годы)")
        ax.set_xlabel("Рождаемость (на 1000 чел.)")
        ax.set_ylabel("Количество наблюдений")
        ax.grid(True, alpha=0.3, axis="y")
        path2 = out_dir / "birth_rate_histogram.png"
        fig.savefig(path2, dpi=120, bbox_inches="tight")
        plt.close(fig)
        print(f"  ✓ график 2: {path2}")


# ── main ──────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    default=str(DATA_DIR / "raw.ndjson"))
    parser.add_argument("--output",   default=str(DATA_DIR / "clean.parquet"))
    parser.add_argument("--rejected", default=str(DATA_DIR / "rejected.ndjson"))
    parser.add_argument("--charts",   default=str(OUTPUT_DIR))
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"❌ Входной файл не найден: {in_path}")

    print(f"→ Загрузка {in_path} (pl.read_ndjson)…")
    df = load_raw(in_path)
    print(f"  загружено {df.height} записей, колонки: {df.columns}")
    print(df.head(5))

    print("\n→ Очистка (Polars)…")
    df_clean, _ = clean_with_polars(df)

    print("\n→ Rust-валидация (опционально)…")
    df_clean, rejected = validate_with_rust(df_clean)

    print("\n→ Агрегация (Polars GROUP BY)…")
    t0 = time.perf_counter()
    polars_agg = aggregate_with_polars(df_clean)
    polars_agg_ms = (time.perf_counter() - t0) * 1000

    out_path = Path(args.output)
    print(f"\n→ Сохранение Parquet…")
    to_parquet(df_clean, out_path)

    if rejected:
        rej_path = Path(args.rejected)
        rej_path.parent.mkdir(parents=True, exist_ok=True)
        with open(rej_path, "w", encoding="utf-8") as f:
            for r in rejected:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  ✓ отклонённые: {rej_path}")

    if out_path.exists():
        aggregate_with_duckdb(out_path, polars_agg_ms)

    print(f"\n→ Сохранение графиков…")
    save_charts(df_clean, Path(args.charts))


if __name__ == "__main__":
    main()
