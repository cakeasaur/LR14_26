"""
Arrow Flight клиент — подключается к Go-серверу на grpc://localhost:8815,
получает поток RecordBatch, конвертирует в polars.DataFrame.
"""
from __future__ import annotations

import argparse
import json
import time

import pyarrow as pa
import pyarrow.flight as flight
import polars as pl


def fetch_demographics(
    address: str = "grpc://localhost:8815",
    indicator: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    region: str | None = None,
) -> tuple[pl.DataFrame, dict]:
    """
    Запрашивает данные у Arrow Flight сервера, возвращает (polars.DataFrame, metrics).

    metrics: {
        'rows': N,
        'batches': M,
        'transfer_ms': float,
        'arrow_bytes': int,     # суммарный размер RecordBatch'ей
        'allocated_mb': float,  # текущая аллокация Arrow allocator (не пик)
    }
    """
    ticket_payload: dict = {}
    if indicator:
        ticket_payload["indicator"] = indicator
    if year_from:
        ticket_payload["year_from"] = year_from
    if year_to:
        ticket_payload["year_to"] = year_to
    if region:
        ticket_payload["region"] = region

    ticket = flight.Ticket(json.dumps(ticket_payload).encode("utf-8"))

    # Контекстный менеджер гарантирует освобождение gRPC-канала.
    with flight.FlightClient(address) as client:
        start = time.perf_counter()
        reader = client.do_get(ticket)

        batches: list[pa.RecordBatch] = []
        total_bytes = 0
        for chunk in reader:
            rb = chunk.data
            batches.append(rb)
            total_bytes += rb.nbytes

        elapsed_ms = (time.perf_counter() - start) * 1000

    if not batches:
        df = pl.DataFrame()
    else:
        table = pa.Table.from_batches(batches)
        df = pl.from_arrow(table)

    metrics = {
        "rows": df.height,
        "batches": len(batches),
        "transfer_ms": round(elapsed_ms, 2),
        "arrow_bytes": total_bytes,
        "allocated_mb": round(pa.total_allocated_bytes() / 1024 / 1024, 2),
    }
    return df, metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Arrow Flight клиент для демографических данных")
    parser.add_argument("--address", default="grpc://localhost:8815")
    parser.add_argument("--indicator", default="birth_rate")
    parser.add_argument("--year-from", type=int, default=2010)
    parser.add_argument("--year-to", type=int, default=2023)
    parser.add_argument("--region", default=None)
    args = parser.parse_args()

    print(f"Подключаюсь к {args.address}…")
    df, m = fetch_demographics(
        address=args.address,
        indicator=args.indicator,
        year_from=args.year_from,
        year_to=args.year_to,
        region=args.region,
    )

    print(f"\nПолучено: {m['rows']} строк, {m['batches']} батчей")
    print(f"Время передачи: {m['transfer_ms']} мс")
    print(f"Размер Arrow: {m['arrow_bytes'] / 1024:.1f} KB")
    print(f"Allocated: {m['allocated_mb']} MB\n")

    if df.height:
        print(df.head(10))
        # групповая статистика
        agg = (
            df.group_by("federal_district")
            .agg([
                pl.col("value").mean().alias("avg"),
                pl.col("value").min().alias("min"),
                pl.col("value").max().alias("max"),
                pl.len().alias("count"),
            ])
            .sort("avg", descending=True)
        )
        print("\nПо округам:")
        print(agg)


if __name__ == "__main__":
    main()
