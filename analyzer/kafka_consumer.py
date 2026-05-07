"""
Kafka-консьюмер демографических данных со sliding window 5 минут.

Логика:
  - Читает топик demographics-raw (confluent-kafka)
  - Хранит deque записей с timestamp = время прихода в консьюмер
  - Каждые 30 секунд пересчитывает агрегаты по записям за последние 5 минут:
      * топ-5 регионов по birth_rate (avg)
  - При накоплении 1000 записей флашит их в Parquet с датой в имени
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
from confluent_kafka import Consumer, KafkaError, KafkaException

# ─── параметры ──────────────────────────────────────────────────────────
DEFAULT_TOPIC = "demographics-raw"
DEFAULT_BROKERS = "localhost:9092"
DEFAULT_GROUP = "demographics-analyzer"

WINDOW_SECONDS = 5 * 60     # 5 минут
TICK_SECONDS = 30           # каждые 30 секунд считаем агрегаты
PARQUET_FLUSH_THRESHOLD = 1000

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "kafka"


# ─── windowed buffer ────────────────────────────────────────────────────
class SlidingWindow:
    """Очередь (timestamp_ns, record_dict). Удаляет хвост старше окна."""

    def __init__(self, window_seconds: int):
        self.window_ns = window_seconds * 1_000_000_000
        self.buf: deque[tuple[int, dict]] = deque()

    def add(self, record: dict, ts_ns: int) -> None:
        self.buf.append((ts_ns, record))

    def evict(self, now_ns: int) -> int:
        """Удаляет всё что старше окна. Возвращает сколько удалено."""
        cutoff = now_ns - self.window_ns
        evicted = 0
        while self.buf and self.buf[0][0] < cutoff:
            self.buf.popleft()
            evicted += 1
        return evicted

    def records(self) -> list[dict]:
        return [r for _, r in self.buf]

    def __len__(self) -> int:
        return len(self.buf)


def make_consumer(brokers: str, group: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10_000,
    })


def aggregate_top5_birth_rate(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame()
    df = pl.DataFrame(records)
    if "indicator" not in df.columns or "value" not in df.columns:
        return pl.DataFrame()
    return (
        df.filter(pl.col("indicator") == "birth_rate")
        .group_by("region")
        .agg([
            pl.col("value").mean().alias("avg_birth_rate"),
            pl.len().alias("n"),
        ])
        .sort("avg_birth_rate", descending=True)
        .head(5)
    )


def flush_parquet(records: list[dict], out_dir: Path) -> Path | None:
    if not records:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    path = out_dir / f"demographics_{ts}.parquet"
    try:
        df = pl.DataFrame(records)
        df.write_parquet(path, compression="zstd")
    except Exception as e:
        # При ошибке записи не теряем буфер — пишем в emergency NDJSON,
        # чтобы данные можно было восстановить позже.
        ndjson_path = path.with_suffix(".ndjson")
        with open(ndjson_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  ⚠ parquet flush failed ({e}), saved as {ndjson_path.name}")
        return ndjson_path
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default=DEFAULT_BROKERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group", default=DEFAULT_GROUP)
    parser.add_argument("--out-dir", default=str(DATA_DIR))
    parser.add_argument("--window-seconds", type=int, default=WINDOW_SECONDS)
    parser.add_argument("--tick-seconds", type=int, default=TICK_SECONDS)
    parser.add_argument("--flush-threshold", type=int, default=PARQUET_FLUSH_THRESHOLD)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    consumer = make_consumer(args.brokers, args.group)
    consumer.subscribe([args.topic])

    print(f"→ Subscribed to '{args.topic}' on {args.brokers}")
    print(f"  window={args.window_seconds}s tick={args.tick_seconds}s "
          f"flush={args.flush_threshold} records → Parquet")

    window = SlidingWindow(args.window_seconds)
    pending: list[dict] = []  # буфер для парквет-флаша
    last_tick = time.monotonic()

    # graceful shutdown
    stop = False

    def handle_sigterm(*_: Any) -> None:
        nonlocal stop
        stop = True
        print("\n⏹  shutdown requested…")

    signal.signal(signal.SIGINT, handle_sigterm)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_sigterm)

    total_consumed = 0
    total_flushed = 0
    try:
        while not stop:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass  # таймаут — переходим к проверке тика
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            else:
                try:
                    rec = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                ts_ns = time.time_ns()
                window.add(rec, ts_ns)
                pending.append(rec)
                total_consumed += 1

                if len(pending) >= args.flush_threshold:
                    path = flush_parquet(pending, out_dir)
                    total_flushed += len(pending)
                    print(f"  💾 Parquet: {path.name} ({len(pending)} записей, "
                          f"всего флашов: {total_flushed})")
                    pending = []

            # тик каждые tick_seconds — пересчёт агрегатов + tick-flush pending
            if time.monotonic() - last_tick >= args.tick_seconds:
                evicted = window.evict(time.time_ns())
                top = aggregate_top5_birth_rate(window.records())
                print(f"\n— Sliding window {args.window_seconds}s: "
                      f"{len(window)} записей (evicted {evicted}), "
                      f"consumed total {total_consumed} —")
                if top.height > 0:
                    print("Топ-5 регионов по birth_rate:")
                    print(top)
                else:
                    print("  (нет birth_rate-записей в окне)")

                # Флашим pending по тику: без этого при остановке потока
                # данных накопленные <1000 записей зависли бы в RAM до SIGINT.
                if pending:
                    path = flush_parquet(pending, out_dir)
                    total_flushed += len(pending)
                    print(f"  💾 Tick flush: {path.name if path else '—'} "
                          f"({len(pending)} записей)")
                    pending = []

                last_tick = time.monotonic()
    finally:
        # флашим хвост перед выходом
        if pending:
            path = flush_parquet(pending, out_dir)
            print(f"  💾 Final flush: {path.name} ({len(pending)} записей)")
        consumer.close()
        print(f"✓ Закрылся. Consumed={total_consumed}, flushed={total_flushed}")


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        print(f"❌ Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
