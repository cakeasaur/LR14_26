"""
Python-сборщик на asyncio + aiohttp — функциональный аналог Go-сборщика
для бенчмарка (Задание 5б).

Логика та же:
  - 84 региона × 24 года × 6 показателей = 12 096 записей
  - параллельный сбор через asyncio.gather (semaphore ограничивает workers)
  - симулируется "запрос" с небольшой I/O-задержкой (asyncio.sleep)
  - пакетная запись NDJSON: flush по N записей или по T секунд
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp  # требуется по ТЗ; ClientSession поднимается в run_collector

INDICATORS = [
    "population", "birth_rate", "death_rate",
    "natural_growth", "migration_growth", "life_expectancy",
]

INDICATOR_PARAMS: dict[str, tuple[float, float]] = {
    "population":       (1_200_000, 800_000),
    "birth_rate":       (11.5, 2.5),
    "death_rate":       (13.0, 2.0),
    "natural_growth":   (-1.5, 3.0),
    "migration_growth": (0.5, 2.0),
    "life_expectancy":  (70.5, 3.5),
}

POPULATION_BASE: dict[str, float] = {
    "Москва": 12_500_000, "Санкт-Петербург": 5_400_000,
    "Московская область": 7_700_000, "Краснодарский край": 5_700_000,
    "Республика Башкортостан": 4_000_000, "Республика Татарстан": 3_900_000,
    "Свердловская область": 4_300_000, "Ростовская область": 4_200_000,
    "Нижегородская область": 3_200_000, "Челябинская область": 3_500_000,
}


@dataclass
class Region:
    name: str
    federal_district: str


def generate_value(region: Region, indicator: str, year: int, rng: random.Random) -> float:
    mu, sigma = INDICATOR_PARAMS[indicator]

    if indicator == "population":
        if region.name in POPULATION_BASE:
            mu = POPULATION_BASE[region.name]
        mu *= 1.0 + (year - 2000) * 0.003

    if indicator == "birth_rate":
        if 2006 <= year <= 2015:
            mu += 1.5
        elif year >= 2016:
            mu -= 0.5

    if indicator == "death_rate":
        if year >= 2005:
            mu -= (year - 2005) * 0.08
        if year in (2020, 2021):
            mu += 3.0

    if indicator == "life_expectancy":
        mu += (year - 2000) * 0.25
        if year in (2020, 2021):
            mu -= 2.0

    val = rng.normalvariate(mu, sigma)

    if indicator in ("birth_rate", "death_rate"):
        val = max(2.0, min(val, 50.0))
    elif indicator == "life_expectancy":
        val = max(40.0, min(val, 85.0))
    elif indicator == "population":
        val = max(10_000, val)

    return round(val, 1)


async def collect_region(
    region: Region,
    queue: asyncio.Queue,
    semaphore: asyncio.Semaphore,
    rng: random.Random,
    api_delay_ms: float,
) -> None:
    """Имитирует "запрос" к источнику Росстата по региону."""
    async with semaphore:
        # имитация I/O — даже без реального API сохраняем сравнимую асинхронность
        if api_delay_ms > 0:
            await asyncio.sleep(api_delay_ms / 1000.0)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for year in range(2000, 2024):
            for ind in INDICATORS:
                rec = {
                    "region": region.name,
                    "federal_district": region.federal_district,
                    "year": year,
                    "indicator": ind,
                    "value": generate_value(region, ind, year, rng),
                    "collected_at": now,
                }
                await queue.put(rec)


async def writer_task(
    queue: asyncio.Queue,
    out_path: Path,
    batch_size: int,
    flush_interval_s: float,
    stop_event: asyncio.Event,
) -> int:
    """Читает записи из очереди и пакетно пишет NDJSON."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    buf: list[dict[str, Any]] = []
    written = 0
    last_flush = time.monotonic()

    def flush(reason: str) -> int:
        nonlocal buf, last_flush
        if not buf:
            return 0
        with open(out_path, "a", encoding="utf-8") as f:
            for r in buf:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        n = len(buf)
        buf = []
        last_flush = time.monotonic()
        return n

    try:
        while not (stop_event.is_set() and queue.empty()):
            try:
                # ждём максимум до момента, когда сработает флаш-таймер
                timeout = max(0.05, flush_interval_s - (time.monotonic() - last_flush))
                rec = await asyncio.wait_for(queue.get(), timeout=timeout)
                buf.append(rec)
                if len(buf) >= batch_size:
                    written += flush("batch_full")
            except asyncio.TimeoutError:
                written += flush("timer")
    finally:
        written += flush("final")

    return written


async def run_collector(
    regions_file: Path,
    output_path: Path,
    workers: int,
    batch_size: int,
    flush_interval_s: float,
    api_delay_ms: float,
) -> dict[str, Any]:
    with open(regions_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    regions = [Region(name=r["name"], federal_district=r["federal_district"]) for r in raw]

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    semaphore = asyncio.Semaphore(workers)
    stop = asyncio.Event()
    rng = random.Random(time.time_ns())

    started = time.perf_counter()

    # ClientSession поднимается даже без реальных HTTP-вызовов: правильное
    # выделение/освобождение ресурсов согласно требованию ТЗ к aiohttp.
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as _session:
        writer = asyncio.create_task(
            writer_task(queue, output_path, batch_size, flush_interval_s, stop)
        )

        collectors = [
            asyncio.create_task(collect_region(r, queue, semaphore, rng, api_delay_ms))
            for r in regions
        ]
        await asyncio.gather(*collectors)
        stop.set()
        written = await writer

    elapsed = time.perf_counter() - started
    return {
        "regions": len(regions),
        "written": written,
        "elapsed_s": round(elapsed, 3),
        "rps": round(written / max(elapsed, 0.001), 1),
    }


def parse_duration_seconds(s: str) -> float:
    """'5s' / '500ms' / '2m' → seconds. Безопаснее чем .rstrip('s')."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*", s)
    if not m:
        raise ValueError(f"invalid duration: {s!r}")
    val = float(m.group(1))
    unit = m.group(2) or "s"
    return {"ms": val / 1000, "s": val, "m": val * 60, "h": val * 3600}[unit]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--regions-file", default="../collector/regions.json")
    parser.add_argument("--output", default="../data/raw_python.ndjson")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--window", default="10s")
    parser.add_argument("--api-delay-ms", type=float, default=0.0,
                        help="Симуляция I/O-задержки одного 'запроса'")
    args = parser.parse_args()

    flush_s = parse_duration_seconds(args.window)

    metrics = asyncio.run(run_collector(
        regions_file=Path(args.regions_file),
        output_path=Path(args.output),
        workers=args.workers,
        batch_size=args.batch,
        flush_interval_s=flush_s,
        api_delay_ms=args.api_delay_ms,
    ))

    print(json.dumps(metrics, ensure_ascii=False, indent=2), file=sys.stderr)


if __name__ == "__main__":
    main()
