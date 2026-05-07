"""
Бенчмарк: сравнение Go-сборщика и Python-сборщика (asyncio + aiohttp)
на одинаковой нагрузке. Запускает оба как subprocess, через psutil
снимает peak memory + средний CPU, парсит количество записей из NDJSON.

Сохраняет таблицу в benchmark/results.md, рисует bar-chart через plotly.
"""
from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Windows-консоль по умолчанию cp1251 — переключаем stdout на UTF-8,
# иначе любой ASCII-арт типа "→" падает с UnicodeEncodeError.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import psutil

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
COLLECTOR_DIR = ROOT / "collector"
COLLECTOR_PY_DIR = ROOT / "collector_python"
DATA_DIR = ROOT / "data"
RESULTS_MD = HERE / "results.md"
CHART_HTML = HERE / "go_vs_python.html"

# Параметры нагрузки (одинаковые для обоих сборщиков)
WORKERS = 8
BATCH = 50
WINDOW = "5s"
API_DELAY_MS = 5.0   # симуляция реального API


def is_windows() -> bool:
    return platform.system() == "Windows"


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n


def measure_subprocess(cmd: list[str], cwd: Path, label: str) -> dict:
    """
    Запускает subprocess, замеряет peak RSS и средний CPU%
    через polling psutil. Возвращает dict с метриками.
    """
    print(f"\n→ {label}: {' '.join(cmd)}")
    start_ts = time.perf_counter()

    # ВАЖНО: stdout/stderr → DEVNULL. Если использовать PIPE без активного
    # чтения, активно-логирующий процесс (Go zap) забьёт 64K-буфер pipe и
    # заблокируется навечно при следующем log.Info — classic deadlock.
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ps = psutil.Process(proc.pid)

    peak_rss_mb = 0.0
    cpu_samples: list[float] = []

    # Прайминг psutil cpu_percent (первый вызов всегда возвращает 0.0)
    try:
        ps.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    while proc.poll() is None:
        try:
            rss = ps.memory_info().rss / 1024 / 1024
            peak_rss_mb = max(peak_rss_mb, rss)
            cpu = ps.cpu_percent(interval=0.2)
            if cpu > 0:
                cpu_samples.append(cpu)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break

    proc.wait(timeout=120)
    elapsed = time.perf_counter() - start_ts
    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0

    return {
        "label": label,
        "elapsed_s": round(elapsed, 3),
        "peak_rss_mb": round(peak_rss_mb, 1),
        "avg_cpu_pct": round(avg_cpu, 1),
        "exit_code": proc.returncode,
    }


def render_markdown(rows: list[dict]) -> str:
    lines = [
        "# Бенчмарк: Go vs Python (сборщик)",
        "",
        f"Параметры: workers={WORKERS}, batch={BATCH}, window={WINDOW}, "
        f"api_delay_ms={API_DELAY_MS}",
        "",
        "| Метрика              | Go       | Python (asyncio) |",
        "|----------------------|---------:|-----------------:|",
    ]
    by_label = {r["label"]: r for r in rows}
    g = by_label.get("Go", {})
    p = by_label.get("Python", {})

    def fmt(d: dict, key: str) -> str:
        return str(d.get(key, "—"))

    lines.append(f"| Записей/сек          | {fmt(g, 'rps')} | {fmt(p, 'rps')} |")
    lines.append(f"| Время сбора (сек)    | {fmt(g, 'elapsed_s')} | {fmt(p, 'elapsed_s')} |")
    lines.append(f"| Записей всего        | {fmt(g, 'records')} | {fmt(p, 'records')} |")
    lines.append(f"| Пик памяти (MB)      | {fmt(g, 'peak_rss_mb')} | {fmt(p, 'peak_rss_mb')} |")
    lines.append(f"| CPU среднее (%)      | {fmt(g, 'avg_cpu_pct')} | {fmt(p, 'avg_cpu_pct')} |")
    lines.append("")
    return "\n".join(lines)


def render_chart(rows: list[dict]) -> None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        print("  (plotly не установлен — пропускаем график)")
        return

    by_label = {r["label"]: r for r in rows}
    metrics = [
        ("Записей/сек", "rps"),
        ("Время сбора (сек)", "elapsed_s"),
        ("Пик памяти (MB)", "peak_rss_mb"),
        ("CPU среднее (%)", "avg_cpu_pct"),
    ]

    fig = go.Figure()
    for label, color in [("Go", "#00ADD8"), ("Python", "#FFD43B")]:
        d = by_label.get(label, {})
        fig.add_trace(go.Bar(
            name=label,
            x=[m[0] for m in metrics],
            y=[d.get(m[1], 0) for m in metrics],
            marker_color=color,
        ))

    fig.update_layout(
        title="Go vs Python: Demographics Collector",
        barmode="group",
        template="plotly_white",
        height=500,
    )
    CHART_HTML.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(CHART_HTML)
    print(f"  ✓ график: {CHART_HTML}")


def update_results_md(new_section: str) -> None:
    """Безопасно мерджит секцию Go vs Python в results.md, не ломая arrow_vs_json."""
    marker = "# Бенчмарк: Go vs Python"
    existing = RESULTS_MD.read_text(encoding="utf-8") if RESULTS_MD.exists() else ""
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip()
    if existing:
        merged = existing + "\n\n---\n\n" + new_section
    else:
        merged = new_section
    RESULTS_MD.write_text(merged, encoding="utf-8")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    go_output = DATA_DIR / "raw_go_bench.ndjson"
    py_output = DATA_DIR / "raw_python_bench.ndjson"
    for p in (go_output, py_output):
        p.unlink(missing_ok=True)

    # ── Go ── Сначала компилируем, чтобы НЕ мерять `go run` (вкл. компиляцию)
    go_exe = "go.exe" if is_windows() else "go"
    if shutil.which(go_exe) is None:
        print(f"❌ {go_exe} не найден в PATH")
        sys.exit(1)

    bin_name = "collector_bench.exe" if is_windows() else "collector_bench"
    bin_path = COLLECTOR_DIR / bin_name
    print(f"→ Компилирую Go-бинарь: {bin_path.name}")
    rc = subprocess.run(
        [go_exe, "build", "-o", bin_name, "."],
        cwd=COLLECTOR_DIR,
    ).returncode
    if rc != 0:
        print("❌ go build failed")
        sys.exit(1)

    go_cmd = [
        str(bin_path),
        "--no-etcd",
        f"--output={go_output.as_posix()}",
        "--regions-file=regions.json",
        f"--window={WINDOW}",
        f"--batch={BATCH}",
        f"--workers={WORKERS}",
    ]
    go_metrics = measure_subprocess(go_cmd, COLLECTOR_DIR, label="Go")
    go_metrics["records"] = count_lines(go_output)
    go_metrics["rps"] = round(go_metrics["records"] / max(go_metrics["elapsed_s"], 0.001), 1)

    # ── Python ──
    py_exe = sys.executable
    py_cmd = [
        py_exe, "main.py",
        f"--regions-file={(COLLECTOR_DIR / 'regions.json').as_posix()}",
        f"--output={py_output.as_posix()}",
        f"--workers={WORKERS}",
        f"--batch={BATCH}",
        f"--window={WINDOW}",
        f"--api-delay-ms={API_DELAY_MS}",
    ]
    py_metrics = measure_subprocess(py_cmd, COLLECTOR_PY_DIR, label="Python")
    py_metrics["records"] = count_lines(py_output)
    py_metrics["rps"] = round(py_metrics["records"] / max(py_metrics["elapsed_s"], 0.001), 1)

    # ── Отчёт ──
    rows = [go_metrics, py_metrics]
    md = render_markdown(rows)
    print("\n" + md)
    update_results_md(md)
    print(f"\n✓ Результаты сохранены в {RESULTS_MD}")

    render_chart(rows)

    # cleanup временного бинаря
    bin_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
