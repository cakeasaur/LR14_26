"""
Тесты для dashboard/app.py — без запущенного Streamlit.
Streamlit-функции (st.metric, st.dataframe, st.tabs) не вызываем —
тестируем только чистую логику: загрузка, фильтры, графики, KPI, _gradient_color.
"""
from __future__ import annotations

import time
from pathlib import Path

import polars as pl
import pytest


# ─── _gradient_color ────────────────────────────────────────────────────


def test_gradient_color_min_returns_red(dashboard_app_mod):
    fn = dashboard_app_mod._gradient_color
    css = fn(v=0.0, vmin=0.0, vmax=10.0)
    assert "background-color: rgb(" in css
    # При t=0 цвет ярко-красный — R высокий, G/B низкий
    rgb = css.split("rgb(")[1].rstrip(")")
    r, g, b = [int(x) for x in rgb.split(",")]
    assert r > 200 and g < 100 and b < 100


def test_gradient_color_max_returns_green(dashboard_app_mod):
    fn = dashboard_app_mod._gradient_color
    css = fn(v=10.0, vmin=0.0, vmax=10.0)
    rgb = css.split("rgb(")[1].rstrip(")")
    r, g, b = [int(x) for x in rgb.split(",")]
    # При t=1 — зелёный преобладает
    assert g > 150 and r < 150


def test_gradient_color_middle_returns_yellow(dashboard_app_mod):
    fn = dashboard_app_mod._gradient_color
    css = fn(v=5.0, vmin=0.0, vmax=10.0)
    rgb = css.split("rgb(")[1].rstrip(")")
    r, g, b = [int(x) for x in rgb.split(",")]
    # При t=0.5 — переход красный→жёлтый, R и G близки и высоки
    assert r > 150 and g > 150


def test_gradient_color_vmin_equals_vmax(dashboard_app_mod):
    """Когда все значения равны — должен возвращать средний цвет, не падать."""
    fn = dashboard_app_mod._gradient_color
    css = fn(v=5.0, vmin=5.0, vmax=5.0)
    assert "background-color: rgb(" in css


# ─── find_latest_dataset ────────────────────────────────────────────────


def test_find_latest_dataset_picks_newest(dashboard_app_mod, tmp_path, monkeypatch):
    """Подменяем DATA_DIR на tmp; создаём 2 файла, проверяем выбор по mtime."""
    monkeypatch.setattr(dashboard_app_mod, "DATA_DIR", tmp_path)
    older = tmp_path / "raw_old.ndjson"
    newer = tmp_path / "raw_new.parquet"
    older.write_text('{"a": 1}\n')
    time.sleep(0.05)
    pl.DataFrame({"a": [1]}).write_parquet(newer)
    result = dashboard_app_mod.find_latest_dataset()
    assert result == newer


def test_find_latest_dataset_skips_aggregated(dashboard_app_mod, tmp_path, monkeypatch):
    """Не должен выбирать aggregated.ndjson, rejected.ndjson, *_bench."""
    monkeypatch.setattr(dashboard_app_mod, "DATA_DIR", tmp_path)
    (tmp_path / "aggregated.ndjson").write_text('{"a": 1}\n')
    (tmp_path / "rejected.ndjson").write_text('{"a": 1}\n')
    (tmp_path / "raw_go_bench.ndjson").write_text('{"a": 1}\n')
    time.sleep(0.05)
    real = tmp_path / "raw.ndjson"
    real.write_text('{"a": 1}\n')
    result = dashboard_app_mod.find_latest_dataset()
    assert result == real


def test_find_latest_dataset_returns_none_when_empty(
    dashboard_app_mod, tmp_path, monkeypatch
):
    monkeypatch.setattr(dashboard_app_mod, "DATA_DIR", tmp_path)
    assert dashboard_app_mod.find_latest_dataset() is None


# ─── load_dataset ───────────────────────────────────────────────────────


def test_load_dataset_parquet(dashboard_app_mod, parquet_file):
    df = dashboard_app_mod.load_dataset(str(parquet_file), parquet_file.stat().st_mtime)
    assert df.height > 0
    assert "indicator" in df.columns


def test_load_dataset_ndjson_filters_agg(dashboard_app_mod, ndjson_file):
    df = dashboard_app_mod.load_dataset(str(ndjson_file), ndjson_file.stat().st_mtime)
    # _agg_ записи должны быть отфильтрованы
    bad = df.filter(pl.col("indicator").str.starts_with("_agg_"))
    assert bad.is_empty()


# ─── KPI ────────────────────────────────────────────────────────────────


def test_kpi_population_returns_total_in_millions(dashboard_app_mod, sample_df):
    fn = dashboard_app_mod.kpi_population
    res = fn(sample_df)
    # 12.5 + 5.4 + 3.9 + 5.7 ≈ 27.5 млн (за 2023)
    assert "млн" in res
    assert "2023" in res


def test_kpi_population_empty_returns_dash(dashboard_app_mod):
    fn = dashboard_app_mod.kpi_population
    empty = pl.DataFrame({
        "region": [], "indicator": [], "year": [], "value": [],
        "federal_district": [],
    }, schema={"region": pl.String, "indicator": pl.String,
                "year": pl.Int64, "value": pl.Float64,
                "federal_district": pl.String})
    assert fn(empty) == "—"


def test_kpi_life_expectancy_format(dashboard_app_mod, sample_df):
    res = dashboard_app_mod.kpi_life_expectancy(sample_df)
    assert "лет" in res
    assert "2023" in res


def test_kpi_negative_natural_growth_count(dashboard_app_mod, sample_df):
    """В наших фикстурах все 4 региона имеют natural_growth < 0 за 2023."""
    res = dashboard_app_mod.kpi_negative_natural_growth(sample_df)
    assert "4" in res  # все 4 региона с убылью
    assert "регион" in res


def test_kpi_no_indicator_data_returns_dash(dashboard_app_mod, sample_df):
    """Если фильтрация убрала весь indicator — возвращаем '—'."""
    df_no_pop = sample_df.filter(pl.col("indicator") != "population")
    assert dashboard_app_mod.kpi_population(df_no_pop) == "—"


# ─── plot_dynamics / plot_choropleth ────────────────────────────────────


def test_plot_dynamics_returns_figure_with_traces(dashboard_app_mod, sample_df):
    fig = dashboard_app_mod.plot_dynamics(sample_df, "birth_rate")
    # plotly Figure
    assert hasattr(fig, "data")
    # Минимум 1 трейс (по линии на округ)
    assert len(fig.data) >= 1


def test_plot_dynamics_empty_indicator(dashboard_app_mod, sample_df):
    """Несуществующий показатель → пустая Figure без падения."""
    fig = dashboard_app_mod.plot_dynamics(sample_df, "nonexistent_indicator")
    assert hasattr(fig, "data")
    assert len(fig.data) == 0


def test_plot_choropleth_returns_figure(dashboard_app_mod, sample_df):
    fig = dashboard_app_mod.plot_choropleth(sample_df, "life_expectancy", year=2023)
    assert hasattr(fig, "data")
    assert len(fig.data) >= 1


def test_plot_choropleth_empty_year(dashboard_app_mod, sample_df):
    fig = dashboard_app_mod.plot_choropleth(sample_df, "life_expectancy", year=1999)
    assert len(fig.data) == 0
