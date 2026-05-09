"""
dashboard/app.py — Streamlit-дашборд "Демография РФ".

Источник данных: последний по времени модификации Parquet-файл в data/
(из analyzer/main.py или kafka_consumer.py). Если нет — fallback на
data/raw.ndjson.

Запуск:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── константы ──────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

INDICATORS = [
    "population", "birth_rate", "death_rate",
    "natural_growth", "migration_growth", "life_expectancy",
]
INDICATOR_LABELS = {
    "population": "Население",
    "birth_rate": "Рождаемость (на 1000)",
    "death_rate": "Смертность (на 1000)",
    "natural_growth": "Естественный прирост (на 1000)",
    "migration_growth": "Миграционный прирост (на 1000)",
    "life_expectancy": "Ожидаемая продолжительность жизни",
}


# ─── загрузка данных ────────────────────────────────────────────────────
def find_latest_dataset() -> Path | None:
    """Ищет самый свежий Parquet/NDJSON в data/."""
    candidates: list[Path] = []
    for ext in ("*.parquet", "*.ndjson"):
        candidates.extend(DATA_DIR.rglob(ext))
    # Игнорируем служебные/бенчмарк-файлы — они мешают автодетекту "свежих данных"
    blocked = ("aggregated", "rejected", "_bench", "smoke")
    candidates = [
        p for p in candidates
        if not any(b in p.name for b in blocked)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


@st.cache_data(ttl=30, show_spinner=False)
def load_dataset(path_str: str, mtime: float) -> pl.DataFrame:
    """Кэшируется по (path, mtime), чтобы пересчитывать при изменении файла."""
    p = Path(path_str)
    if p.suffix == ".parquet":
        df = pl.read_parquet(p)
    else:
        df = pl.read_ndjson(p)
    if "indicator" in df.columns:
        df = df.filter(~pl.col("indicator").str.starts_with("_agg_"))
    return df


# ─── KPI ────────────────────────────────────────────────────────────────
def kpi_population(df: pl.DataFrame) -> str:
    pop = df.filter(pl.col("indicator") == "population")
    if pop.height == 0:
        return "—"
    last_year = pop["year"].max()
    total = (
        pop.filter(pl.col("year") == last_year)["value"].sum()
    )
    return f"{total / 1_000_000:.1f} млн (за {last_year})"


def kpi_life_expectancy(df: pl.DataFrame) -> str:
    le = df.filter(pl.col("indicator") == "life_expectancy")
    if le.height == 0:
        return "—"
    last_year = le["year"].max()
    avg = le.filter(pl.col("year") == last_year)["value"].mean()
    return f"{avg:.1f} лет (за {last_year})"


def kpi_negative_natural_growth(df: pl.DataFrame) -> str:
    ng = df.filter(pl.col("indicator") == "natural_growth")
    if ng.height == 0:
        return "—"
    last_year = ng["year"].max()
    n = ng.filter((pl.col("year") == last_year) & (pl.col("value") < 0)).height
    return f"{n} регионов"


# ─── графики ────────────────────────────────────────────────────────────
def plot_dynamics(df: pl.DataFrame, indicator: str) -> go.Figure:
    """Линия = федеральный округ, X = год, Y = среднее значение по округу."""
    sub = df.filter(pl.col("indicator") == indicator)
    if sub.height == 0:
        return go.Figure()
    agg = (
        sub.group_by(["federal_district", "year"])
        .agg(pl.col("value").mean().alias("avg"))
        .sort("year")
    ).to_pandas()
    fig = px.line(
        agg, x="year", y="avg", color="federal_district",
        markers=True,
        title=f"Динамика: {INDICATOR_LABELS.get(indicator, indicator)} по округам",
    )
    fig.update_layout(template="plotly_white", height=500)
    # Аннотации: ковид-провал и перепись
    fig.add_vrect(
        x0=2020, x1=2021,
        fillcolor="LightSalmon", opacity=0.25, line_width=0,
        annotation_text="COVID-19", annotation_position="top left",
    )
    fig.add_vline(
        x=2021, line_width=1, line_dash="dash", line_color="gray",
        annotation_text="Перепись 2021", annotation_position="top right",
    )
    return fig


def plot_choropleth(df: pl.DataFrame, indicator: str, year: int) -> go.Figure:
    """Простая bar-альтернатива choropleth: для геодиаграммы РФ
    нужен внешний geojson региональных границ. Чтобы не тащить
    тяжёлую зависимость, отрисовываем горизонтальный bar."""
    sub = df.filter(
        (pl.col("indicator") == indicator) & (pl.col("year") == year)
    ).sort("value", descending=True)
    if sub.height == 0:
        return go.Figure()
    pdf = sub.to_pandas()
    fig = px.bar(
        pdf,
        x="value", y="region", orientation="h",
        color="federal_district",
        title=f"{INDICATOR_LABELS.get(indicator, indicator)} по регионам ({year})",
        height=max(500, 18 * len(pdf)),
    )
    fig.update_layout(
        template="plotly_white",
        yaxis={"categoryorder": "total ascending"},
        showlegend=True,
    )
    return fig


def _gradient_color(v: float, vmin: float, vmax: float) -> str:
    """Линейная интерполяция red→yellow→green по value.
    Не зависит от matplotlib (Styler.background_gradient(cmap=...) требует его).
    Возвращает CSS color: rgb(r, g, b) для inline-style.
    """
    if vmax == vmin:
        return "background-color: rgb(255, 235, 130)"  # ровно середина → жёлтый
    t = (v - vmin) / (vmax - vmin)  # 0..1
    if t < 0.5:
        # красный → жёлтый
        r, g, b = 235, int(80 + (235 - 80) * (t * 2)), 80
    else:
        # жёлтый → зелёный
        r, g, b = int(235 - (235 - 80) * ((t - 0.5) * 2)), 200, int(80 + (140 - 80) * ((t - 0.5) * 2))
    return f"background-color: rgb({r}, {g}, {b})"


def conditional_format_table(df: pl.DataFrame, indicator: str, year: int):
    """Топ-20 регионов с подсветкой относительно медианы."""
    sub = df.filter(
        (pl.col("indicator") == indicator) & (pl.col("year") == year)
    )
    if sub.height == 0:
        st.info("Нет данных для выбранного показателя/года")
        return
    pdf = sub.sort("value", descending=True).head(20).to_pandas()
    median = float(pdf["value"].median())
    vmin, vmax = float(pdf["value"].min()), float(pdf["value"].max())

    def style_value(v: float) -> str:
        return _gradient_color(v, vmin, vmax)

    styled = (
        pdf[["region", "federal_district", "value"]]
        .style
        .map(style_value, subset=["value"])  # element-wise styling, без matplotlib
        .format({"value": "{:.2f}"})
    )
    st.caption(f"Медиана = {median:.2f}; зелёный — выше, красный — ниже")
    st.dataframe(styled, use_container_width=True, height=600)


# ─── main ───────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="Демография РФ",
        layout="wide",
        page_icon="🇷🇺",
    )
    st.title("Демография РФ — мониторинг")

    # ── sidebar ──
    with st.sidebar:
        st.header("Фильтры")
        latest = find_latest_dataset()
        if latest is None:
            st.error("Нет данных. Запусти сборщик: `go run collector/...`")
            st.stop()

        st.caption(f"Источник: `{latest.relative_to(ROOT)}`")
        df = load_dataset(str(latest), latest.stat().st_mtime)
        st.caption(f"Записей: {df.height:,}")

        if df.height == 0 or "indicator" not in df.columns:
            st.error("Файл пуст или невалиден.")
            st.stop()

        indicator = st.selectbox(
            "Показатель",
            options=INDICATORS,
            format_func=lambda x: INDICATOR_LABELS.get(x, x),
        )

        years_in_df = df["year"].unique().sort().to_list()
        if not years_in_df:
            st.error("Нет данных по годам.")
            st.stop()
        y_min, y_max = int(years_in_df[0]), int(years_in_df[-1])
        year_range = st.slider(
            "Диапазон лет",
            min_value=y_min, max_value=y_max,
            value=(y_min, y_max),
        )

        all_districts = sorted(df["federal_district"].unique().to_list())
        districts = st.multiselect(
            "Федеральные округа",
            options=all_districts,
            default=all_districts,
        )

        autorefresh = st.toggle("Автообновление каждые 30 сек", value=False)
        if st.button("🔄 Обновить данные сейчас"):
            st.cache_data.clear()
            st.rerun()

    # ── фильтрация ──
    filtered = df.filter(
        (pl.col("year") >= year_range[0]) &
        (pl.col("year") <= year_range[1]) &
        (pl.col("federal_district").is_in(districts))
    )

    # ── KPI ──
    pop_label = (
        "Население РФ"
        if len(districts) == len(all_districts)
        else f"Население ({len(districts)} округов)"
    )
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(pop_label, kpi_population(filtered))
    with c2:
        st.metric("Ср. ожидаемая продолжительность", kpi_life_expectancy(filtered))
    with c3:
        st.metric("Регионов с убылью населения", kpi_negative_natural_growth(filtered))

    # ── вкладки ──
    tab1, tab2, tab3 = st.tabs(["📈 Динамика", "🗺️ По регионам", "📋 Таблица"])

    with tab1:
        st.plotly_chart(plot_dynamics(filtered, indicator), use_container_width=True)

    with tab2:
        last_year = year_range[1] if filtered.height == 0 else int(filtered["year"].max())
        chart_year = st.slider(
            "Год для среза",
            min_value=year_range[0], max_value=year_range[1],
            value=last_year,
            key="map_year",
        )
        st.plotly_chart(
            plot_choropleth(filtered, indicator, chart_year),
            use_container_width=True,
        )

    with tab3:
        last_year = year_range[1] if filtered.height == 0 else int(filtered["year"].max())
        table_year = st.slider(
            "Год для таблицы",
            min_value=year_range[0], max_value=year_range[1],
            value=last_year,
            key="table_year",
        )
        conditional_format_table(filtered, indicator, table_year)

    # ── автообновление ──
    # Используем HTML meta refresh вместо time.sleep(30) — sleep блокировал
    # бы main-thread Streamlit, не давая выключить toggle до истечения 30с.
    # Meta refresh выполняется браузером, страница остаётся отзывчивой.
    if autorefresh:
        st.markdown(
            '<meta http-equiv="refresh" content="30">',
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
