# Лабораторная работа №14, вариант 26 — Демографические данные Росстат

**ФИО:** Кипчатов Максим Маратович
**Группа:** 221331
**Репозиторий:** [cakeasaur/LR14_26](https://github.com/cakeasaur/LR14_26)

---

## Описание проекта

Распределённый пайплайн обработки демографических данных по 84 регионам РФ
за 2000–2023 годы. Шесть показателей: `population`, `birth_rate`, `death_rate`,
`natural_growth`, `migration_growth`, `life_expectancy`.

Поскольку Росстат API нестабилен, реализован **встроенный эмулятор** с реалистичными
значениями (нормальное распределение вокруг исторических средних, ковидный провал
2020–2021, реформа материнского капитала 2006).

---

## Стек технологий

| Слой | Технология | Назначение |
|------|------------|------------|
| Сбор данных | **Go 1.23**, etcd, goroutines | Распределённый сборщик с шардированием |
| Координация | **etcd** через `clientv3` | Lease TTL, keepalive, детерминированное шардирование |
| Транспорт | **Apache Arrow Flight RPC** | Стрим RecordBatch'ей по 1000 строк |
| Транспорт | **Apache Kafka** (segmentio/kafka-go) | 3 партиции, ключ = region |
| Валидация | **Rust** (PyO3 0.22) | Быстрая валидация на Bound API |
| Анализ | **Polars** + **DuckDB** | Чтение Parquet, SQL-агрегации |
| Визуализация | **Streamlit** + **Plotly** | Дашборд с автообновлением |
| Деплой | **Docker** + **Kubernetes** + **HPA** | replicas=2, autoscale 1-5 |
| Метрики | **Prometheus** | `/metrics` на :9090 |

---

## Структура проекта

```
demographics-pipeline/
├── collector/               # Go-сборщик
│   ├── main.go              # точка входа, CLI, graceful shutdown
│   ├── coordinator.go       # etcd-координация шардов
│   ├── aggregator.go        # tumbling window
│   ├── arrow_server.go      # Arrow Flight RPC сервер
│   ├── kafka_producer.go    # Kafka-продюсер
│   ├── metrics.go           # Prometheus /metrics
│   ├── regions.json         # 84 региона РФ
│   └── Dockerfile           # многоэтапный билд
├── collector_python/        # asyncio-сборщик для бенчмарка
│   └── main.py
├── validator/               # Rust-крейт через PyO3
│   ├── Cargo.toml
│   ├── pyproject.toml       # для maturin
│   └── src/lib.rs
├── analyzer/                # Python-обработка
│   ├── main.py              # пайплайн валидации + DuckDB
│   ├── arrow_client.py      # Arrow Flight клиент
│   ├── kafka_consumer.py    # Kafka consumer + sliding window
│   └── requirements.txt
├── dashboard/
│   └── app.py               # Streamlit-дашборд
├── benchmark/
│   ├── arrow_vs_json.py     # Arrow Flight vs JSON
│   ├── go_vs_python.py      # Go vs Python (asyncio)
│   └── results.md
├── k8s/
│   ├── etcd.yaml            # StatefulSet etcd
│   ├── deployment.yaml      # Deployment + Service
│   ├── hpa.yaml             # HPA с CPU + custom metric
│   └── servicemonitor.yaml  # для kube-prometheus-stack
├── data/                    # NDJSON, Parquet (в .gitignore)
├── docker-compose.yml       # etcd + Kafka + Zookeeper
└── README.md
```

---

## Реализованные задания

### Задание 1 — Распределённый Go-сборщик с координацией через etcd

- Регистрация `/collectors/{hostname}` с TTL-лизом 15 с + keepalive
- Детерминированное шардирование: `sort hostnames → i % N == myIdx`
- Graceful shutdown по SIGINT/SIGTERM → flush → Deregister
- Горутины по регионам с буферизованным каналом (100), per-goroutine `*rand.Rand`
  (math/rand НЕ thread-safe)

### Задание 2 — Tumbling window агрегация в Go

- Закрытие окна по таймеру (`--window`) ИЛИ батчу (`--batch`)
- Группировка по `(region, indicator)` → `sum/avg/min/max/count/p50`
- Лог: `"Window closed: N records → M aggregated groups, saved Xms"`

### Задание 3 — Apache Arrow Flight RPC

- Go-сервер на порту 8815, схема `(region, federal_district, year, indicator, value, collected_at)`
- DoGet принимает ticket-JSON с фильтрами `{indicator, year_from, year_to, region}`
- Стрим `RecordBatch` по 1000 строк
- Python-клиент: `pl.from_arrow(table)` + замеры `time.perf_counter()`

### Задание 4 — Rust-валидатор через PyO3

- PyO3 0.22 с `abi3-py39` (Python 3.9–3.14)
- `validate_record(dict) -> {valid, errors}`, `validate_batch(list) -> list`
- 6 правил валидации: длина region, диапазоны year/value/indicator
- Скорость: ~430 000 записей/сек

### Задание 5 — Kubernetes + HPA + Prometheus

- **Dockerfile**: многоэтапный, статический бинарь, non-root uid 10001
- **StatefulSet etcd** с volumeClaimTemplates 1Gi
- **Deployment**: replicas=2, emptyDir + `OUTPUT_PATH=/data/raw-$(POD_NAME).ndjson`
  (per-pod файл — не конкурентная запись)
- **HPA**: minReplicas=1, maxReplicas=5, две метрики:
  1. CPU `averageUtilization: 70`
  2. `Pods type` метрика `collector_queue_depth averageValue: 80`
- **InitContainer** ждёт etcd через `nc` — устраняет CrashLoopBackOff
- **Liveness** через `tcpSocket`, **Readiness** через `/health`

### Задание 5б — Бенчмарк Go vs Python (asyncio)

| Метрика | Go | Python (asyncio) |
|---|---:|---:|
| Записей/сек | **58 153** | 2 009 |
| Время сбора (сек) | 0.21 | 6.02 |
| Пик памяти (MB) | **1.9** | 45.6 |
| CPU среднее (%) | 0.0¹ | 64.4 |

¹ Go завершился за 0.21 с — `psutil.cpu_percent(interval=0.2)` не успел сэмплировать.

**Go даёт ~30× записей/сек при ~24× меньшей памяти.**

### Задание 5в — Apache Kafka

- **Go-продюсер** (segmentio/kafka-go): топик `demographics-raw`, 3 партиции,
  Hash-балансер (ключ = region → стабильная партиция)
- **Python-консьюмер** (confluent-kafka): sliding window 5 мин, тик каждые 30 с
  → топ-5 регионов по `birth_rate`, flush 1000 записей → Parquet
- Tick-flush pending буфера; fallback в emergency NDJSON при сбое Parquet

### Задание 6 — Streamlit-дашборд

- Sidebar: автодетект последнего Parquet/NDJSON по mtime, фильтры,
  toggle автообновления, кнопка «Обновить сейчас»
- 3 KPI: население РФ, ср. продолжительность жизни, регионов с убылью
- 3 Tab'а:
  - **📈 Динамика** — линии по округам, аннотации COVID 2020-2021 + Перепись 2021
  - **🗺️ По регионам** — горизонтальный bar по выбранному году
  - **📋 Таблица** — top-20 с условным форматированием (red→yellow→green)
- Кэш `@st.cache_data(ttl=30)` с ключом `(path, mtime)` — автоинвалидация
- Автообновление через `<meta refresh>` — UI остаётся отзывчивым

---

## Установка и запуск

### Требования

- Go 1.23+
- Python 3.9–3.14
- Rust 1.70+ (для валидатора)
- Docker + docker-compose
- Опционально: minikube + kubectl

### Установка зависимостей

```bash
# Python
pip install -r analyzer/requirements.txt

# Rust (валидатор)
cd validator
maturin develop --release
cd ..

# Go
cd collector
go mod download
cd ..
```

### Запуск инфраструктуры

```bash
docker-compose up -d etcd kafka zookeeper
```

### Демо-сценарии

#### 1. Локальный сбор в NDJSON

```bash
cd collector
go run . --etcd=localhost:2379 --output=../data/raw.ndjson
```

#### 2. Standalone-режим без etcd

```bash
cd collector
go run . --no-etcd --output=../data/raw.ndjson
```

#### 3. Распределённый сбор (2 экземпляра)

```bash
# Терминал 1
go run . --etcd=localhost:2379 --output=../data/raw1.ndjson

# Терминал 2 (автоматически возьмёт другой шард регионов)
go run . --etcd=localhost:2379 --output=../data/raw2.ndjson
```

#### 4. Arrow Flight RPC

```bash
# Сервер
cd collector
go run . --flight-only --flight-source=../data/raw.ndjson

# Клиент
cd ../analyzer
python arrow_client.py --indicator=birth_rate --year-from=2010
```

#### 5. Валидация через Rust

```bash
cd validator && maturin develop --release && cd ..
python analyzer/main.py --input=data/raw.ndjson --output=data/clean.parquet
```

#### 6. Kafka-режим

```bash
# Терминал 1: Go-продюсер
cd collector
go run . --no-etcd --mode=kafka --kafka-brokers=localhost:9092 \
  --output=/tmp/unused.ndjson

# Терминал 2: Python-консьюмер
python -u analyzer/kafka_consumer.py
```

#### 7. Streamlit-дашборд

```bash
python -m streamlit run dashboard/app.py
# → http://localhost:8501
```

#### 8. Бенчмарки

```bash
# Arrow vs JSON
python benchmark/arrow_vs_json.py

# Go vs Python
python benchmark/go_vs_python.py
```

#### 9. Kubernetes (minikube)

```bash
minikube start
minikube addons enable metrics-server
eval $(minikube docker-env)         # PowerShell: minikube -p minikube docker-env | iex
docker build -t demographics-collector:latest collector/
kubectl apply -f k8s/etcd.yaml
kubectl rollout status statefulset/etcd
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/hpa.yaml
kubectl get hpa -w
```

---

## История разработки

Проект разработан с практикой **аудита после каждого задания**.
В ходе аудитов выявлены и исправлены 20+ багов, включая критичные:

- **Задание 1+2**: panic при двойном `close(rawCh)`, data race на `*rand.Rand`,
  потенциальный deadlock при cancel
- **Задание 4**: пустые `region=""`/`indicator=""` проходили валидацию,
  SQL-инъекция через `parquet_path` в DuckDB f-string
- **Задание 5**: `emptyDir` + non-root user → `EACCES`, livenessProbe через
  `/health` создавал каскадные рестарты при сбое etcd
- **Задание 5б**: `subprocess.PIPE` без чтения → 222-секундный pipe deadlock
- **Задание 6**: `Styler.background_gradient(cmap="RdYlGn")` требует matplotlib,
  Tab "Таблица" падал бы с ImportError

Все коммиты помечены префиксами `feat:` / `fix:` / `chore:`.

---

## Лицензия

Учебный проект, использование без ограничений.
