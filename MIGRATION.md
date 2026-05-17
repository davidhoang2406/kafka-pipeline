# Migration Strategy: Monorepo → Multi-Repo Data Platform

## Context

This repo (`kafka-pipeline`) was built phase-by-phase as a learning project. It now covers the full data stack — ingestion, stream processing, batch processing, and orchestration — in a single repository. The goal of this migration is to split it into focused, independently deployable services as the foundation of a proper data platform.

The guiding principle: **get the data right first.** Infrastructure separation is secondary. We migrate incrementally, keeping everything running at each step.

---

## Target Repository Layout

```
data-platform/
├── platform-infra/          # Docker Compose, infrastructure-only
├── market-data-models/      # Shared schemas, Avro specs, Kafka topic contracts
├── market-data-ingestion/   # Producers + storage consumer (Kafka → MinIO)
├── market-stream-analysis/  # Flink price alert job
├── market-batch-analysis/   # Spark jobs + Dagster orchestration
└── kafka-pipeline/          # This repo — kept as reference, archived when done
```

---

## What Goes Where

### `platform-infra`
Infrastructure only. No application code.

```
docker/
  docker-compose.yml     # Kafka, MinIO, Flink, Spark, Dagster, Jupyter
  flink.Dockerfile
  spark.Dockerfile
  dagster.Dockerfile
  jupyter.Dockerfile
config/
  stocks.json            # Symbol list + poll interval
  crypto.json            # Binance pairs
  alerts.json            # Price alert threshold rules
Makefile                 # topics-create, minio-init, service up/down
```

Every other repo points at this compose file to spin up the platform locally. In production, each service deploys independently and connects to shared Kafka and MinIO endpoints via environment variables.

---

### `market-data-models`
The shared contract between all services. Nothing here runs — it is imported as a package.

```
market_data_models/
  schemas.py             # Avro schema definitions
  message.py             # PriceMessage dataclass
  topics.py              # Kafka topic name constants
  coerce.py              # Type coercion utilities (coerce_float, coerce_int, to_ts)
pyproject.toml
```

This package is installed as a dependency in ingestion, stream, and batch repos. For now, install directly from GitHub:

```bash
pip install git+https://github.com/<org>/market-data-models.git
```

When the API stabilises, publish to PyPI or a private registry.

**Why a separate repo:** the message schema is the contract between producers and consumers. If it lives inside ingestion, the stream and batch repos have no clean way to depend on it without pulling in unrelated code.

---

### `market-data-ingestion`
Owns the "raw data in" path: polling market prices and landing them in Kafka, then draining Kafka into MinIO.

```
producers/
  base_producer.py
  stock_price_producer.py
  crypto_price_producer.py
  utils.py               # load_json_config, validate_rules, evaluate_rules
consumers/
  base_consumer.py
  storage_consumer.py    # Kafka → MinIO (Avro), with DLQ
db/
  init_minio.py
  flush_minio.py
model/
  minio_store.py         # MinIO read/write abstraction
main.py                  # CLI entry points: stock-price-producer, crypto-price-producer, storage-consumer
Makefile
requirements.txt
tests/
```

**Depends on:** `market-data-models`

---

### `market-stream-analysis`
Owns real-time processing. Currently just the Flink price alert job; expands as new stream jobs are added.

```
analysis/
  stream/
    price_alert_job.py
config/
  alerts.json            # symlinked or copied from platform-infra
main.py
Makefile
requirements.txt
tests/
```

**Depends on:** `market-data-models`

---

### `market-batch-analysis`
Owns batch processing and orchestration. Spark jobs transform raw snapshots into OHLCV bars and technical indicators; Dagster schedules and monitors them.

```
analysis/
  batch/
    ohlcv_daily_ingest.py
    technical_job.py
    digest.py
    screener.py
model/
  spark.py               # SparkFactory context manager
  minio_store.py         # copied or shared from ingestion
dagster/
  dagster_project/       # Partitions, assets, resources, schedules
  dagster.yaml
  workspace.yaml
main.py
Makefile
requirements.txt
tests/
```

**Depends on:** `market-data-models`

---

## Shared Code Problem

Three modules are currently used across all services:

| Module | Used by | Resolution |
|---|---|---|
| `schemas/message.py` | ingestion, stream, batch | Move to `market-data-models` |
| `model/minio_store.py` | ingestion, batch | Copy into each repo for now; extract later if they diverge |
| `producers/utils.py` (coerce, evaluate_rules) | ingestion, stream | Split: coerce → `market-data-models`; alert logic → stream repo |

**Rule:** Do not share code by importing across repos at runtime. Each repo must be self-contained. If two repos need the same logic, copy it or extract it into `market-data-models`.

---

## Migration Phases

### Phase 1 — Extract the shared models (start here)
1. Create `market-data-models` repo
2. Move `schemas/message.py`, `model/schemas.py`, coerce utilities from `producers/utils.py`
3. Publish via `pip install git+https://...`
4. Update imports in this repo to use the package — verify all tests still pass
5. Tag this repo at `v1.0-pre-split` before touching anything else

### Phase 2 — Extract ingestion
1. Create `market-data-ingestion` repo
2. Copy `producers/`, `consumers/`, `db/`, `model/minio_store.py`, relevant `main.py` commands
3. Wire up `market-data-models` as a dependency
4. Smoke-test: run producer → Kafka → storage consumer → MinIO end-to-end
5. Freeze this repo once green

### Phase 3 — Extract stream analysis
1. Create `market-stream-analysis` repo
2. Move `analysis/stream/price_alert_job.py`
3. Wire up `market-data-models`
4. Submit the Flink job against the shared `platform-infra` compose stack
5. Verify alerts fire correctly

### Phase 4 — Extract batch analysis
1. Create `market-batch-analysis` repo
2. Move `analysis/batch/`, `model/spark.py`, the full `dagster/` tree
3. Wire up `market-data-models`
4. Run `make dagster-up`, trigger `ohlcv_daily_bars` partition — verify Spark job completes

### Phase 5 — Extract infrastructure
1. Create `platform-infra` repo
2. Move `docker/`, `config/`, top-level `Makefile` infra targets
3. Each service repo updates its `README` with a pointer to `platform-infra` for local setup
4. Archive `kafka-pipeline`

---

## Decision Log

**Why not a monorepo with packages?**
A single repo with proper Python packages (e.g. using `uv workspaces` or `hatch`) would be simpler to start but harder to enforce service boundaries. Independent repos make it obvious when a dependency is being smuggled across service lines.

**Why keep `minio_store.py` in both ingestion and batch instead of sharing?**
Ingestion writes Avro; batch reads Parquet. The two uses are already diverging. Copying now avoids over-engineering a shared abstraction for a pattern that will likely split further.

**Why start with the models repo, not the infrastructure?**
The schema is the contract. Every other split depends on having a stable, independently versioned definition of what a `PriceMessage` is. Getting infrastructure separation right first would leave the data contracts implicit and make the later splits harder.

**Why keep Dagster in `market-batch-analysis` instead of its own repo?**
Dagster directly submits Spark jobs in this setup. Separating them would require a cross-repo API. Once the platform grows to need an orchestration plane that spans multiple domains, Dagster (or a replacement) earns its own repo.
