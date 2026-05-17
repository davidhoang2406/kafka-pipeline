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
├── market-jobs/             # All jobs: Flink stream + Spark batch + Dagster orchestration
├── market-notebooks/        # Jupyter notebooks for exploration and ad-hoc analysis
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

### `market-jobs`
Owns all data processing jobs — both stream and batch — and the Dagster orchestration layer that ties them together. Stream and batch jobs share the same MinIO/Kafka dependencies and evolve together as new jobs are added, so they live in one repo rather than being split by processing type.

```
jobs/
  stream/
    price_alert_job.py   # Flink: real-time price threshold alerts
  batch/
    ohlcv_daily_ingest.py  # Spark: raw snapshots → OHLCV Parquet
    technical_job.py       # Spark: SMA/RSI/MACD/Bollinger Bands
    digest.py              # Spark: daily gainers/losers/volume summary
    screener.py            # Spark: fundamental screener (P/E, D/E, EPS)
model/
  spark.py               # SparkFactory context manager
  minio_store.py         # MinIO read/write (Parquet-focused)
dagster/
  dagster_project/       # Partitions, assets, resources, schedules
  dagster.yaml
  workspace.yaml
config/
  alerts.json            # Price alert threshold rules
main.py                  # CLI entry points for all jobs
Makefile
requirements.txt
tests/
```

**Depends on:** `market-data-models`

**Why merged:** stream and batch jobs share `minio_store.py`, `SparkFactory`, the same Dagster orchestration layer, and the same deployment environment (Flink + Spark + MinIO). Splitting them would mean duplicating shared model code and coordinating two repos for what is effectively one pipeline. As the platform grows, new jobs of either type simply land in `jobs/stream/` or `jobs/batch/`.

---

### `market-notebooks`
Jupyter notebooks for exploration, ad-hoc analysis, and visualisation. This repo is intentionally separate from production code — notebooks are not tested or deployed; they read from MinIO and Kafka but never write to production sinks.

```
notebooks/
  exploration/           # One-off investigation notebooks
  reporting/             # Recurring analysis templates (OHLCV review, screener output)
  onboarding/            # Walkthrough notebooks for new contributors
docker/
  jupyter.Dockerfile     # Inherited from platform-infra; kept here for notebook-specific deps
requirements.txt         # Notebook-only deps (matplotlib, plotly, pandas, etc.)
README.md
```

**Depends on:** `market-data-models` (for schema-aware reading), `platform-infra` (for the running MinIO + Kafka stack)

**Rules for this repo:**
- No notebook output is committed (strip cell outputs before push — enforce with `nbstripout` pre-commit hook)
- Notebooks read data; they never write back to `market-data` or `market-analysis` buckets
- Production-ready logic extracted from a notebook goes into `market-jobs`, not back into this repo

---

## Shared Code Problem

Three modules are currently used across all services:

| Module | Used by | Resolution |
|---|---|---|
| `schemas/message.py` | ingestion, jobs, notebooks | Move to `market-data-models` |
| `model/minio_store.py` | ingestion, jobs | Copy into each repo; the two uses are diverging (Avro write vs Parquet read) |
| `producers/utils.py` (coerce, evaluate_rules) | ingestion, jobs | Split: coerce → `market-data-models`; alert logic stays in `market-jobs` |
| `docker/jupyter.Dockerfile` | notebooks | Move to `market-notebooks`; keep a reference copy in `platform-infra` |

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

### Phase 3 — Extract jobs
1. Create `market-jobs` repo
2. Move `analysis/stream/`, `analysis/batch/`, `model/spark.py`, `model/minio_store.py`, the full `dagster/` tree, and `config/alerts.json`
3. Wire up `market-data-models`
4. Verify stream: submit Flink price alert job, confirm alerts fire
5. Verify batch: run `make dagster-up`, trigger `ohlcv_daily_bars` partition, confirm Parquet output in MinIO

### Phase 4 — Extract notebooks
1. Create `market-notebooks` repo
2. Move `notebooks/` contents and `docker/jupyter.Dockerfile`
3. Install `nbstripout` pre-commit hook to strip cell outputs on commit
4. Verify: start Jupyter container, open an existing notebook, confirm MinIO and Kafka are reachable via `platform-infra`

### Phase 5 — Extract infrastructure
1. Create `platform-infra` repo
2. Move `docker/` (excluding `jupyter.Dockerfile`, which now lives in `market-notebooks`), `config/stocks.json`, `config/crypto.json`, top-level `Makefile` infra targets
3. Each service repo updates its `README` with a pointer to `platform-infra` for local setup
4. Archive `kafka-pipeline`

---

## Data Contracts and SLAs

Splitting repos makes implicit contracts explicit. Each service boundary must define what it promises to produce and what it requires to consume.

### Kafka topic contracts

| Topic | Producer | Consumer(s) | Schema | SLA |
|---|---|---|---|---|
| `stock.price.realtime` | market-data-ingestion | market-jobs (Flink), market-data-ingestion (storage) | `PriceMessage` v1 (Avro) | < 60 s lag under normal load |
| `crypto.price.realtime` | market-data-ingestion | market-jobs (Flink), market-data-ingestion (storage) | `PriceMessage` v1 (Avro) | < 90 s lag |

Consumers must tolerate unknown fields (forward compatibility). Producers must never remove or rename existing fields without a major version bump (backward compatibility).

### MinIO partition contracts

| Path prefix | Written by | Read by | Format | Freshness SLA |
|---|---|---|---|---|
| `price.snapshot/asset_class=*/...` | market-data-ingestion | market-jobs (Spark) | Avro | Daily, by 15:30 HCM |
| `ohlcv.bar/asset_class=*/...` | market-jobs (Spark) | market-jobs (technical Spark job), market-notebooks | Parquet | Daily, by 17:00 HCM |

These paths are the hand-off points between repos. They must not change without coordinating across all repos that read them.

---

## Schema Evolution Strategy

`market-data-models` will be versioned with [semver](https://semver.org/). The rules:

- **Patch** (`0.1.x`) — bug fixes, docstring changes, no schema changes
- **Minor** (`0.x.0`) — additive changes: new optional fields, new topic constants
- **Major** (`x.0.0`) — breaking changes: renamed fields, removed fields, type changes

Each service repo pins to a minor version (`market-data-models>=0.1,<0.2`) to get patches automatically but not breaking changes.

**Adding a new field to `PriceMessage`:**
1. Add the field as optional with a default in `market-data-models` (minor bump)
2. Release a new version
3. Update each service repo independently — ingestion first (producer), then consumers
4. Old consumers reading messages without the new field get the default — no downtime

**Renaming or removing a field:**
1. Deprecate in a minor release (keep the old name, add the new one)
2. Give all services a migration window (at least one sprint)
3. Remove the old name in the next major release
4. Update all service repos before cutting the major release

---

## Data Quality Across Service Boundaries

Quality checks must live in the service that owns the data, not in the service that consumes it.

| Check | Where it lives | Current state |
|---|---|---|
| Non-null price, volume ≥ 0 | market-data-ingestion (storage consumer) | Implemented via DLQ |
| OHLCV bar validity (high ≥ low, etc.) | market-batch-analysis (ohlcv_daily_ingest) | Implemented — invalid bars are dropped and logged |
| Partition freshness | market-batch-analysis (Dagster observable asset) | Implemented via `price_snapshots` asset |
| Technical indicator NaN rate | market-batch-analysis (technical_job) | Not yet implemented — add before migration |

Before migrating any service, ensure its quality checks are in place. A bug found in a monorepo is fixed in one PR. A bug found after splitting requires coordinating across repos.

---

## Data Lineage

Once split, tracing a bad technical indicator back to a raw price snapshot crosses three repos. Document the lineage now while it is still easy to see:

```
vnstock / Binance API
  └── stock-price-producer / crypto-price-producer   [market-data-ingestion]
        └── stock.price.realtime / crypto.price.realtime   [Kafka]
              ├── price_alert_job (Flink)                  [market-jobs]
              │     └── alert output (stdout / future sink)
              └── storage-consumer                         [market-data-ingestion]
                    └── price.snapshot/...                 [MinIO — Avro]
                          └── ohlcv_daily_ingest (Spark)   [market-jobs]
                                └── ohlcv.bar/...          [MinIO — Parquet]
                                      ├── technical_job (Spark)  [market-jobs]
                                      │     └── report output
                                      └── Jupyter notebooks      [market-notebooks]
```

When the platform grows, consider adding [OpenLineage](https://openlineage.io/) markers to the Spark jobs and Dagster assets. Both support it natively and it gives lineage visibility across repos without manual documentation.

---

## Cross-Repo CI/CD

Each repo gets its own GitHub Actions pipeline. The integration point is `market-data-models`.

```
market-data-models
  └── on push: unit tests → publish to GitHub Packages (or PyPI)

market-data-ingestion
  └── on push: unit tests
  └── on release: integration test against platform-infra compose stack

market-jobs
  └── on push: unit tests (dagster/tests/, job unit tests)
  └── on release:
        stream — submit Flink job to test cluster, verify alert fires
        batch  — run ohlcv_daily_ingest on a fixture date, assert Parquet output in MinIO

market-notebooks
  └── on push: nbstripout check (fail if any notebook has committed cell output)
  └── no release pipeline — notebooks are not deployed
```

**Dependency update bot:** when `market-data-models` publishes a new version, open an automated PR in each downstream repo to bump the pinned version. Review the diff before merging — this is where schema changes surface.

---

## Environment Management

Each repo supports three environments via environment variables. No code changes required between environments.

| Variable | Local (dev) | Staging | Production |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka-staging:9092` | `kafka-prod:9092` |
| `MINIO_ENDPOINT` | `http://localhost:9000` | `http://minio-staging:9000` | `https://s3.amazonaws.com` |
| `MINIO_BUCKET` | `market-data` | `market-data-staging` | `market-data-prod` |

Secrets (access keys, API tokens) are never committed. Use `.env` for local dev, GitHub Secrets for CI, and a secrets manager (Vault, AWS SSM) for production.

---

## Decision Log

**Why not a monorepo with packages?**
A single repo with proper Python packages (e.g. using `uv workspaces` or `hatch`) would be simpler to start but harder to enforce service boundaries. Independent repos make it obvious when a dependency is being smuggled across service lines.

**Why keep `minio_store.py` in both ingestion and batch instead of sharing?**
Ingestion writes Avro; batch reads Parquet. The two uses are already diverging. Copying now avoids over-engineering a shared abstraction for a pattern that will likely split further.

**Why start with the models repo, not the infrastructure?**
The schema is the contract. Every other split depends on having a stable, independently versioned definition of what a `PriceMessage` is. Getting infrastructure separation right first would leave the data contracts implicit and make the later splits harder.

**Why merge stream and batch into `market-jobs` instead of separate repos?**
Stream (Flink) and batch (Spark) jobs share `minio_store.py`, `SparkFactory`, the same Dagster orchestration layer, and the same deployment environment. Splitting them saves nothing and doubles the coordination cost when adding a new job. The `jobs/stream/` vs `jobs/batch/` directory split inside the repo is enough to keep them distinct.

**Why keep Dagster in `market-jobs` instead of its own repo?**
Dagster directly submits Spark jobs in this setup. Separating them would require a cross-repo API. Once the platform grows to need an orchestration plane that spans multiple domains, Dagster (or a replacement) earns its own repo.

**Why split notebooks into `market-notebooks`?**
Notebooks are exploration artifacts, not production code. They have no tests, no deployment pipeline, and no SLA. Keeping them in the same repo as production jobs creates pressure to treat them like production code (or to ignore quality standards for them). A dedicated repo makes it clear: code that runs in production lives in `market-jobs`; code that only runs on your laptop lives in `market-notebooks`.
