# Kafka Streaming Pipeline

A real-time data streaming pipeline for Vietnamese stock and cryptocurrency market data, built with Apache Kafka, MinIO, Apache Flink, and Apache Spark.

## Overview

Converts pull-based market APIs into a continuous push pipeline — any number of consumers read the same stream independently without hitting upstream APIs more than once.

```
vnstock API  ──► stock_price_producer  ──► stock.price.realtime  ──┐
                                                                     ├──► StorageConsumer ──► market-data (MinIO, Avro)
Crypto API   ──► crypto_price_producer ──► crypto.price.realtime ──┘          │
                                                                               ▼
Kafka topics ──► PriceAlertJob (Flink) ──► alerts            ohlcv_daily_ingest (Spark)
             └──► alert_consumer (Python) ──► alerts                           │
                                                                               ▼
                                                               market-analysis (MinIO, Parquet)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 4.0 (KRaft, no ZooKeeper) |
| Object storage | MinIO (S3-compatible) |
| Stream processing | Apache Flink 2.0 + PyFlink |
| Batch processing | Apache Spark 4.1.1 (standalone Docker cluster) |
| Stock data | vnstock 4.0 (Vietnamese equities, KBS source) |
| Crypto data | CCXT (Binance) |
| Infrastructure | Docker Compose |

## Kafka Topics

| Topic | Partitions | Cadence | Purpose |
|---|---|---|---|
| `stock.price.realtime` | 6 | 30 s | Live HOSE price board snapshots |
| `crypto.price.realtime` | 6 | 5–60 s | Live Binance ticker snapshots |

OHLCV bars are **derived from the stored price snapshots** by the Spark job — they are not fetched from the upstream API again and do not flow through Kafka.

## Quick Start

**Prerequisites:** Docker, Python 3.12, `make`

```bash
git clone https://github.com/davidhoang2406/kafka-pipeline.git
cd kafka-pipeline
python3.12 -m venv .venv && source .venv/bin/activate

make install                  # interactive: select Kafka / MinIO / Flink / Spark
```

`make install` prompts for which services to start, then handles Docker builds, S3A JAR downloads, bucket creation, and topic creation automatically.

## Running the Pipeline

Open a separate terminal for each long-running process:

```bash
# Producers (run continuously)
make run-stock-price-producer    # Vietnamese stocks → Kafka (every 30 s)
make run-crypto-price-producer   # Crypto prices → Kafka (every 5–60 s)

# Consumers (run continuously)
make run-storage-consumer        # Kafka → MinIO (Avro, partitioned by asset_class/symbol/date)
make run-alert-consumer          # Real-time price threshold alerts (Python, stateless)

# Flink job (requires Flink containers running)
make run-flink-alert             # Submit PriceAlertJob to Flink cluster

# Spark batch job (run once at end of day; requires Spark containers running)
make run-ohlcv-daily-ingest      # Derive OHLCV bars from today's snapshots → MinIO Parquet
```

## Infrastructure UIs

| Service | URL | Credentials |
|---|---|---|
| Kafka UI (topic/partition browser) | http://localhost:8080 | — |
| MinIO Web Console | http://localhost:9001 | minioadmin / minioadmin |
| Flink Web UI | http://localhost:8081 | — |
| Spark Master Web UI | http://localhost:8082 | — |
| Spark History Server | http://localhost:18080 | — |
| JupyterLab | http://localhost:8888 | no token |

## Storage Layout

**`market-data`** (raw streaming, 30-day expiry):
```
price.snapshot/asset_class={stock|crypto}/symbol={SYM}/year={Y}/month={M}/day={D}/part-{ts}.avro
```

**`market-analysis`** (derived/processed, no expiry):
```
ohlcv.bar/asset_class={stock|crypto}/year={Y}/month={M}/day={D}/part-{ts}.parquet
```

## Jupyter

JupyterLab runs as a Docker service — no local install needed:

```bash
make jupyter-build   # build the image once (downloads S3A JARs — takes a moment)
make jupyter         # start → http://localhost:8888 (no token)
```

Starter notebooks in `notebooks/`: price snapshot explorer, OHLCV analysis with technical indicators, and a Spark query playground using `SparkFactory` + S3A in `local[*]` mode.

## Spark Cluster

Spark runs as a standalone Docker cluster to mirror a production environment.

```bash
make spark-build                 # Build (or rebuild) the Spark Docker image
```

`SparkFactory` (in `model/spark.py`) resolves the master automatically:
- `SPARK_MASTER_URL` unset → `local[*]` (local development)
- `SPARK_MASTER_URL=spark://spark-master:7077` → Docker cluster

## Alert Rules

Alert rules live in `config/alerts.json` and are evaluated by both `alert_consumer.py` (Python, stateless) and `PriceAlertJob` (Flink, stateful):

```json
[
  {"source": "stock",  "symbol": "*",        "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "Sharp drop — down 3% or more"},
  {"source": "crypto", "symbol": "BTC/USDT", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "BTC dropped 3% or more"}
]
```

## Testing

```bash
make test               # All tests (Docker must be running for integration)
make test-unit          # Unit tests only (no Docker needed)
make test-integration   # Integration tests (requires Kafka + MinIO running)
```

## Project Structure

```
├── docker/                     # docker-compose.yml, flink.Dockerfile, spark.Dockerfile
├── config/                     # stocks.json, crypto.json, alerts.json
├── producers/                  # stock_price_producer, crypto_price_producer, base_producer
├── consumers/                  # storage_consumer, alert_consumer, base_consumer
├── model/                      # MinioStore, SparkFactory, Avro/Parquet schemas
├── analysis/
│   ├── price_alert_job.py      # Flink: KeyedProcessFunction price alerts
│   └── batch/
│       └── ohlcv_daily_ingest.py  # Spark: price snapshots → OHLCV bars
├── schemas/                    # build_envelope() — common Kafka message wrapper
├── db/                         # MinIO bucket init and flush utilities
├── tests/                      # unit/ and integration/ with pytest markers
├── design/                     # DESIGN.md, TEST.md, architecture.drawio
├── reports/                    # Generated analysis output (gitignored)
└── main.py                     # CLI entry point
```

## Implementation Phases

| Phase | Status | What was built |
|---|---|---|
| 1 | ✅ | Docker Compose (Kafka + MinIO), bucket init, topic creation |
| 2 | ✅ | Smoke producer + consumer |
| 3 | ✅ | `stock_price_producer` — vnstock polling loop |
| 4 | ✅ | `storage_consumer` — Kafka → MinIO (Avro, asset_class/symbol/date partitions) |
| 5 | ✅ | `alert_consumer` — configurable threshold rules |
| 6 | ✅ | `crypto_price_producer` — CCXT/Binance polling |
| 7 | ✅ | `PriceAlertJob` — PyFlink DataStream + KeyedProcessFunction |
| 8 | ✅ | `ohlcv_daily_ingest` — Spark Docker cluster, S3A, derive OHLCV from snapshots |
| 9 | 📋 | `TechnicalJob` — SMA/RSI/MACD/BB over OHLCV Parquet history |
| 10 | 📋 | `DigestJob` — daily gainers/losers/volume digest |
| 11 | 📋 | `ScreenerJob` — P/E, D/E, EPS fundamental filter |
| 12 | 📋 | `VolatilityBurstJob` — Flink sliding window + per-symbol ValueState |
