# Kafka Streaming Pipeline

A real-time data streaming pipeline for Vietnamese stock and cryptocurrency market data, built with Apache Kafka, MinIO (Avro), and Apache Flink.

## Overview

Converts pull-based market APIs into a continuous push pipeline — any number of consumers (database writer, alerter, Flink jobs) read the same stream independently without hitting upstream APIs multiple times.

```
vnstock API  ──┐
               ├──► Producers ──► Kafka ──► StorageConsumer ──► MinIO (Avro)
Crypto API   ──┘                       └──► Flink Jobs ──► Alerts / Reports
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 4.0 (KRaft, no ZooKeeper) |
| Object storage | MinIO (S3-compatible, Avro files) |
| Stream processing | Apache Flink 2.0 + PyFlink |
| Stock data | vnstock (Vietnamese equities) |
| Crypto data | CCXT (Binance) |
| Infrastructure | Docker Compose |

## Kafka Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `stock.price.realtime` | 6 | Live stock price snapshots (every 30 s) |
| `crypto.price.realtime` | 6 | Live crypto ticker snapshots (every 60 s) |

OHLCV bars and financial statements are batch-ingested directly into MinIO — they do not flow through Kafka.

All messages share a common JSON envelope:

```json
{
  "event_type": "price.snapshot",
  "symbol": "BTC/USDT",
  "exchange": "BINANCE",
  "timestamp": "2024-05-12T10:30:00+00:00",
  "source": "ccxt/binance",
  "payload": {
    "price": 62500.0,
    "pct_change": 1.96,
    "volume": 1234567890
  }
}
```

## Quick Start

**Prerequisites:** Docker, Python 3.12, `make`

```bash
# Clone and set up
git clone https://github.com/davidhoang2406/kafka-pipeline.git
cd kafka-pipeline
python3.12 -m venv .venv && source .venv/bin/activate

# Copy environment config (defaults work out of the box)
cp .env.example .env

# Interactive install — select which services to start
make install
```

`make install` asks which infrastructure to start (Kafka, MinIO, Flink), then handles Docker builds, bucket creation, and topic creation automatically.

## Running the Pipeline

Open separate terminals for each process:

```bash
# Producers
python main.py price-producer           # Vietnamese stocks → Kafka (every 30 s)
python main.py ohlcv-producer           # Daily stock OHLCV → Kafka
python main.py crypto-price-producer    # Crypto prices → Kafka (every 60 s)
python main.py crypto-ohlcv-producer    # Daily crypto OHLCV → Kafka

# Consumers
python main.py storage-consumer         # Kafka → MinIO (Avro)
python main.py alert-consumer           # Real-time price threshold alerts

# Flink job (requires Flink containers running)
make run-flink-alert                    # Submit PriceAlertJob to Flink cluster
```

## Flink Price Alert Job

`PriceAlertJob` is a stateful streaming job that reads from both `stock.price.realtime` and `crypto.price.realtime`, partitions events by symbol via `key_by`, and evaluates each tick against configurable rules in `config/alerts.json`.

```bash
# Submit job
make run-flink-alert

# Monitor
docker exec flink-jobmanager flink list
docker logs -f flink-taskmanager

# Flink Web UI
open http://localhost:8081
```

Alert rules are configured in `config/alerts.json`:

```json
[
  {"source": "stock",  "symbol": "*",       "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "Sharp drop — down 3% or more"},
  {"source": "crypto", "symbol": "BTC/USDT","field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "BTC dropped 3% or more"}
]
```

## Infrastructure UIs

| Service | URL |
|---|---|
| Kafka UI (topic browser) | http://localhost:8080 |
| MinIO Web Console | http://localhost:9001 |
| Flink Web UI | http://localhost:8081 |

## Analysis Reports

```bash
python main.py technical    # SMA / RSI / MACD / Bollinger Bands
python main.py digest       # Top gainers, losers, volume spikes
python main.py screener     # Fundamental filter: P/E, D/E, EPS
```

Reports are written to `reports/` (gitignored).

## Testing

```bash
make test               # All tests (Docker must be running for integration)
make test-unit          # Unit tests only (no Docker needed)
make test-integration   # Integration tests (Docker must be running)
```

## Project Structure

```
├── producers/              # Kafka producers (stock + crypto)
├── consumers/              # Storage and alert consumers
├── analysis/               # PyFlink streaming jobs
├── schemas/                # Shared message envelope builder
├── config/                 # Symbols, alert rules, screener thresholds
├── db/                     # MinIO bucket initialisation (init_minio.py)
├── tests/                  # Unit + integration tests
├── design/                 # Architecture diagram and design document
├── docker-compose.yml
├── flink.Dockerfile        # PyFlink 2.0 image with Kafka connector
└── main.py                 # CLI entry point
```

## Implementation Phases

| Phase | What was built |
|---|---|
| 1 | Docker Compose setup, MinIO bucket, Kafka topics |
| 2 | Smoke producer + consumer |
| 3 | `price_producer.py` — vnstock polling loop |
| 4 | `storage_consumer.py` — Kafka → MinIO (partitioned Avro) |
| 5 | `alert_consumer.py` — threshold rules |
| 6 | `ohlcv_producer.py` — daily OHLCV + financials |
| 7 | Crypto producers (CCXT) |
| 8 | `PriceAlertJob` — PyFlink DataStream + KeyedProcessFunction |
| 9–11 | Flink windowed jobs: technical analysis, digest, screener *(planned)* |
