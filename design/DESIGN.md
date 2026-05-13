# Design Document: Stock & Crypto Data Streaming with Kafka

## 1. Problem Statement

Both vnstock (Vietnamese equities) and crypto exchanges expose data through pull-based APIs. Without a broker in the middle, every downstream script must call each API directly — no history, no fanout, no decoupling. Kafka solves this by converting the pull into a push pipeline:

```
vnstock API  ──┐
               ├──► Producers ──► Kafka ──► Consumer(s)
Crypto API   ──┘
```

Any number of consumers (database writer, alerter, dashboard, ML model) can independently read the same stream without hitting the upstream APIs multiple times. Adding a new data source only requires a new producer; all existing consumers continue to work unchanged.

---

## 2. System Architecture

See **`architecture.drawio`** — open in [app.diagrams.net](https://app.diagrams.net) or the VS Code Draw.io extension.

The diagram uses a left-to-right landscape layout across four zones:

![architecture.jpg](architecture.jpg)

| Zone | Components |
|---|---|
| **Ingestion** | vnstock API → PriceProducer · OHLCVProducer; Crypto Exchange API (CCXT) → CryptoPriceProducer · CryptoOHLCVProducer |
| **Apache Kafka 4.0** (KRaft) | `stock.price.realtime` · `stock.ohlcv.daily` · `stock.financials` · `crypto.price.realtime` · `crypto.ohlcv.daily` |
| **Storage path** | StorageConsumer → MinIO (Avro: `price.snapshot`, `ohlcv.bar`, `financials.report`) |
| **Apache Flink 2.0** (streaming) | PriceAlertJob · TechnicalJob · VolatilityBurstJob → real-time alerts |
| **Apache Spark** (batch) | FundamentalValuationScreen · HistoricalBacktestingEngine · DataQualityAudit → scheduled reports |

**Arrow key:** solid lines = storage writes; dashed lines = Flink streaming reads from Kafka; dotted lines = Spark batch reads from MinIO.

---

## 3. Kafka Topic Design

| Topic | Partition Key | Retention | Purpose |
|---|---|---|---|
| `stock.price.realtime` | stock symbol (e.g. `VCB`) | 1 day | Live price board snapshots (vnstock) |
| `stock.ohlcv.daily` | stock symbol | 30 days | End-of-day OHLCV bars (vnstock) |
| `stock.financials` | stock symbol | 90 days | Balance sheet, income statement (vnstock) |
| `crypto.price.realtime` | trading pair (e.g. `BTC-USDT`) | 1 day | Live ticker snapshots (CCXT) |
| `crypto.ohlcv.daily` | trading pair | 30 days | End-of-day OHLCV bars (CCXT) |

**Why partition by symbol / pair?** Messages for the same asset always go to the same partition, preserving order and making it cheap for consumers that only care about a subset.

**Why separate topics for stock vs crypto?** Different cadences, different sources, and different compliance/retention requirements. Consumers that only care about stocks can subscribe to `stock.*` without processing crypto noise — and vice versa.

---

## 4. Message Schema

All messages from both pipelines share the same JSON envelope:

```json
{
  "event_type": "price.snapshot",
  "symbol": "BTC/USDT",
  "exchange": "BINANCE",
  "timestamp": "2024-05-12T10:30:00+00:00",
  "source": "ccxt/binance",
  "payload": {
    "price": 62500.0,
    "change": 1200.0,
    "pct_change": 1.96,
    "volume": 1234567890,
    "bid": 62490.0,
    "ask": 62510.0
  }
}
```

The `source` field distinguishes the data origin (`"vnstock/KBS"` vs `"ccxt/binance"`). The `exchange` field carries the venue (`"HOSE"`, `"BINANCE"`, etc.). The `payload` shape is identical for the same `event_type` regardless of source — no special-casing needed in StorageConsumer or Flink jobs.

---

## 5. Component Breakdown

### `producers/price_producer.py`
- Polls `Trading(source='KBS').price_board(symbols)` every N seconds (default 300 s)
- Publishes to `stock.price.realtime` with `source="vnstock/KBS"`
- Symbol list loaded from `config/symbols.json`

### `producers/ohlcv_producer.py`
- Runs once daily (triggered by a scheduler or cron)
- Calls `Quote(symbol, source='VCI').history(...)` and `Finance(symbol).income_statement()` for each symbol
- Publishes to `stock.ohlcv.daily` and `stock.financials`

### `producers/crypto_price_producer.py`
- Polls `exchange.fetch_tickers(symbols)` every N seconds (default 60 s)
- Exchange and symbol list loaded from `config/crypto.json` (default: Binance, BTC/USDT · ETH/USDT · BNB/USDT · SOL/USDT)
- Publishes to `crypto.price.realtime` with `source="ccxt/<exchange>"`
- Kafka partition key uses `-` instead of `/` in pair names (`BTC-USDT`)

### `producers/crypto_ohlcv_producer.py`
- Runs once daily (triggered by a scheduler or cron)
- Calls `exchange.fetch_ohlcv(symbol, timeframe='1d', limit=N)` for each pair
- Publishes to `crypto.ohlcv.daily`

### `consumers/storage_consumer.py`
- Subscribes to **all five topics** (stock and crypto)
- Routes by `event_type` — `price.snapshot`, `ohlcv.bar`, `financials.report` each have a dedicated extractor
- No special-casing needed: the `exchange` field already distinguishes HOSE rows from BINANCE rows
- Batches rows in memory (up to 500 or 30 s), then flushes as deflate-compressed Avro to MinIO
- Partition layout: `s3://market-data/{event_type}/symbol={symbol}/year={year}/month={month}/day={day}/part-{ts}.avro`

### `consumers/alert_consumer.py`
- Subscribes to `stock.price.realtime`
- Checks configurable rules (e.g. "alert if VCB drops > 2%")
- Prints to console (extendable to email/Telegram)

---

## 5a. Storage Schema (MinIO + Avro)

Data lands in a single MinIO bucket (`market-data`) partitioned by event type, symbol, year, month, and day. Avro is used because it embeds the schema in each file and is row-oriented — well suited for streaming appends. Files are deflate-compressed via fastavro.

```
market-data/
├── price.snapshot/
│   └── symbol=VCB/
│       └── year=2024/month=05/day=12/
│           └── part-1715510400000.avro
├── ohlcv.bar/
│   └── symbol=BTC-USDT/
│       └── year=2024/month=05/day=12/
│           └── part-1715510400000.avro
└── financials.report/
    └── symbol=VCB/
        └── year=2024/month=03/day=31/
            └── part-1715510400000.avro
```

**Avro schemas** (defined in `consumers/storage_consumer.py` via fastavro):

| Event type | Key columns |
|---|---|
| `price.snapshot` | `time`, `symbol`, `exchange`, `price`, `change`, `pct_change`, `volume`, `bid`, `ask` |
| `ohlcv.bar` | `time`, `symbol`, `exchange`, `open`, `high`, `low`, `close`, `volume` |
| `financials.report` | `report_date`, `symbol`, `period`, `revenue`, `net_income`, `total_assets`, `total_debt`, `eps` |

Connection settings (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`) come from `.env`. Bucket is initialised by `db/init_minio.py`.

---

## 5b. Streaming Layer — Apache Flink

The streaming layer is built on **Apache Flink 2.0**, a stateful stream-processing engine that reads directly from Kafka topics. All three jobs are latency-sensitive — they lose value if results are delayed beyond seconds.

Flink runs two services in Docker Compose:
- **JobManager** — coordinates job scheduling, fault tolerance, and checkpointing.
- **TaskManager** — executes the actual operators (4 task slots).

Web UI: http://localhost:8081

### `PriceAlertJob`
- **Source:** `stock.price.realtime` + `crypto.price.realtime` (consumer group `flink-alerts`)
- Applies configurable threshold rules from `config/alerts.json` using Flink's `ProcessFunction`
- **Sink:** console / Telegram

### `TechnicalJob`
- **Source:** `stock.ohlcv.daily` (consumer group `flink-technical`)
- Uses a **sliding window** over the last N bars to compute SMA 20/50/200, RSI (14), MACD, Bollinger Bands
- **Sink:** `reports/technical_YYYY-MM-DD.txt`

### `VolatilityBurstJob`
- **Source:** `stock.price.realtime` + `crypto.price.realtime` (consumer group `flink-volatility`)
- Uses a **sliding window** (e.g. 5 minutes) to track the peak-to-trough price range per symbol; fires an alert when the intra-window range exceeds a configurable threshold (e.g. 3%)
- Tracks range using `ValueState` — distinct from `PriceAlertJob` which only checks a single tick's `pct_change`
- **Sink:** console / Telegram

### Key Flink concepts used
| Concept | Where applied |
|---|---|
| **DataStream API** | all three jobs |
| **Sliding window** | TechnicalJob · VolatilityBurstJob — rolling aggregation over recent events |
| **ProcessFunction** | PriceAlertJob · VolatilityBurstJob — fine-grained per-record and per-window logic |
| **ValueState** | VolatilityBurstJob — tracks min/max price within each window per symbol |
| **Kafka source connector** | all jobs read from Kafka with managed offsets |
| **Consumer groups** | each job has its own group — full copy of every message |

---

## 5c. Batch Layer — Apache Spark

The batch layer is built on **PySpark** and reads the Avro files written to MinIO by the StorageConsumer. Jobs are triggered on a schedule (e.g. nightly or weekly) and are suited to workloads that require full historical data rather than real-time results.

Spark runs in **local mode** (`local[*]`) — no separate cluster needed. It reads MinIO via the S3A connector (`hadoop-aws` + `aws-java-sdk` JARs), treating `s3a://market-data/...` as the data source.

### `FundamentalValuationScreen`
- **Sources:** `financials.report` + `price.snapshot` Avro partitions in MinIO
- Joins latest quarterly financials with most recent price per symbol
- Computes P/E, P/B, debt-to-equity, EPS growth; ranks all symbols by composite score
- **Sink:** `reports/valuation_YYYY-WW.txt` (weekly)

### `HistoricalBacktestingEngine`
- **Source:** `ohlcv.bar` Avro partitions in MinIO for a configurable date range
- Applies a configurable strategy (e.g. SMA crossover, RSI reversal) to each symbol's full price history
- Computes per-symbol P&L, Sharpe ratio, max drawdown, and win rate
- **Sink:** `reports/backtest_YYYY-MM-DD.txt`

### `DataQualityAudit`
- **Source:** all Avro partitions in MinIO (`price.snapshot`, `ohlcv.bar`, `financials.report`)
- Checks for: missing trading days per symbol, zero or null prices, duplicate timestamps, symbols with stale feeds (last record older than expected cadence)
- **Sink:** `reports/data_quality_YYYY-MM-DD.txt`

### Key Spark concepts used
| Concept | Where applied |
|---|---|
| **SparkSession (local mode)** | all three jobs — no cluster required |
| **S3A connector** | reads Avro files from MinIO using `s3a://` path |
| **DataFrame API** | all jobs — filtering, groupBy, join, window functions |
| **Window functions** | FundamentalValuationScreen — latest-record-per-symbol using `row_number()` |
| **UDFs** | HistoricalBacktestingEngine — strategy logic applied per symbol partition |
| **Cross-partition scan** | DataQualityAudit — reads all year/month/day partitions in one pass |

---

## 6. Local Infrastructure (Docker)

Everything runs locally via Docker Compose — no cloud account needed:

```
docker-compose.yml
  └── kafka                (port 9092)   image: apache/kafka:4.0.0  — KRaft mode, no ZooKeeper
  └── kafka-ui             (port 8080)   image: ghcr.io/kafbat/kafka-ui — topic browser
  └── minio                (port 9000/9001) image: minio/minio — S3-compatible object storage + web console
  └── flink-jobmanager     (port 8081)   image: flink:2.0-java17 — Flink Web UI + job coordinator
  └── flink-taskmanager               — 4 task slots for parallel operator execution
```

Kafka 4.0 removed ZooKeeper entirely. The broker runs in **KRaft mode** — single node acts as both `broker` and `controller`.

On first run, initialise the MinIO bucket:
```bash
python db/init_minio.py
```

---

## 7. Project File Layout

```
Kafka/
├── docker-compose.yml
├── config/
│   ├── symbols.json            # Stock watchlist and poll interval
│   ├── crypto.json             # Crypto exchange, pairs, and poll interval
│   ├── alerts.json             # Price alert rules
│   └── screener.json           # Screener filter thresholds
├── producers/
│   ├── base_producer.py        # Shared KafkaProducer setup
│   ├── price_producer.py       # vnstock: real-time price polling loop
│   ├── ohlcv_producer.py       # vnstock: daily OHLCV + financials
│   ├── crypto_price_producer.py  # CCXT: real-time crypto ticker polling
│   └── crypto_ohlcv_producer.py  # CCXT: daily crypto OHLCV
├── consumers/
│   ├── base_consumer.py        # Shared KafkaConsumer setup
│   ├── storage_consumer.py     # Persist all topics to MinIO (Avro)
│   └── alert_consumer.py       # Price threshold alerts (stocks)
├── schemas/
│   └── message.py              # build_envelope() — common JSON wrapper
├── db/
│   └── init_minio.py           # Creates the market-data bucket in MinIO
├── analysis/
│   ├── stream/
│   │   ├── price_alert_job.py      # Flink: per-record threshold alerts
│   │   ├── technical_job.py        # Flink: sliding-window SMA/RSI/MACD/BB
│   │   └── volatility_burst_job.py # Flink: intra-window peak-to-trough range alerts
│   └── batch/
│       ├── fundamental_valuation.py # Spark: P/E, D/E ranking from MinIO
│       ├── backtesting.py           # Spark: strategy simulation over OHLCV history
│       └── data_quality.py          # Spark: missing data, stale feeds, outlier scan
├── reports/                    # Output from analysis layer (gitignored)
├── tests/
│   ├── conftest.py
│   ├── unit/
│   └── integration/
├── design/
│   ├── DESIGN.md               # This document
│   └── TEST.md                 # Testing strategy
├── .env                        # MINIO_* / KAFKA_* settings (gitignored)
├── requirements.txt
└── main.py                     # CLI entry point
```

---

## 8. Implementation Phases

| Phase | Type | Goal | Concept learned |
|---|---|---|---|
| **1** | Infra | Docker Compose up (Kafka + MinIO), initialise bucket via `db/init_minio.py` | Docker multi-service setup, MinIO S3-compatible storage |
| **2** | Infra | Produce a hardcoded message, consume and print it | Kafka: topics, producers, consumers |
| **3** | Producer | `price_producer.py` polling vnstock every 5 min | Kafka: producer loop, serialization, partition keys |
| **4** | Consumer | `storage_consumer.py` writing to MinIO as Avro | Kafka: consumer groups, offset management; MinIO: partitioned Avro writes |
| **5** | Consumer | `alert_consumer.py` with threshold rules | Kafka: multiple consumer groups on the same topic |
| **6** | Producer | `ohlcv_producer.py` + daily historical data | Kafka: multiple topics with different cadences |
| **7** | Producer | `crypto_price_producer.py` + `crypto_ohlcv_producer.py` (CCXT) | Multi-source ingestion; same envelope schema across sources |
| **8** | Streaming | `stream/price_alert_job.py` — Flink job replaces alert_consumer | Flink: DataStream API, Kafka source connector, ProcessFunction |
| **9** | Streaming | `stream/technical_job.py` — SMA, RSI, MACD, BB via sliding window | Flink: sliding windows, stateful aggregation with ValueState |
| **10** | Streaming | `stream/volatility_burst_job.py` — intra-window peak-to-trough range alerts | Flink: sliding windows with per-symbol min/max state, multi-topic source |
| **11** | Batch | `batch/fundamental_valuation.py` — P/E, D/E, EPS ranking across all stocks | Spark: SparkSession local mode, S3A connector, DataFrame joins, window functions |
| **12** | Batch | `batch/backtesting.py` — strategy simulation over full OHLCV history | Spark: UDFs, partitioned reads, P&L and Sharpe ratio computation |
| **13** | Batch | `batch/data_quality.py` — missing days, stale feeds, outlier detection | Spark: cross-partition scan, null checks, groupBy aggregations |

---

## 9. Key Kafka Concepts Encountered in This Project

- **Producer acknowledgment (`acks`)** — `acks=1` (fast, small risk of loss) vs `acks=all` (durable). Start with `acks=1` during development.
- **Consumer groups** — two consumers in the *same* group share partitions (load balancing); two consumers in *different* groups each receive a full copy of every message. The alert and storage consumers must be in **different groups**.
- **Auto offset reset** — `earliest` replays all stored messages on first start; `latest` only reads new ones. Use `earliest` in development so consumers can be rerun against existing data.
- **Topic naming convention** — `<source>.<data-type>.<cadence>` (e.g. `crypto.price.realtime`) makes it easy to filter by source or cadence with wildcard subscriptions.
- **Polling cadence** — vnstock and CCXT are both HTTP APIs. Polling on a timer and publishing snapshots to Kafka is a standard pattern (mirrors what Kafka Connect's JDBC/HTTP source connectors do).

---

## 10. Out of Scope (Intentional)

- **Schema registry** (Avro/Protobuf) — plain JSON is sufficient to learn core concepts
- **Multi-broker cluster** — single broker is functionally identical from the application's perspective
- **PyFlink** — the Python Flink API exists but adds JVM bridging overhead. The Flink jobs in this project are written in Python using PyFlink (`apache-flink` on PyPI), which submits jobs to the Java Flink cluster running in Docker.
- **WebSocket feeds** — CCXT REST API polling is simpler and sufficient; WebSocket would replace the polling loop in `crypto_price_producer.py` for sub-second latency
- **Crypto financials** — on-chain metrics (TVL, fees, staking APR) are out of scope for this learning project
