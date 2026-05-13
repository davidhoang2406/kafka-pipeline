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
| **Storage path** | StorageConsumer → TimescaleDB (`price_snapshots`, `ohlcv_daily`, `financials`) |
| **Apache Flink 2.0** | PriceAlertJob · TechnicalJob · DigestJob · ScreenerJob → reports / alerts |

**Arrow key:** solid lines = storage writes; dashed lines = Flink streaming reads from Kafka.

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
- Routes by `event_type` — `price.snapshot` → `price_snapshots`, `ohlcv.bar` → `ohlcv_daily`, `financials.report` → `financials`
- No special-casing needed: the `exchange` column already distinguishes HOSE rows from BINANCE rows
- Uses `psycopg2` with `execute_values` for efficient batch inserts; idempotent via `ON CONFLICT DO NOTHING`

### `consumers/alert_consumer.py`
- Subscribes to `stock.price.realtime`
- Checks configurable rules (e.g. "alert if VCB drops > 2%")
- Prints to console (extendable to email/Telegram)

---

## 5a. Database Schema (TimescaleDB)

Three tables cover all data produced by both pipelines. The `exchange` column distinguishes stock venues from crypto venues — no schema changes needed to add crypto.

```sql
-- Hypertable: real-time price snapshots (stock and crypto)
CREATE TABLE price_snapshots (
  time        TIMESTAMPTZ   NOT NULL,
  symbol      TEXT          NOT NULL,
  exchange    TEXT,                      -- 'HOSE', 'BINANCE', etc.
  price       NUMERIC,
  change      NUMERIC,
  pct_change  NUMERIC,
  volume      BIGINT,
  bid         NUMERIC,
  ask         NUMERIC
);
SELECT create_hypertable('price_snapshots', 'time');
CREATE INDEX ON price_snapshots (symbol, time DESC);

-- Hypertable: end-of-day OHLCV bars (stock and crypto)
CREATE TABLE ohlcv_daily (
  time        TIMESTAMPTZ   NOT NULL,
  symbol      TEXT          NOT NULL,
  exchange    TEXT,
  open        NUMERIC,
  high        NUMERIC,
  low         NUMERIC,
  close       NUMERIC,
  volume      BIGINT,
  UNIQUE (time, symbol)
);
SELECT create_hypertable('ohlcv_daily', 'time');

-- Regular table: quarterly financial statements (stocks only)
CREATE TABLE financials (
  report_date   DATE    NOT NULL,
  symbol        TEXT    NOT NULL,
  period        TEXT,
  revenue       NUMERIC,
  net_income    NUMERIC,
  total_assets  NUMERIC,
  total_debt    NUMERIC,
  eps           NUMERIC,
  PRIMARY KEY (report_date, symbol)
);
```

Schema lives in `db/schema.sql`. Connection settings come from `TIMESCALE_URL` in `.env`.

---

## 5b. Analysis Layer — Apache Flink

The analysis layer is built on **Apache Flink 2.0**, a stateful stream-processing engine that reads directly from Kafka topics. Flink jobs process live streams with windowing operators and emit results continuously.

Flink runs two services in Docker Compose:
- **JobManager** — coordinates job scheduling, fault tolerance, and checkpointing.
- **TaskManager** — executes the actual operators (4 task slots).

Web UI: http://localhost:8081

### `PriceAlertJob`
- **Source:** `stock.price.realtime` (consumer group `flink-alerts`)
- Applies configurable threshold rules from `config/alerts.json` using Flink's `ProcessFunction`
- **Sink:** console / Telegram

### `TechnicalJob`
- **Source:** `stock.ohlcv.daily` (consumer group `flink-technical`)
- Uses a **sliding window** over the last N bars to compute SMA 20/50/200, RSI (14), MACD, Bollinger Bands
- **Sink:** `reports/technical_YYYY-MM-DD.txt`

### `DigestJob`
- **Source:** `stock.price.realtime` (consumer group `flink-digest`)
- Uses a **tumbling window** (one trading session) to rank symbols by `pct_change` and detect volume spikes
- **Sink:** `reports/digest_YYYY-MM-DD.txt`

### `ScreenerJob`
- **Sources:** `stock.financials` + `stock.price.realtime` (co-stream join)
- Computes P/E, EPS, debt-to-equity; applies filter thresholds from `config/screener.json`
- **Sink:** `reports/screener_YYYY-MM-DD.txt`

### Key Flink concepts used
| Concept | Where applied |
|---|---|
| **DataStream API** | all four jobs |
| **Sliding window** | TechnicalJob — rolling indicator history |
| **Tumbling window** | DigestJob — session-level aggregation |
| **Co-stream join** | ScreenerJob — join two Kafka topics by symbol |
| **ProcessFunction** | PriceAlertJob — fine-grained per-record logic |
| **Kafka source connector** | all jobs read from Kafka with managed offsets |
| **Consumer groups** | each job has its own group — full copy of every message |

---

## 6. Local Infrastructure (Docker)

Everything runs locally via Docker Compose — no cloud account needed:

```
docker-compose.yml
  └── kafka                (port 9092)   image: apache/kafka:4.0.0  — KRaft mode, no ZooKeeper
  └── kafka-ui             (port 8080)   image: ghcr.io/kafbat/kafka-ui — topic browser
  └── timescaledb          (port 5432)   image: timescale/timescaledb:latest-pg16
  └── flink-jobmanager     (port 8081)   image: flink:2.0-java17 — Flink Web UI + job coordinator
  └── flink-taskmanager               — 4 task slots for parallel operator execution
```

Kafka 4.0 removed ZooKeeper entirely. The broker runs in **KRaft mode** — single node acts as both `broker` and `controller`.

On first run, apply the TimescaleDB schema:
```bash
psql $TIMESCALE_URL -f db/schema.sql
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
│   ├── storage_consumer.py     # Persist all topics to TimescaleDB
│   └── alert_consumer.py       # Price threshold alerts (stocks)
├── schemas/
│   └── message.py              # build_envelope() — common JSON wrapper
├── db/
│   └── schema.sql              # TimescaleDB table definitions
├── analysis/
│   ├── price_alert_job.py      # Flink: CEP threshold alerts
│   ├── technical_job.py        # Flink: sliding-window SMA/RSI/MACD/BB
│   ├── digest_job.py           # Flink: tumbling-window top movers
│   └── screener_job.py         # Flink: co-stream join, P/E filter
├── reports/                    # Output from analysis layer (gitignored)
├── tests/
│   ├── conftest.py
│   ├── unit/
│   └── integration/
├── design/
│   ├── DESIGN.md               # This document
│   └── TEST.md                 # Testing strategy
├── .env                        # TIMESCALE_URL (gitignored)
├── requirements.txt
└── main.py                     # CLI entry point
```

---

## 8. Implementation Phases

| Phase | Goal | Concept learned |
|---|---|---|
| **1** | Docker Compose up (Kafka + TimescaleDB), apply `db/schema.sql` | Docker multi-service setup, TimescaleDB hypertables |
| **2** | Produce a hardcoded message, consume and print it | Kafka: topics, producers, consumers |
| **3** | `price_producer.py` polling vnstock every 5 min | Kafka: producer loop, serialization, partition keys |
| **4** | `storage_consumer.py` writing to TimescaleDB | Kafka: consumer groups, offset management; DB: batch inserts, idempotency |
| **5** | `alert_consumer.py` with threshold rules | Kafka: multiple consumer groups on the same topic |
| **6** | `ohlcv_producer.py` + daily historical data | Kafka: multiple topics with different cadences |
| **7** | `crypto_price_producer.py` + `crypto_ohlcv_producer.py` (CCXT) | Multi-source ingestion; same envelope schema across sources |
| **8** | `analysis/price_alert_job.py` — Flink job replaces alert_consumer | Flink: DataStream API, Kafka source connector, ProcessFunction |
| **9** | `analysis/technical_job.py` — SMA, RSI, MACD, BB via sliding window | Flink: sliding windows, stateful aggregation |
| **10** | `analysis/digest_job.py` — top movers + volume spikes | Flink: tumbling windows, keyed streams |
| **11** | `analysis/screener_job.py` — fundamental ratio filter | Flink: co-stream join across two Kafka topics |

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
