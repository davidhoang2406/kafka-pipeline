-- ── Create crypto database (idempotent) ──────────────────────────────────────
SELECT 'CREATE DATABASE crypto'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'crypto')\gexec

-- ══════════════════════════════════════════════════════════════════════════════
-- stocks database
-- ══════════════════════════════════════════════════════════════════════════════
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS price_snapshots (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    exchange    TEXT,
    price       NUMERIC,
    change      NUMERIC,
    pct_change  NUMERIC,
    volume      BIGINT,
    bid         NUMERIC,
    ask         NUMERIC
);

SELECT create_hypertable('price_snapshots', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_price_snapshots_symbol_time ON price_snapshots (symbol, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_snapshots_time_symbol ON price_snapshots (time, symbol);

CREATE TABLE IF NOT EXISTS ohlcv_daily (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    exchange    TEXT,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC,
    volume      BIGINT,
    UNIQUE (time, symbol)
);

SELECT create_hypertable('ohlcv_daily', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol_time ON ohlcv_daily (symbol, time DESC);

-- Quarterly financial statements (low volume — plain table, not hypertable)
CREATE TABLE IF NOT EXISTS financials (
    report_date  DATE NOT NULL,
    symbol       TEXT NOT NULL,
    period       TEXT,
    revenue      NUMERIC,
    net_income   NUMERIC,
    total_assets NUMERIC,
    total_debt   NUMERIC,
    eps          NUMERIC,
    PRIMARY KEY (report_date, symbol)
);

-- ══════════════════════════════════════════════════════════════════════════════
-- crypto database
-- ══════════════════════════════════════════════════════════════════════════════
\connect crypto

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS price_snapshots (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    exchange    TEXT,
    price       NUMERIC,
    change      NUMERIC,
    pct_change  NUMERIC,
    volume      BIGINT,
    bid         NUMERIC,
    ask         NUMERIC
);

SELECT create_hypertable('price_snapshots', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_price_snapshots_symbol_time ON price_snapshots (symbol, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_snapshots_time_symbol ON price_snapshots (time, symbol);

CREATE TABLE IF NOT EXISTS ohlcv_daily (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    exchange    TEXT,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC,
    volume      BIGINT,
    UNIQUE (time, symbol)
);

SELECT create_hypertable('ohlcv_daily', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol_time ON ohlcv_daily (symbol, time DESC);
