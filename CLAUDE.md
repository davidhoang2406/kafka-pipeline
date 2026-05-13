# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

Python 3.12 (via Homebrew at `/opt/homebrew/bin/python3.12`) with a venv at `.venv/`. Activate before running anything:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in values (defaults work with the Docker Compose setup as-is):

```bash
cp .env.example .env
```

## Infrastructure

Start Kafka (KRaft, no ZooKeeper) and TimescaleDB:

```bash
docker compose up -d
```

Apply the database schema (run once after first `docker compose up`):

```bash
source .venv/bin/activate
psql $TIMESCALE_URL -f db/schema.sql
# or if psql is not installed locally:
docker exec -i timescaledb psql -U postgres -d stocks < db/schema.sql
```

Stop and tear down (data is preserved in Docker volumes):

```bash
docker compose down
```

## Running the Pipeline

All commands are run from the project root with the venv active:

```bash
python main.py price-producer      # poll vnstock every 30 s → Kafka
python main.py ohlcv-producer      # fetch daily OHLCV → Kafka
python main.py storage-consumer    # Kafka → TimescaleDB
python main.py alert-consumer      # real-time price alerts
python main.py technical           # analysis report: SMA/RSI/MACD/BB
python main.py digest              # analysis report: gainers/losers/volume
python main.py screener            # analysis report: P/E, D/E, EPS filter
```

## Architecture

See `DESIGN.md` for the full design document and `architecture.drawio` for the system diagram (open in app.diagrams.net or the VS Code Draw.io extension).

**Data flow:** vnstock API → Producers → Kafka topics → StorageConsumer → TimescaleDB → Analysis layer → reports/

**Kafka topics:** `stock.price.realtime` · `stock.ohlcv.daily` · `stock.financials` — all partitioned by stock symbol.

**Key calibration:** poll interval and watchlist symbols live in `config/symbols.json`. Analysis filter thresholds live in `config/screener.json` (created in Phase 9).
