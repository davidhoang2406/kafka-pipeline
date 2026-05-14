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

Start Kafka (KRaft, no ZooKeeper) and MinIO:

```bash
docker compose up -d
```

Create the MinIO bucket (run once after first `docker compose up`):

```bash
make minio-init
```

MinIO Web Console: http://localhost:9001 (user: `minioadmin` / pass: `minioadmin`)

Stop and tear down (data is preserved in Docker volumes):

```bash
docker compose down
```

## Running the Pipeline

All commands are run from the project root with the venv active:

```bash
python main.py price-producer      # poll vnstock every 30 s → Kafka
python main.py ohlcv-producer      # fetch daily OHLCV → Kafka
python main.py storage-consumer    # Kafka → MinIO (Avro)
python main.py alert-consumer      # real-time price alerts
python main.py technical           # analysis report: SMA/RSI/MACD/BB
python main.py digest              # analysis report: gainers/losers/volume
python main.py screener            # analysis report: P/E, D/E, EPS filter
```

## Git Workflow

Every new phase or feature must follow this branch workflow:

1. **Create a feature branch** before writing any code:
   ```bash
   git checkout -b feature/<phase-or-feature-name>
   ```
2. **Develop and commit** on that branch.
3. **Push the branch** to origin when done:
   ```bash
   git push -u origin feature/<phase-or-feature-name>
   ```
4. **Open a PR** targeting `main` using `gh`:
   ```bash
   gh pr create --base main --title "..." --body "..."
   ```

Never commit directly to `main`. Always return the PR URL when done.

## Architecture

See `DESIGN.md` for the full design document and `architecture.drawio` for the system diagram (open in app.diagrams.net or the VS Code Draw.io extension).

**Data flow:** vnstock API → Producers → Kafka topics → StorageConsumer → MinIO (Avro) → Analysis layer → reports/

**Storage layout:** `s3://market-data/{event_type}/symbol={symbol}/year={year}/month={month}/day={day}/part-{ts}.avro` — partitioned by event type, symbol, year, month, and day. Files are deflate-compressed Avro (fastavro).

**Kafka topics:** `stock.price.realtime` · `crypto.price.realtime` — real-time price snapshots only. OHLCV and financials are batch-ingested directly to MinIO, not routed through Kafka.

**Key calibration:** poll interval and symbols live in `config/stocks.json` (stocks) and `config/crypto.json` (crypto). Analysis filter thresholds live in `config/screener.json` (created in Phase 9).
