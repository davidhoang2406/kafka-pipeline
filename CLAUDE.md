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

All Docker services live in `docker/docker-compose.yml`. Start everything:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Or use the interactive installer which prompts for which services to start:

```bash
make install
```

Create MinIO buckets (run once after first start):

```bash
make minio-init
```

Key service URLs: Kafka UI :8080 · MinIO console :9001 · Flink UI :8081 · Spark Master :8082 · Spark History :18080

## Running the Pipeline

```bash
make run-stock-price-producer    # vnstock → stock.price.realtime (every 30 s)
make run-crypto-price-producer   # CCXT/Binance → crypto.price.realtime
make run-storage-consumer        # Kafka → MinIO Avro (asset_class/symbol/date partitions)
make run-alert-consumer          # Python threshold alerts
make run-flink-alert             # Submit PriceAlertJob to Flink cluster
make run-ohlcv-daily-ingest      # Spark: derive OHLCV bars from today's snapshots → Parquet
```

## Jupyter

```bash
make jupyter    # starts JupyterLab at http://localhost:8888, rooted at notebooks/
```

Three starter notebooks are in `notebooks/`:
- `01_price_snapshots.ipynb` — explore raw Avro files from MinIO
- `02_ohlcv_analysis.ipynb` — OHLCV charts + SMA/RSI/MACD/BB via `ta`
- `03_spark_query.ipynb` — cross-day queries using SparkFactory + S3A

All notebooks use `PYTHONPATH=.` (set by the Makefile target) so `from model.minio_store import MinioStore` etc. resolve correctly.

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

See `design/DESIGN.md` for the full design document and `design/architecture.drawio` for the system diagram.

**Data flow:** Producers → Kafka → StorageConsumer → `market-data` MinIO (Avro) → `ohlcv_daily_ingest` Spark job → `market-analysis` MinIO (Parquet) → Jupyter / analysis reports

**Storage:**
- `market-data`: `price.snapshot/asset_class={stock|crypto}/symbol={sym}/year=/month=/day=/part-{ts}.avro`
- `market-analysis`: `ohlcv.bar/asset_class={stock|crypto}/year=/month=/day=/part-{ts}.parquet`

**Kafka topics:** `stock.price.realtime` · `crypto.price.realtime` (6 partitions each, key = symbol)

**Spark:** `SparkFactory` in `model/spark.py` — context manager, auto-selects `local[*]` vs Docker cluster via `SPARK_MASTER_URL` env var. S3A JARs (hadoop-aws 3.4.1 + awssdk bundle 2.24.6) are pre-baked in the Docker image.

**Key config files:** `config/stocks.json` (HOSE symbols + poll interval) · `config/crypto.json` (Binance pairs) · `config/alerts.json` (threshold rules)
