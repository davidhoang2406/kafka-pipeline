# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

Python 3.12 (via Homebrew at `/opt/homebrew/bin/python3.12`) with a venv at `.venv/`. Activate before running anything:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Environment variables live in `.env` (committed; the defaults work with the Docker Compose setup as-is).

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
make run-spark-technical         # Spark: SMA/RSI/MACD/BB report submitted to the Docker cluster
```

## Jupyter

Runs as a Docker service (no local install needed):

```bash
make jupyter-build   # build the image (first time or after requirements change)
make jupyter         # start container → http://localhost:8888 (no token)
```

The container mounts `notebooks/` (gitignored — for local exploration only). Inside it `PYTHONPATH=/opt/project` is set, and MinIO/Kafka are reached via their Docker service names (`minio:9000`, `kafka:29092`).

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

**Data flow:** Producers → Kafka → StorageConsumer → `market-data` MinIO (Avro) → `ohlcv_daily_ingest` Spark job → `market-analysis` MinIO (Parquet) → `technical_job` Spark report / Jupyter / analysis reports

**Storage:**
- `market-data`: `price.snapshot/asset_class={stock|crypto}/symbol={sym}/year=/month=/day=/part-{ts}.avro`
- `market-analysis`: `ohlcv.bar/asset_class={stock|crypto}/year=/month=/day=/part-{ts}.parquet`

**Kafka topics:** `stock.price.realtime` · `crypto.price.realtime` (6 partitions each, key = symbol)

**Spark:** `SparkFactory` in `model/spark.py` — context manager, auto-selects `local[*]` vs Docker cluster via `SPARK_MASTER_URL` env var. S3A JARs (hadoop-aws 3.4.1 + awssdk bundle 2.24.6) are pre-baked in the Docker image.

**Spark reading rule:** Always read partitioned data by pointing Spark at the root S3A prefix — never use `MinioStore` to list files and pass individual paths to `read.parquet()` / `read.format("avro").load()`. Spark walks the partition tree natively and infers partition columns automatically.

```python
# correct
df = spark.read.parquet("s3a://market-analysis/ohlcv.bar")

# never do this
files = [f"s3a://market-analysis/{o.object_name}" for o in store.list_objects(...)]
df = spark.read.parquet(*files)
```

**Key config files:** `config/stocks.json` (HOSE symbols + poll interval) · `config/crypto.json` (Binance pairs) · `config/alerts.json` (threshold rules)
