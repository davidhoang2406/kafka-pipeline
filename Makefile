.PHONY: install uninstall topics-create minio-init storage-flush run run-smoke-producer run-smoke-consumer \
        run-price-producer run-ohlcv-ingest \
        run-crypto-price-producer \
        run-storage-consumer run-alert-consumer \
        run-flink-alert \
        run-technical run-digest run-screener \
        test test-unit test-integration

PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

# ── Installation ──────────────────────────────────────────────────────────────
install: ## Interactively install selected infrastructure (Kafka, MinIO, Flink)
	$(PIP) install -r requirements.txt
	@echo "Select infrastructure to install:"
	@read -p "  Kafka + Kafka UI? [y/n] " k; \
	read -p "  MinIO (object storage)? [y/n] " m; \
	read -p "  Flink (JobManager + TaskManager)? [y/n] " fl; \
	if [ "$$k" != "y" ] && [ "$$m" != "y" ] && [ "$$fl" != "y" ]; then \
		echo "Nothing selected — aborted."; \
	else \
		services=""; \
		if [ "$$k" = "y" ]; then services="$$services kafka kafka-ui"; fi; \
		if [ "$$m" = "y" ]; then services="$$services minio"; fi; \
		if [ "$$fl" = "y" ]; then services="$$services flink-jobmanager flink-taskmanager"; fi; \
		if [ "$$fl" = "y" ]; then \
			echo "Building PyFlink Docker image..."; \
			docker compose build flink-jobmanager flink-taskmanager; \
			echo "Downloading Flink Kafka connector JAR (local mode)..."; \
			mkdir -p jars; \
			curl -fL -o jars/flink-sql-connector-kafka-4.0.1-2.0.jar \
				"https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar"; \
			echo "JAR ready in jars/"; \
		fi; \
		echo "Starting:$$services"; \
		docker compose up -d $$services; \
		if [ "$$m" = "y" ]; then \
			echo "Waiting for MinIO..."; \
			until curl -sf http://localhost:9000/minio/health/live 2>/dev/null; do \
				printf '.'; sleep 2; \
			done; \
			echo ""; \
			$(MAKE) minio-init; \
		fi; \
		if [ "$$k" = "y" ]; then \
			echo "Waiting for Kafka..."; \
			until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null; do \
				printf '.'; sleep 2; \
			done; \
			echo ""; \
			$(MAKE) topics-create; \
		fi; \
		echo "Setup complete."; \
	fi

uninstall: ## Selectively stop and remove services (data is permanently deleted)
	@echo "Select services to remove (data is permanently deleted):"
	@read -p "  Kafka + Kafka UI? [y/n] " k; \
	read -p "  MinIO (all stored Avro data)? [y/n] " m; \
	read -p "  Flink (JobManager + TaskManager)? [y/n] " fl; \
	if [ "$$k" != "y" ] && [ "$$m" != "y" ] && [ "$$fl" != "y" ]; then \
		echo "Nothing selected — aborted."; \
	else \
		if [ "$$k" = "y" ]; then \
			echo "Removing Kafka + Kafka UI..."; \
			docker compose rm -sf kafka kafka-ui; \
			docker volume ls -q | grep kafka_data | xargs docker volume rm 2>/dev/null || true; \
		fi; \
		if [ "$$m" = "y" ]; then \
			echo "Removing MinIO..."; \
			docker compose rm -sf minio; \
			docker volume ls -q | grep minio_data | xargs docker volume rm 2>/dev/null || true; \
		fi; \
		if [ "$$fl" = "y" ]; then \
			echo "Removing Flink..."; \
			docker compose rm -sf flink-jobmanager flink-taskmanager; \
		fi; \
		echo "Uninstall complete."; \
	fi

topics-create: ## Create all Kafka topics (safe to re-run — uses --if-not-exists)
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic stock.price.realtime  --partitions 6 --replication-factor 1
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic crypto.price.realtime --partitions 6 --replication-factor 1

minio-init: ## Create the market-data and market-analysis bucket in MinIO (safe to re-run)
	PYTHONPATH=. $(PYTHON) db/init_minio.py

storage-flush: ## Delete all objects from MinIO (irreversible)
	@echo "WARNING: this permanently deletes all data in the market-data bucket."
	@read -p "Type 'yes' to confirm: " ans && [ "$$ans" = "yes" ] || (echo "Aborted."; exit 1)
	PYTHONPATH=. $(PYTHON) db/flush_minio.py

run: ## Start all infrastructure containers (Kafka, MinIO, Flink, Kafka UI)
	docker compose up -d

# ── Running ───────────────────────────────────────────────────────────────────
run-smoke-producer:   ## [Phase 2] Send one hardcoded VCB message to Kafka
	$(PYTHON) main.py smoke-producer

run-smoke-consumer:   ## [Phase 2] Print messages arriving on stock.price.realtime
	$(PYTHON) main.py smoke-consumer

run-price-producer:   ## Poll vnstock price board → Kafka (every 30 s)
	$(PYTHON) main.py price-producer

run-ohlcv-ingest:           ## Derive OHLCV bars from price snapshots in MinIO (all symbols)
	$(PYTHON) main.py ohlcv-ingest

run-crypto-price-producer:  ## Poll crypto exchange prices → Kafka (every 60 s)
	$(PYTHON) main.py crypto-price-producer

run-storage-consumer: ## Kafka → MinIO (Avro)
	$(PYTHON) main.py storage-consumer

run-alert-consumer:   ## Real-time price threshold alerts
	$(PYTHON) main.py alert-consumer

run-flink-alert:      ## [Phase 8] Submit Flink price alert job to the Docker cluster
	docker exec flink-jobmanager flink run --python /opt/project/analysis/price_alert_job.py

run-technical:        ## Technical analysis report (SMA/RSI/MACD/BB)
	$(PYTHON) main.py technical

run-digest:           ## Daily market digest (gainers/losers/volume)
	$(PYTHON) main.py digest

run-screener:         ## Fundamental screener (P/E, D/E, EPS)
	$(PYTHON) main.py screener

test:                 ## Run all tests (Docker must be running for integration)
	$(PYTHON) -m pytest

test-unit:            ## Run unit tests only (no Docker needed)
	$(PYTHON) -m pytest -m unit

test-integration:     ## Run integration tests only (Docker must be running)
	$(PYTHON) -m pytest -m integration
