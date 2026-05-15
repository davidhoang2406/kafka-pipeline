import argparse


def main():
    parser = argparse.ArgumentParser(description="Stock & crypto streaming pipeline")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    sub.add_parser("smoke-producer",        help="[Phase 2] Send one hardcoded message to Kafka")
    sub.add_parser("smoke-consumer",        help="[Phase 2] Print every message on stock.price.realtime")
    sub.add_parser("stock-price-producer",  help="Poll vnstock price board → Kafka (every 30 s)")
    sub.add_parser("ohlcv-daily-ingest",    help="Derive daily OHLCV bars from price snapshots in MinIO")
    sub.add_parser("crypto-price-producer", help="Poll crypto exchange prices → Kafka (every 60 s)")
    sub.add_parser("storage-consumer",      help="Consume all topics and write to MinIO as Avro")
    sub.add_parser("alert-consumer",        help="Consume price topic and fire threshold alerts")
    sub.add_parser("flink-alert",           help="[Phase 8] Flink DataStream job: price alerts via KeyedProcessFunction")
    sub.add_parser("technical",             help="Run technical analysis report (SMA/RSI/MACD/BB)")
    sub.add_parser("digest",                help="Run daily market digest report (gainers/losers/volume)")
    sub.add_parser("screener",              help="Run fundamental screener report (P/E, D/E, EPS)")

    args = parser.parse_args()

    if args.command == "smoke-producer":
        import json
        from producers.base_producer import BaseProducer
        from schemas.message import build_envelope

        msg = build_envelope(
            event_type="price.snapshot",
            symbol="VCB",
            exchange="HOSE",
            payload={
                "price": 85000,
                "change": 500,
                "pct_change": 0.59,
                "volume": 1_234_567,
                "bid": 84900,
                "ask": 85100,
            },
        )
        with BaseProducer() as p:
            p.send("stock.price.realtime", value=msg, key="VCB")
            p.flush()
        print(f"Sent to stock.price.realtime:\n{json.dumps(msg, indent=2)}")

    elif args.command == "smoke-consumer":
        import json
        from consumers.base_consumer import BaseConsumer

        print("Listening on stock.price.realtime — press Ctrl+C to stop.\n")
        with BaseConsumer(["stock.price.realtime"], group_id="smoke") as c:
            for msg in c.messages():
                print(
                    f"partition={msg.partition}  offset={msg.offset}  key={msg.key}\n"
                    f"{json.dumps(msg.value, indent=2)}\n"
                )

    elif args.command == "stock-price-producer":
        from producers.stock_price_producer import run
        run()
    elif args.command == "ohlcv-daily-ingest":
        from analysis.batch.ohlcv_daily_ingest import run
        run()
    elif args.command == "crypto-price-producer":
        from producers.crypto_price_producer import run
        run()
    elif args.command == "storage-consumer":
        from consumers.storage_consumer import run
        run()
    elif args.command == "alert-consumer":
        from consumers.alert_consumer import run
        run()
    elif args.command == "flink-alert":
        from analysis.stream.price_alert_job import run
        run()
    elif args.command == "technical":
        from analysis.batch.technical_job import run
        run()
    elif args.command == "digest":
        from analysis.digest import run
        run()
    elif args.command == "screener":
        from analysis.screener import run
        run()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
