# Flink streaming job (Phase 8) that reads from stock.price.realtime and
# crypto.price.realtime, partitions events by symbol via key_by, and evaluates
# each tick against threshold rules in config/alerts.json using a KeyedProcessFunction.
# Logs every tick at INFO and prints/logs triggered alerts at WARNING.
# Must run inside the Flink Docker cluster: make run-flink-alert
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Explicit path so load_dotenv works regardless of cwd (local or Docker)
load_dotenv(Path(__file__).parent.parent / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = ["stock.price.realtime", "crypto.price.realtime"]
ALERTS_CONFIG = Path(__file__).parent.parent / "config" / "alerts.json"

# Used in local mode only. When running via flink run-python inside Docker,
# the JAR is already in $FLINK_HOME/lib/ and Flink loads it automatically.
_JAR = Path(__file__).parent.parent / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar"

from producers.utils import evaluate_rules


def run() -> None:
    try:
        from pyflink.common import WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            KafkaOffsetsInitializer,
            KafkaSource,
        )
        from pyflink.datastream.functions import KeyedProcessFunction
    except ImportError:
        raise SystemExit(
            "apache-flink is not installed.\n"
            "Run via Docker instead:  make flink-build && make run-flink-alert"
        )

    with open(ALERTS_CONFIG) as f:
        rules = json.load(f)

    # ── KeyedProcessFunction ──────────────────────────────────────────────────
    # Defined inside run() so it closes over `rules` — serialises correctly
    # in local mini-cluster mode (same process).
    # In Docker cluster mode (flink run-python), Flink serialises this class
    # via pickle and ships it to the TaskManager.

    class PriceAlertFunction(KeyedProcessFunction):
        """
        Stateless per-element check against threshold rules.
        Keyed by symbol even though no per-key state is stored yet — this
        partitions the stream so each parallel task owns a fixed subset of
        symbols, which is the prerequisite for adding ValueState / timers
        (cooldown windows, accumulating changes) in Phase 9.
        """

        def process_element(self, msg: dict, ctx: "KeyedProcessFunction.Context"):
            symbol  = msg.get("symbol", "")
            payload = msg.get("payload", {})
            source  = msg.get("source", "")
            price   = payload.get("price", 0.0)
            pct     = payload.get("pct_change", 0.0)
            ts      = msg.get("timestamp", "")[:19]

            log.info("tick  %-12s  price=%.4f  pct=%+.2f%%  source=%s", symbol, price, pct, source)

            for hit in evaluate_rules(rules, symbol, payload, source):
                alert = (
                    f"[ALERT {ts}] {symbol:10s} | {hit['message']}"
                    f" | price={price:.2f}  pct={pct:+.2f}%"
                    f"  {hit['matched_field']}={hit['matched_value']}"
                )
                log.warning(alert)
                yield alert

    # ── Build pipeline ────────────────────────────────────────────────────────

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # matches taskmanager.numberOfTaskSlots in docker-compose

    # Load Kafka connector JAR in local mode; Docker cluster has it in $FLINK_HOME/lib/
    if _JAR.exists():
        env.add_jars(f"file://{_JAR.resolve()}")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(*TOPICS)
        .set_group_id("flink-alerts")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka · price.realtime (stock + crypto)",
    )

    (
        stream
        .map(json.loads)
        .filter(lambda m: m.get("event_type") == "price.snapshot")
        .key_by(lambda m: m["symbol"])
        .process(PriceAlertFunction())
        .print()
    )

    log.info("Starting PriceAlertJob | topics=%s | rules=%d", TOPICS, len(rules))
    env.execute("PriceAlertJob")


if __name__ == "__main__":
    run()
