"""
Phase 8 — Flink price alert job.

Concepts introduced:
  - StreamExecutionEnvironment  : entry point for the DataStream API
  - KafkaSource                 : Flink-native Kafka connector (vs plain kafka-python)
  - key_by                      : partition stream by symbol so one task owns all
                                  events for a given ticker (prerequisite for stateful
                                  processing in later phases)
  - KeyedProcessFunction        : per-element logic with access to per-key state and
                                  timers (state unused here; added in Phase 9)

Run (Docker cluster):
  make flink-build              # one-time: builds PyFlink image with Kafka JAR
  make run-flink-alert          # submits job via flink run-python inside the container
"""
import json
import logging
import operator
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

_OPS = {
    "<":  operator.lt,
    ">":  operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "==": operator.eq,
}


def _asset_class(source: str) -> str:
    if source.startswith("vnstock"):
        return "stock"
    if source.startswith("ccxt"):
        return "crypto"
    return "unknown"


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
            source      = msg.get("source", "")
            asset_class = _asset_class(source)
            symbol      = msg.get("symbol", "")
            payload     = msg.get("payload", {})
            price       = payload.get("price", 0.0)
            pct         = payload.get("pct_change", 0.0)
            ts          = msg.get("timestamp", "")[:19]

            log.info("tick  %-12s  price=%.4f  pct=%+.2f%%  source=%s", symbol, price, pct, source)

            for rule in rules:
                rule_source = rule.get("source", "*")
                if rule_source != "*" and rule_source != asset_class:
                    continue
                if rule["symbol"] != "*" and rule["symbol"] != symbol:
                    continue
                field = rule["field"]
                value = payload.get(field)
                if value is None:
                    continue
                op_fn = _OPS.get(rule["operator"])
                if op_fn and op_fn(value, rule["threshold"]):
                    alert = (
                        f"[ALERT {ts}] {symbol:10s} | {rule['message']}"
                        f" | price={price:.2f}  pct={pct:+.2f}%  {field}={value}"
                    )
                    log.warning(alert)
                    yield alert

    # ── Build pipeline ────────────────────────────────────────────────────────

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

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
