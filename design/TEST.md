# TEST.md

Testing strategy for the Vietnamese Stock Streaming Pipeline.

## Tiers

Tests are split by external dependency:

| Tier | Marker | Requires | Speed |
|---|---|---|---|
| **Unit** | `@pytest.mark.unit` | Nothing (pure Python) | < 1 s total |
| **Integration** | `@pytest.mark.integration` | Docker Compose up, Kafka + TimescaleDB healthy | Seconds–minutes |

## Running tests

```bash
# Unit only (no Docker needed)
.venv/bin/pytest -m unit

# Integration only (Docker must be running)
.venv/bin/pytest -m integration

# Everything
.venv/bin/pytest
```

## File layout

```
tests/
├── conftest.py                  # shared fixtures (DB connection, Kafka producer/consumer)
├── unit/
│   ├── test_message.py          # build_envelope
│   ├── test_coerce.py           # _coerce_float / _coerce_int / _to_ts
│   ├── test_alert_rules.py      # _check() — the core alert logic
│   └── test_storage_routes.py   # _ROUTES extractors in storage_consumer
└── integration/
    ├── test_price_pipeline.py   # produce → Kafka → consume → verify
    ├── test_ohlcv_pipeline.py   # ohlcv bar round-trip
    └── test_alert_pipeline.py   # threshold triggers end-to-end
```

## `pytest.ini`

```ini
[pytest]
markers =
    unit: no external dependencies
    integration: requires Docker Compose (Kafka + TimescaleDB)
```

---

## Unit test cases

### `tests/unit/test_message.py` — envelope schema

| Test | What it checks |
|---|---|
| `test_all_fields_present` | returned dict has all 6 keys |
| `test_default_timestamp_is_utc` | timestamp ends with `+00:00` when no override |
| `test_custom_timestamp_passes_through` | explicit timestamp is not replaced |
| `test_payload_is_not_mutated` | original payload dict is untouched |

```python
from schemas.message import build_envelope

def test_all_fields_present():
    msg = build_envelope("price.snapshot", "VCB", "HOSE", {"price": 85000})
    assert {"event_type", "symbol", "exchange", "timestamp", "source", "payload"} == set(msg)

def test_custom_timestamp_passes_through():
    msg = build_envelope("ohlcv.bar", "VCB", "HOSE", {}, timestamp="2024-05-10T00:00:00+00:00")
    assert msg["timestamp"] == "2024-05-10T00:00:00+00:00"
```

---

### `tests/unit/test_coerce.py` — defensive type coercion

| Test | Input | Expected |
|---|---|---|
| `test_float_none` | `None` | `0.0` |
| `test_float_bad_string` | `"N/A"` | `0.0` |
| `test_float_valid` | `"85000.5"` | `85000.5` |
| `test_int_none` | `None` | `0` |
| `test_int_float_string` | `"1234.9"` | `1234` |
| `test_to_ts_bare_date` | `"2024-05-10"` | `"2024-05-10T00:00:00+00:00"` |
| `test_to_ts_pandas_timestamp` | `pd.Timestamp("2024-05-10")` | contains `"2024-05-10"` |

---

### `tests/unit/test_alert_rules.py` — rule evaluation

This is the highest-value unit test because `_check()` is pure logic with no I/O.

| Test | Rule | Payload | Expected output |
|---|---|---|---|
| `test_wildcard_sharp_drop` | `* pct_change <= -3.0` | `pct_change=-3.5` | prints alert |
| `test_wildcard_no_trigger` | `* pct_change <= -3.0` | `pct_change=-1.0` | silent |
| `test_symbol_specific_match` | `VCB pct_change >= 2.0` | symbol=VCB, pct=2.5 | prints alert |
| `test_symbol_specific_skip` | `VCB pct_change >= 2.0` | symbol=ACB, pct=2.5 | silent |
| `test_zero_price_flag` | `* price == 0.0` | `price=0.0` | prints alert |
| `test_all_operators` | one rule per operator | boundary values | correct trigger/no-trigger |

```python
from consumers.alert_consumer import _check

RULES = [{"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "drop"}]

def test_wildcard_sharp_drop(capsys):
    _check(RULES, "HPG", {"price": 50.0, "pct_change": -3.5})
    assert "HPG" in capsys.readouterr().out

def test_wildcard_no_trigger(capsys):
    _check(RULES, "HPG", {"price": 50.0, "pct_change": -1.0})
    assert capsys.readouterr().out == ""
```

---

### `tests/unit/test_storage_routes.py` — row extractor lambdas

Each `_ROUTES` extractor is a pure function: message dict → DB row tuple. Verify the column order matches the SQL.

| Test | Event type | Checks |
|---|---|---|
| `test_price_snapshot_extractor` | `price.snapshot` | tuple[2] is symbol, tuple[4] is price |
| `test_ohlcv_bar_extractor` | `ohlcv.bar` | tuple[3] is open, tuple[6] is close |
| `test_financials_report_extractor` | `financials.report` | tuple[0] is report_date string |
| `test_unknown_event_type_ignored` | `"unknown"` | `buf.add()` does not append any row |

---

## Integration test cases

All integration tests use a shared `conftest.py` fixture that:
1. Verifies Kafka and TimescaleDB are reachable (skips with `pytest.skip` if not)
2. Creates a temporary Kafka topic and consumer group per test
3. Wraps DB operations in a transaction that is rolled back after each test

### `tests/integration/test_price_pipeline.py`

| Test | Steps | Assert |
|---|---|---|
| `test_price_snapshot_round_trip` | Produce one `price.snapshot` → run StorageConsumer for 15 s → query DB | Row exists in `price_snapshots` with correct symbol and price |
| `test_duplicate_message_idempotent` | Produce same message twice → run consumer | Exactly 1 row in DB (ON CONFLICT DO NOTHING) |
| `test_consumer_group_isolation` | Produce 1 msg → two consumers in different groups each read it | Both consumers receive the message |

### `tests/integration/test_ohlcv_pipeline.py`

| Test | Steps | Assert |
|---|---|---|
| `test_ohlcv_bar_round_trip` | Produce `ohlcv.bar` with a past trading date as timestamp | Row in `ohlcv_daily` with correct `time` column (not insertion time) |

### `tests/integration/test_alert_pipeline.py`

| Test | Steps | Assert |
|---|---|---|
| `test_alert_fires_on_threshold` | Produce `price.snapshot` with `pct_change=-4.0` → run AlertConsumer | Console output contains `[ALERT` |
| `test_alert_silent_below_threshold` | Produce with `pct_change=-1.0` | No output |
