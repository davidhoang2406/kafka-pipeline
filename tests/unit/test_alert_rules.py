import pytest

from producers.utils import evaluate_rules

# ── fixtures / helpers ────────────────────────────────────────────────────────

WILDCARD_DROP = [
    {"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "Sharp drop"}
]
VCB_RISE = [
    {"symbol": "VCB", "field": "pct_change", "operator": ">=", "threshold": 2.0, "message": "VCB rose"}
]
ZERO_PRICE = [
    {"symbol": "*", "field": "price", "operator": "==", "threshold": 0.0, "message": "Zero price"}
]


# ── wildcard rules ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_wildcard_sharp_drop_fires():
    hits = evaluate_rules(WILDCARD_DROP, "HPG", {"price": 50.0, "pct_change": -3.5})
    assert len(hits) == 1
    assert hits[0]["matched_field"] == "pct_change"


@pytest.mark.unit
def test_wildcard_no_trigger_below_threshold():
    assert evaluate_rules(WILDCARD_DROP, "HPG", {"price": 50.0, "pct_change": -1.0}) == []


@pytest.mark.unit
def test_wildcard_exact_threshold_triggers():
    assert evaluate_rules(WILDCARD_DROP, "VCB", {"price": 85.0, "pct_change": -3.0})


# ── symbol-specific rules ─────────────────────────────────────────────────────

@pytest.mark.unit
def test_symbol_specific_match_fires():
    assert evaluate_rules(VCB_RISE, "VCB", {"price": 85.0, "pct_change": 2.5})


@pytest.mark.unit
def test_symbol_specific_skips_other_symbol():
    assert evaluate_rules(VCB_RISE, "ACB", {"price": 30.0, "pct_change": 2.5}) == []


# ── data quality rule ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_zero_price_flag_fires():
    assert evaluate_rules(ZERO_PRICE, "VCB", {"price": 0.0, "pct_change": 0.0})


@pytest.mark.unit
def test_zero_price_flag_silent_for_nonzero():
    assert evaluate_rules(ZERO_PRICE, "VCB", {"price": 85000.0, "pct_change": 0.0}) == []


# ── missing field ─────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_missing_field_does_not_crash():
    # no pct_change key — should silently skip the rule, not raise
    assert evaluate_rules(WILDCARD_DROP, "VCB", {"price": 85.0}) == []


# ── multiple rules ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_multiple_matching_rules_each_fire():
    rules = [
        {"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "big drop"},
        {"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -2.0, "message": "small drop"},
    ]
    assert len(evaluate_rules(rules, "VCB", {"price": 85.0, "pct_change": -3.5})) == 2


# ── all operators ─────────────────────────────────────────────────────────────

@pytest.mark.unit
@pytest.mark.parametrize("op,threshold,price,should_fire", [
    ("<",  10.0,  9.0, True),
    ("<",  10.0, 10.0, False),
    (">",  10.0, 11.0, True),
    (">",  10.0, 10.0, False),
    ("<=", 10.0, 10.0, True),
    ("<=", 10.0, 11.0, False),
    (">=", 10.0, 10.0, True),
    (">=", 10.0,  9.0, False),
    ("==", 10.0, 10.0, True),
    ("==", 10.0,  9.0, False),
])
def test_operator(op, threshold, price, should_fire):
    rule = [{"symbol": "*", "field": "price", "operator": op,
             "threshold": threshold, "message": "test"}]
    hits = evaluate_rules(rule, "VCB", {"price": price, "pct_change": 0.0})
    if should_fire:
        assert hits, f"{op} {threshold} vs {price} should have fired"
    else:
        assert hits == [], f"{op} {threshold} vs {price} should not have fired"
