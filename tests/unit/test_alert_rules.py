import pytest

from consumers.alert_consumer import _check

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
def test_wildcard_sharp_drop_fires(capsys):
    _check(WILDCARD_DROP, "HPG", {"price": 50.0, "pct_change": -3.5})
    assert "HPG" in capsys.readouterr().out


@pytest.mark.unit
def test_wildcard_no_trigger_below_threshold(capsys):
    _check(WILDCARD_DROP, "HPG", {"price": 50.0, "pct_change": -1.0})
    assert capsys.readouterr().out == ""


@pytest.mark.unit
def test_wildcard_exact_threshold_triggers(capsys):
    _check(WILDCARD_DROP, "VCB", {"price": 85.0, "pct_change": -3.0})
    assert "[ALERT" in capsys.readouterr().out


# ── symbol-specific rules ─────────────────────────────────────────────────────

@pytest.mark.unit
def test_symbol_specific_match_fires(capsys):
    _check(VCB_RISE, "VCB", {"price": 85.0, "pct_change": 2.5})
    assert "VCB" in capsys.readouterr().out


@pytest.mark.unit
def test_symbol_specific_skips_other_symbol(capsys):
    _check(VCB_RISE, "ACB", {"price": 30.0, "pct_change": 2.5})
    assert capsys.readouterr().out == ""


# ── data quality rule ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_zero_price_flag_fires(capsys):
    _check(ZERO_PRICE, "VCB", {"price": 0.0, "pct_change": 0.0})
    assert "[ALERT" in capsys.readouterr().out


@pytest.mark.unit
def test_zero_price_flag_silent_for_nonzero(capsys):
    _check(ZERO_PRICE, "VCB", {"price": 85000.0, "pct_change": 0.0})
    assert capsys.readouterr().out == ""


# ── missing field ─────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_missing_field_does_not_crash(capsys):
    _check(WILDCARD_DROP, "VCB", {"price": 85.0})   # no pct_change key
    capsys.readouterr()                               # just verify no exception


# ── multiple rules ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_multiple_matching_rules_each_fire(capsys):
    rules = [
        {"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "big drop"},
        {"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -2.0, "message": "small drop"},
    ]
    _check(rules, "VCB", {"price": 85.0, "pct_change": -3.5})
    assert capsys.readouterr().out.count("[ALERT") == 2


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
def test_operator(op, threshold, price, should_fire, capsys):
    rule = [{"symbol": "*", "field": "price", "operator": op,
             "threshold": threshold, "message": "test"}]
    _check(rule, "VCB", {"price": price, "pct_change": 0.0})
    out = capsys.readouterr().out
    if should_fire:
        assert "[ALERT" in out, f"{op} {threshold} vs {price} should have fired"
    else:
        assert out == "",       f"{op} {threshold} vs {price} should not have fired"
