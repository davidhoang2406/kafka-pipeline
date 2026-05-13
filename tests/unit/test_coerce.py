import pytest
import pandas as pd

from producers.utils import coerce_float as _coerce_float, coerce_int as _coerce_int, to_ts as _to_ts


# ── _coerce_float ─────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCoerceFloat:
    def test_none_returns_default(self):
        assert _coerce_float(None) == 0.0

    def test_bad_string_returns_default(self):
        assert _coerce_float("N/A") == 0.0

    def test_empty_string_returns_default(self):
        assert _coerce_float("") == 0.0

    def test_valid_string(self):
        assert _coerce_float("85000.5") == 85000.5

    def test_valid_int(self):
        assert _coerce_float(42) == 42.0

    def test_negative_preserved(self):
        assert _coerce_float(-3.5) == -3.5

    def test_zero_preserved(self):
        assert _coerce_float(0) == 0.0

    def test_custom_default(self):
        assert _coerce_float(None, default=99.9) == 99.9


# ── _coerce_int ───────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCoerceInt:
    def test_none_returns_default(self):
        assert _coerce_int(None) == 0

    def test_bad_string_returns_default(self):
        assert _coerce_int("N/A") == 0

    def test_float_string_truncates(self):
        assert _coerce_int("1234.9") == 1234

    def test_valid_string(self):
        assert _coerce_int("1000000") == 1000000

    def test_negative_preserved(self):
        assert _coerce_int(-5) == -5

    def test_custom_default(self):
        assert _coerce_int(None, default=5) == 5


# ── _to_ts ────────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestToTs:
    def test_bare_date_string_gets_utc_midnight(self):
        assert _to_ts("2024-05-10") == "2024-05-10T00:00:00+00:00"

    def test_full_datetime_string_unchanged(self):
        result = _to_ts("2024-05-10T15:30:00+07:00")
        assert "2024-05-10" in result

    def test_pandas_timestamp(self):
        ts = pd.Timestamp("2024-05-10")
        assert "2024-05-10" in _to_ts(ts)

    def test_pandas_timestamp_with_time(self):
        ts = pd.Timestamp("2024-05-10 08:30:00")
        result = _to_ts(ts)
        assert "2024-05-10" in result
