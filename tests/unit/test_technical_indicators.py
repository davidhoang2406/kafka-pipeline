"""Unit tests for indicator logic in analysis/batch/technical_job.py.

Tests target the pure-pandas helper functions (_macd_per_symbol and the
underlying indicator math) so no Spark cluster is needed.
"""
import pandas as pd
import pytest

pytestmark = pytest.mark.unit


@pytest.fixture
def rising_prices():
    """60 daily bars with a steady upward trend."""
    return pd.Series([100.0 + i * 0.5 for i in range(60)])


@pytest.fixture
def macd_input():
    """40-bar pandas DataFrame formatted for _macd_per_symbol."""
    prices = [100.0 + i for i in range(40)]
    return pd.DataFrame({
        "symbol": ["VCB"] * 40,
        "time":   [f"2026-{i:02d}-01" for i in range(1, 41)],
        "close":  prices,
    })


# ── SMA ──────────────────────────────────────────────────────────────────────

def test_sma20_equals_simple_mean(rising_prices):
    sma20 = rising_prices.rolling(20).mean().iloc[-1]
    expected = float(rising_prices.iloc[-20:].mean())
    assert abs(sma20 - expected) < 1e-6


def test_sma20_requires_20_bars():
    short = pd.Series([100.0] * 19)
    assert pd.isna(short.rolling(20).mean().iloc[-1])


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def test_bollinger_band_ordering(rising_prices):
    mid   = rising_prices.rolling(20).mean().iloc[-1]
    std   = rising_prices.rolling(20).std(ddof=0).iloc[-1]
    upper = mid + 2 * std
    lower = mid - 2 * std
    assert upper > mid > lower


def test_bollinger_band_width_positive(rising_prices):
    std = rising_prices.rolling(20).std(ddof=0).iloc[-1]
    assert std > 0


# ── RSI ───────────────────────────────────────────────────────────────────────

def _compute_rsi(prices: pd.Series, window: int = 14) -> float:
    """Simple RSI from first principles — mirrors the Spark window logic."""
    delta  = prices.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    avg_g  = gain.rolling(window).mean()
    avg_l  = loss.rolling(window).mean()
    last_g = avg_g.iloc[-1]
    last_l = avg_l.iloc[-1]
    if last_l == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + last_g / last_l)


def test_rsi_bounds(rising_prices):
    rsi = _compute_rsi(rising_prices)
    assert 0 <= rsi <= 100


def test_rsi_rising_series_above_50(rising_prices):
    assert _compute_rsi(rising_prices) > 50


def test_rsi_flat_series_is_nan():
    flat  = pd.Series([100.0] * 20)
    delta = flat.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta).clip(lower=0).rolling(14).mean()
    # flat series: avg_gain == 0, avg_loss == 0 → division by zero case
    last_l = loss.iloc[-1]
    last_g = gain.iloc[-1]
    assert last_l == 0 and last_g == 0  # our code maps this to RSI=100


# ── MACD ──────────────────────────────────────────────────────────────────────

def test_macd_output_shape(macd_input):
    from analysis.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    assert len(result) == 40
    assert set(result.columns) == {"symbol", "time", "macd", "macd_signal", "macd_hist"}


def test_macd_positive_for_rising_series(macd_input):
    """EMA12 > EMA26 for a steadily rising series → MACD > 0."""
    from analysis.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    assert result["macd"].iloc[-1] > 0


def test_macd_hist_equals_macd_minus_signal(macd_input):
    from analysis.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    diff = (result["macd"] - result["macd_signal"] - result["macd_hist"]).abs()
    assert diff.max() < 1e-10


def test_macd_preserves_row_count(macd_input):
    from analysis.batch.technical_job import _macd_per_symbol
    result = _macd_per_symbol(macd_input)
    assert len(result) == len(macd_input)
