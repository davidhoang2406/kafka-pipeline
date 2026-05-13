# Fetches end-of-day OHLCV bars and quarterly financial statements from vnstock (VCI source)
# for all symbols in config/symbols.json, then publishes to `stock.ohlcv.daily` and
# `stock.financials`. Intended to run once per day (cron or manual trigger).
import logging
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from vnstock import Quote

from producers.base_producer import BaseProducer
from producers.utils import coerce_float, coerce_int, load_json_config, to_ts
from schemas.message import build_envelope

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OHLCV_TOPIC = "stock.ohlcv.daily"
FINANCIALS_TOPIC = "stock.financials"
CONFIG = Path(__file__).parent.parent / "config" / "symbols.json"
LOOKBACK_DAYS = 1


def _publish_ohlcv(producer: BaseProducer, symbol: str, exchange: str, start: str, end: str) -> int:
    df = Quote(symbol=symbol, source="VCI").history(start=start, end=end)
    if df is None or df.empty:
        log.warning("%s: OHLCV returned empty", symbol)
        return 0

    log.info("%s OHLCV columns: %s", symbol, list(df.columns))

    count = 0
    for _, row in df.iterrows():
        r = row.to_dict()
        trading_date = r.get("time") or r.get("tradingDate") or r.get("date")
        if trading_date is None:
            continue

        payload = {
            "open":   coerce_float(r.get("open")),
            "high":   coerce_float(r.get("high")),
            "low":    coerce_float(r.get("low")),
            "close":  coerce_float(r.get("close")),
            "volume": coerce_int(r.get("volume")),
        }
        producer.send(
            OHLCV_TOPIC,
            value=build_envelope("ohlcv.bar", symbol, exchange, payload, timestamp=to_ts(trading_date)),
            key=symbol,
        )
        count += 1

    return count


def _publish_financials(producer: BaseProducer, symbol: str, exchange: str) -> int:
    try:
        from vnstock import Finance  # noqa: PLC0415
    except ImportError:
        log.warning("Finance class not available in this vnstock version — skipping financials")
        return 0

    try:
        income  = Finance(symbol=symbol, period="quarter", source="VCI").income_statement(lang="en")
        balance = Finance(symbol=symbol, period="quarter", source="VCI").balance_sheet(lang="en")
    except Exception:
        log.exception("%s: financials fetch failed", symbol)
        return 0

    if income is None or income.empty:
        log.warning("%s: income statement returned empty", symbol)
        return 0

    log.info("%s income columns: %s", symbol, list(income.columns))
    if balance is not None and not balance.empty:
        log.info("%s balance columns: %s", symbol, list(balance.columns))

    count = 0
    for i, (_, row) in enumerate(income.iterrows()):
        r = row.to_dict()
        report_date = r.get("year") or r.get("report_date") or r.get("fiscal_date")
        if report_date is None:
            continue

        period_label = str(r.get("quarter", r.get("period", "Q?")))

        # Best-effort: match balance sheet row by position
        bal: dict = {}
        if balance is not None and not balance.empty and i < len(balance):
            bal = balance.iloc[i].to_dict()

        payload = {
            "report_date":   str(report_date)[:10],
            "period":        period_label,
            "revenue":       coerce_float(r.get("revenue") or r.get("net_revenue")),
            "net_income":    coerce_float(r.get("net_income") or r.get("profit_after_tax")),
            "total_assets":  coerce_float(bal.get("total_assets")),
            "total_debt":    coerce_float(bal.get("total_debt") or bal.get("short_term_borrowing")),
            "eps":           coerce_float(r.get("eps") or r.get("basic_eps")),
        }
        producer.send(
            FINANCIALS_TOPIC,
            value=build_envelope("financials.report", symbol, exchange, payload),
            key=symbol,
        )
        count += 1

    return count


def run() -> None:
    config = load_json_config(CONFIG)
    symbols: list = config["watchlist"]

    end   = date.today().strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    log.info("Starting OHLCV producer | symbols=%s | %s → %s", symbols, start, end)

    with BaseProducer() as producer:
        total_ohlcv = 0
        total_fin   = 0

        for symbol in symbols:
            try:
                n = _publish_ohlcv(producer, symbol, exchange="HOSE", start=start, end=end)
                log.info("%s: %d OHLCV bars → %s", symbol, n, OHLCV_TOPIC)
                total_ohlcv += n
            except Exception:
                log.exception("%s: OHLCV fetch failed", symbol)

            try:
                n = _publish_financials(producer, symbol, exchange="HOSE")
                if n:
                    log.info("%s: %d financial reports → %s", symbol, n, FINANCIALS_TOPIC)
                total_fin += n
            except Exception:
                log.exception("%s: financials fetch failed", symbol)

        producer.flush()
        log.info(
            "Done | OHLCV: %d bars → %s | Financials: %d reports → %s",
            total_ohlcv, OHLCV_TOPIC, total_fin, FINANCIALS_TOPIC,
        )
