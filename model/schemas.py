import pyarrow as pa

OHLCV_BAR_SCHEMA = pa.schema([
    pa.field("time",     pa.string()),
    pa.field("symbol",   pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("open",     pa.float64()),
    pa.field("high",     pa.float64()),
    pa.field("low",      pa.float64()),
    pa.field("close",    pa.float64()),
    pa.field("volume",   pa.int64()),
])
