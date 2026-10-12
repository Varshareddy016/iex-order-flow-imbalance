import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from iex_parser import Parser, DEEP_1_0

RAW = "../data/raw/deep_20260911.pcap.gz"
OUT = Path("../data/interim/deep_20260911")
SYMBOLS = {"SPY","QQQ","IWM","VCSH","EMB"}
KEEP_TYPES = {"price_level_update","trade_report"}
CHUNK = 100000

def flush(rows,part):
    if not rows:
        return
    table = pa.Table.from_pylist(rows)
    pq.write_table(table,OUT / f"part_{part:04d}.parquet")
    print(f"whole part {part} ({len(rows):,} rows)", flush=True)

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    rows,part,seen = [],0,0
    with Parser (RAW,DEEP_1_0) as reader:
        for msg in reader:
            if seen > 200000:
                break;
            seen += 1
            if msg.get("type") not in KEEP_TYPES:
                continue
            sym = msg.get("symbol")
            sym = sym.decode() if isinstance(sym,bytes) else sym
            if sym not in SYMBOLS:
                continue
            rows.append({
                "timestamp" : msg["timestamp"],
                "symbol": sym,
                "type": msg["type"],
                "side": msg.get("side"),
                "price": float(msg["price"]),
                "size": int(msg["size"]),
                "flags": msg.get("flags"),
                "trade_id": msg.get("trade_id"),
            })
            if len(rows) >= CHUNK :
                flush(rows,part);
                rows,part = [],part+1
    flush(rows,part)
    print(f"done: scanned {seen:,} messages")

if __name__ == "__main__":
    sys.exit(main())