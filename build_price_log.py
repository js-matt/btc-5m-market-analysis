import glob
import io
import json
import os
import re
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.abspath(__file__))
ZIP_DIR = os.path.join(ROOT, "data", "5m", "2026", "04", "19")
MARKETS = os.path.join(ROOT, "data", "markets.json")
OUT = os.path.join(ROOT, "data", "orderbook_price_log.parquet")

SCHEMA = pa.schema(
    [
        ("timestamp", pa.float64()),
        ("window_start", pa.int64()),
        ("market", pa.string()),
        ("asset_id", pa.string()),
        ("outcome", pa.string()),
        ("best_bid", pa.float64()),
        ("best_ask", pa.float64()),
        ("mid", pa.float64()),
    ]
)

FILENAME_RE = re.compile(r"btc-updown-5m-(\d+)\.zip$")


def load_asset_outcome_map():
    with open(MARKETS) as f:
        markets = json.load(f)
    m = {}
    for cond, info in markets.items():
        if info.get("up"):
            m[info["up"]] = "Up"
        if info.get("down"):
            m[info["down"]] = "Down"
    return m


def iter_price_changes(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            for raw in io.TextIOWrapper(f, encoding="utf-8"):
                d = json.loads(raw)
                if d.get("event_type") != "price_change":
                    continue
                yield d


def build_row_group(zip_path, outcome_map):
    window_start = int(FILENAME_RE.search(os.path.basename(zip_path)).group(1))
    ts, mkt, aid, out, bb, ba, mid = [], [], [], [], [], [], []
    for d in iter_price_changes(zip_path):
        timestamp_s = d["timestamp"] / 1000.0
        market = d["market"]
        for pc in d.get("price_changes", ()):
            bid = pc.get("best_bid")
            ask = pc.get("best_ask")
            if bid is None or ask is None:
                continue
            asset_id = pc["asset_id"]
            ts.append(timestamp_s)
            mkt.append(market)
            aid.append(asset_id)
            out.append(outcome_map.get(asset_id))
            bb.append(bid)
            ba.append(ask)
            mid.append((bid + ask) / 2.0)
    return pa.record_batch(
        [
            pa.array(ts, pa.float64()),
            pa.array([window_start] * len(ts), pa.int64()),
            pa.array(mkt, pa.string()),
            pa.array(aid, pa.string()),
            pa.array(out, pa.string()),
            pa.array(bb, pa.float64()),
            pa.array(ba, pa.float64()),
            pa.array(mid, pa.float64()),
        ],
        schema=SCHEMA,
    )


def main():
    outcome_map = load_asset_outcome_map()
    zips = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))
    print(f"{len(zips)} zips, {len(outcome_map)} asset_ids labeled")

    writer = pq.ParquetWriter(OUT, SCHEMA, compression="zstd", compression_level=3)
    t0 = time.time()
    total = 0
    unmapped = 0
    try:
        for i, zp in enumerate(zips, 1):
            batch = build_row_group(zp, outcome_map)
            writer.write_batch(batch)
            total += batch.num_rows
            unmapped += sum(1 for x in batch.column("outcome").to_pylist() if x is None)
            if i % 10 == 0 or i == len(zips):
                dt = time.time() - t0
                size_mb = os.path.getsize(OUT) / 1e6
                print(
                    f"[{i}/{len(zips)}] rows={total:,} unmapped={unmapped:,} "
                    f"file={size_mb:.1f} MB elapsed={dt:.1f}s"
                )
    finally:
        writer.close()

    size_mb = os.path.getsize(OUT) / 1e6
    print(f"done: {total:,} rows, {size_mb:.1f} MB at {OUT}")


if __name__ == "__main__":
    main()
