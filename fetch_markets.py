import glob
import json
import os
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data", "5m", "2026", "04", "19")
OUT = os.path.join(ROOT, "data", "markets.json")
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def scan_condition_ids():
    conds = set()
    for path in glob.glob(os.path.join(DATA_DIR, "*.jsonl")) + [
        p[:-4] + ".jsonl" for p in glob.glob(os.path.join(DATA_DIR, "*.zip"))
    ]:
        pass
    import zipfile

    for zp in sorted(glob.glob(os.path.join(DATA_DIR, "*.zip"))):
        with zipfile.ZipFile(zp) as z:
            with z.open(z.namelist()[0]) as f:
                first = f.readline()
                if first:
                    conds.add(json.loads(first)["market"])
    return sorted(conds)


def fetch(cond):
    url = f"https://clob.polymarket.com/markets/{cond}"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return cond, json.loads(r.read())


def main():
    conds = scan_condition_ids()
    print(f"{len(conds)} unique condition_ids")
    out = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(fetch, c): c for c in conds}
        for i, fut in enumerate(as_completed(futs), 1):
            cond = futs[fut]
            try:
                _, m = fut.result()
            except Exception as e:
                print(f"[{i}/{len(conds)}] fail {cond[:12]}: {e}")
                continue
            tokens = {t["outcome"]: t["token_id"] for t in m.get("tokens", [])}
            out[cond] = {
                "question": m.get("question"),
                "end_date": m.get("end_date_iso") or m.get("end_date"),
                "up": tokens.get("Up"),
                "down": tokens.get("Down"),
            }
            if i % 25 == 0 or i == len(conds):
                print(f"[{i}/{len(conds)}] {cond[:12]} up={tokens.get('Up','?')[:10]}... down={tokens.get('Down','?')[:10]}...")
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {OUT} ({len(out)} markets)")


if __name__ == "__main__":
    main()
