"""
One-off backfill: calls RentCast's AVM endpoint for leads discovered since
a cutoff date and writes arv_estimate/arv_low/arv_high/arv_comp_count back
into data/records.json and dashboard/records.json.

Manual test run — RENTCAST_API_KEY was only just added as a repo secret and
this isn't wired into the nightly fetch.py pipeline yet. Run via the
"ARV Backfill (manual)" workflow to validate the integration on a small
batch first. Each unique address is only fetched once even if it appears
in both files, to stay inside RentCast's free-tier call budget.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API_KEY = os.environ.get("RENTCAST_API_KEY", "")
CUTOFF = sys.argv[1] if len(sys.argv) > 1 else "2026-08-07"
BASE_URL = "https://api.rentcast.io/v1/avm/value"


def fetch_arv(address, city, zipcode):
    full_addr = f"{address}, {city}, TX {zipcode}".strip(", ")
    url = f"{BASE_URL}?{urllib.parse.urlencode({'address': full_addr})}"
    req = urllib.request.Request(url, headers={
        "X-Api-Key": API_KEY,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return {
            "arv_estimate": data.get("price"),
            "arv_low": data.get("priceRangeLow"),
            "arv_high": data.get("priceRangeHigh"),
            "arv_comp_count": len(data.get("comparables") or []),
            "arv_fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        print(f"  HTTP {e.code} for {full_addr}: {body[:200]}")
        return None
    except Exception as e:
        print(f"  error for {full_addr}: {e}")
        return None


def main():
    if not API_KEY:
        print("RENTCAST_API_KEY not set — aborting")
        sys.exit(1)

    cutoff_dt = datetime.fromisoformat(CUTOFF).replace(tzinfo=timezone.utc)

    with open("data/records.json", encoding="utf-8") as f:
        data_records = json.load(f)

    eligible = []
    for r in data_records:
        ts = r.get("run_ts", "")
        if not ts or r.get("arv_estimate") or not r.get("address"):
            continue
        try:
            rt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if rt >= cutoff_dt:
            eligible.append(r)

    print(f"{len(eligible)} leads eligible (run_ts >= {CUTOFF}, no arv_estimate yet)")

    results_by_doc = {}
    for r in eligible:
        doc = r.get("doc_number")
        print(f"  fetching ARV: {r.get('address')}, {r.get('city')} {r.get('zip')}")
        result = fetch_arv(r.get("address", ""), r.get("city", ""), r.get("zip", ""))
        if result:
            results_by_doc[doc] = result
        time.sleep(1.2)

    print(f"{len(results_by_doc)}/{len(eligible)} ARV lookups succeeded")

    for path in ("data/records.json", "dashboard/records.json"):
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        updated = 0
        for r in records:
            res = results_by_doc.get(r.get("doc_number"))
            if res:
                r.update(res)
                updated += 1
        with open(path, "w", encoding="utf-8") as f:
            if "dashboard" in path:
                f.write(json.dumps(records, separators=(",", ":"), ensure_ascii=True))
            else:
                json.dump(records, f, indent=2)
        print(f"{path}: {updated} records updated")


if __name__ == "__main__":
    main()
