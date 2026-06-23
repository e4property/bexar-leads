"""
tracerfy_skip.py — Auto skip trace new NOF/TAX leads via Tracerfy API.
Runs after main scrape via .github/workflows/skip_trace.yml
Only processes leads where:
  - type is NOF or TAX
  - is_new is True
  - tracerfy_phone is not already set
Picks rank-1 non-DNC mobile phone, falls back to rank-1 non-DNC landline.
Writes tracerfy_phone, tracerfy_name, tracerfy_dnc back to records.json.
"""

import json
import logging
import os
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TRACERFY_URL = "https://tracerfy.com/v1/api/trace/lookup/"
RECORDS_PATH = Path("dashboard/records.json")


def best_phone(phones):
    """
    Pick the best phone from Tracerfy phones array.
    Strategy: rank-1 non-DNC mobile first, then rank-1 non-DNC landline,
    then any rank-1 number, then None.
    Only ever returns ONE number.
    """
    if not phones:
        return None, False
    # Sort by rank
    sorted_phones = sorted(phones, key=lambda p: p.get("rank", 99))
    # 1. Non-DNC mobile
    for p in sorted_phones:
        if not p.get("dnc") and (p.get("type") or "").lower() == "mobile":
            return p["number"], p.get("dnc", False)
    # 2. Non-DNC landline
    for p in sorted_phones:
        if not p.get("dnc") and (p.get("type") or "").lower() == "landline":
            return p["number"], p.get("dnc", False)
    # 3. Any non-DNC
    for p in sorted_phones:
        if not p.get("dnc"):
            return p["number"], False
    # 4. Rank-1 even if DNC (flagged)
    return sorted_phones[0]["number"], sorted_phones[0].get("dnc", True)


def trace_lead(lead, api_key):
    """Call Tracerfy sync endpoint for one lead. Returns (phone, name, dnc) or (None, None, None)."""
    address = (lead.get("address") or "").strip()
    city = (lead.get("city") or "San Antonio").strip()
    zip_code = (lead.get("zip") or "").strip()
    if not address:
        return None, None, None
    payload = json.dumps({
        "address": address,
        "city": city,
        "state": "TX",
        "zip": zip_code,
        "find_owner": True
    }).encode("utf-8")
    req = urllib.request.Request(
        TRACERFY_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning(f"Tracerfy error for {address}: {e}")
        return None, None, None
    if not data.get("hit"):
        log.info(f"  MISS: {address}")
        return None, None, None
    persons = data.get("persons") or []
    if not persons:
        return None, None, None
    # Use first person (property owner)
    person = persons[0]
    name = person.get("full_name") or f"{person.get('first_name','')} {person.get('last_name','')}".strip()
    phones = person.get("phones") or []
    phone, dnc = best_phone(phones)
    if phone:
        log.info(f"  HIT: {address} → {phone} ({person.get('phones',[{}])[0].get('type','?')}) name={name}")
    else:
        log.info(f"  HIT but no usable phone: {address}")
    return phone, name, dnc


def main():
    api_key = os.environ.get("TRACERFY_API_KEY", "").strip()
    if not api_key:
        log.error("ABORT: TRACERFY_API_KEY env var not set")
        sys.exit(1)

    if not RECORDS_PATH.exists():
        log.error(f"ABORT: {RECORDS_PATH} not found")
        sys.exit(1)

    try:
        records = json.loads(RECORDS_PATH.read_text())
    except Exception as e:
        log.error(f"ABORT: Could not load records.json: {e}")
        sys.exit(1)

    log.info(f"Loaded {len(records)} records")

    # Find new NOF/TAX leads that haven't been traced yet
    targets = [
        r for r in records
        if r.get("is_new")
        and r.get("type") in ("NOF", "TAX")
        and not r.get("tracerfy_phone")
        and r.get("address")
    ]

    log.info(f"New NOF/TAX leads to trace: {len(targets)}")

    if not targets:
        log.info("Nothing to trace — exiting")
        print("traced=0")
        return

    hits = 0
    misses = 0
    for i, rec in enumerate(targets):
        log.info(f"[{i+1}/{len(targets)}] {rec.get('address')} ({rec.get('type')})")
        phone, name, dnc = trace_lead(rec, api_key)
        # Find and update the record in the full list
        for full_rec in records:
            if full_rec.get("doc_number") == rec.get("doc_number"):
                full_rec["tracerfy_phone"] = phone or ""
                full_rec["tracerfy_name"] = name or ""
                full_rec["tracerfy_dnc"] = dnc or False
                full_rec["tracerfy_ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                break
        if phone:
            hits += 1
        else:
            misses += 1
        # Rate limit — 500 RPM = ~8/sec, stay well under
        time.sleep(0.3)

    log.info(f"Tracerfy done: {hits} hits, {misses} misses out of {len(targets)} leads")

    # Save
    tmp = RECORDS_PATH.with_suffix(".tmp")
    json_str = json.dumps(records, separators=(",", ":"), ensure_ascii=True)
    tmp.write_text(json_str)
    tmp.replace(RECORDS_PATH)
    log.info(f"Saved {len(records)} records ({len(json_str):,} bytes)")
    print(f"traced={hits}")
    log.info("Done.")


if __name__ == "__main__":
    main()
