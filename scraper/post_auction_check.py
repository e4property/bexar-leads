#!/usr/bin/env python3
"""
post_auction_check.py - look up what happened to foreclosure leads 2 and 7 days after their auction date.

Counties:
  bexar   : checks the current owner on the Bexar appraisal district (Harris Govern public API) and classifies
            the result (still the lead / lender-REO / investor / person). Fully automatic.
  nueces  : the Nueces CAD site blocks automated searches (session token + reCAPTCHA) and the local appraisal roll is a
            twice-a-year file, so this mode builds the DUE LIST with one-click lookup links (clerk records, Xome,
            Nueces CAD) instead of guessing. It also flags leads whose owner changed on the local roll, if the roll is newer.

Read-only. Never contacts anyone. Writes results next to the records file under data/ (not into the public dashboard).

Examples:
  python post_auction_check.py --county bexar  --records dashboard/records.json
  python post_auction_check.py --county nueces --records dashboard/records.json --date 2026-11-05
  python post_auction_check.py --county bexar  --records dashboard/records.json --date 2026-10-08 --limit 15 --dry-run
Options:
  --all-leads   check every lead (default: only leads we actually pushed to Jarvis, i.e. contacted)
"""
import argparse, csv, datetime as dt, json, os, re, sys, time, urllib.parse, urllib.request

WINDOWS = (2, 7)
HGO_URL = "https://hgo.harrisgovern.com/bexar/api/property/property-search/property-basic-search-results"
LENDER_WORDS = ("BANK", "MORTGAGE", "FEDERAL", "FANNIE", "FREDDIE", "HOUSING", "SECRETARY", "TRUSTEE", "SERVIC", "LOAN", "FUNDING",
                "PENNYMAC", "WELLS FARGO", "CHASE", "NATIONSTAR", "MR COOPER", "LAKEVIEW", "CITIMORTGAGE", "OCWEN", "CARRINGTON",
                "SELENE", "NEWREZ", "FINANCE", "CREDIT UNION", "SAVINGS", "N A", "NATIONAL ASSOCIATION", "REO", "DEUTSCHE", "US BANK",
                "U S BANK", "BNY", "MELLON", "VETERANS", "HUD")
ENTITY_WORDS = (" LLC", " L L C", " INC", " LP", " L P", " LTD", " CORP", "HOLDINGS", "INVESTMENT", "PROPERTIES", "CAPITAL", "VENTURES",
                "REALTY", "HOMES", "PARTNERS", "GROUP", "TRUST", "ENTERPRISES", "ACQUISITION")


def parse_date(s):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime((s or "").strip(), fmt).date()
        except ValueError:
            pass
    return None


def tokens(s):
    return {t for t in re.sub(r"[^A-Z ]", " ", (s or "").upper()).split() if len(t) > 2 and t not in ("EST", "THE", "AND", "LLC")}


def classify(prev_owner, now_owner):
    n = (now_owner or "").upper()
    if not n:
        return "no owner found", "Could not read an owner. Check by hand."
    if tokens(prev_owner) & tokens(n):
        return "still the lead", "Owner unchanged on the roll. Not sold yet, reinstated, postponed, or the roll has not updated."
    if any(w in n for w in LENDER_WORDS):
        return "REO / lender-owned", "Owner is now a lender or servicer: bank-owned. Look for an REO listing."
    if any(w in " " + n + " " for w in ENTITY_WORDS):
        return "sold to investor/entity", "Owner is now an entity: a possible cash buyer. Note the name."
    return "sold to person", "Owner is now a different individual (third-party buyer or private sale)."


def street_core(addr):
    a = re.sub(r"\s+", " ", (addr or "").upper().replace(",", " ").replace("\n", " ")).strip()
    a = re.sub(r"\b(SAN ANTONIO|CORPUS CHRISTI|TX|TEXAS)\b.*$", "", a).strip()
    t = a.split()
    if len(t) < 2 or not t[0].isdigit():
        return None
    suf = {"ST", "DR", "RD", "PL", "LN", "BLVD", "CIR", "RDG", "MDW", "CYN", "PASS", "WAY", "CT", "AVE", "LOOP", "TRL", "PKWY", "STREET", "DRIVE",
           "ROAD", "PLACE", "LANE", "CIRCLE", "RIDGE", "MEADOW", "CANYON", "COURT", "AVENUE", "BEND", "CV", "COVE"}
    w = t[1:]
    if w and w[-1] in suf:
        w = w[:-1]
    return t[0] + " " + " ".join(w)


def _hgo_query(core):
    q = urllib.parse.urlencode({"searchText": '"' + core + '"', "skip": 0, "take": 5})
    err = "no response"
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(HGO_URL + "?" + q, headers={"User-Agent": "Mozilla/5.0"})
            d = json.loads(urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace"))
            res = d if isinstance(d, list) else d.get("results") or d.get("Results") or d.get("data") or []
            for r in res:
                if r.get("PropertyTypeCodeOnly") != "R":   # skip vehicles etc.
                    continue
                sit = re.sub(r"\s+", " ", (r.get("SitusAddress") or "").upper())
                if sit.startswith(core.split()[0] + " "):
                    return {"owner": (r.get("OwnerFullName") or "").strip(), "appraised": r.get("AppraisedValue"), "prop_id": r.get("PropertyId")}
            return {"owner": "", "appraised": None, "prop_id": None}
        except Exception as e:
            err = str(e)[:80]
            time.sleep(2)
    return {"error": err}


def hgo_owner(addr):
    core = street_core(addr)
    if not core:
        return None
    # try the full street name first, then drop the last word (unlisted suffixes like FALLS, BOULEVARD)
    tries = [core]
    words = core.split()
    if len(words) > 2:
        tries.append(" ".join(words[:-1]))
    out = None
    for t in tries:
        out = _hgo_query(t)
        if "error" in out or out.get("owner"):
            return out
    return out


def links(addr, county):
    one = re.sub(r"\s+", " ", (addr or "").replace("\n", " ")).strip()
    slug = re.sub(r"[^a-z0-9]+", "-", one.lower()).strip("-")
    c = {"Clerk records": "https://%s.tx.publicsearch.us" % county, "Xome": "https://www.xome.com/realestate/" + slug,
         "Zillow search": "https://www.zillow.com/homes/" + urllib.parse.quote(one) + "_rb/"}
    if county == "nueces":
        c["Nueces CAD"] = "https://esearch.nuecescad.net/"
    else:
        c["Bexar CAD"] = "https://hgo.harrisgovern.com/bexar/property/search"
        c["Bexar tax office"] = "https://bexar.acttax.com/act_webdev/bexar/index.jsp"
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--county", required=True, choices=["bexar", "nueces"])
    ap.add_argument("--records", required=True)
    ap.add_argument("--date", help="treat this as today (YYYY-MM-DD)")
    ap.add_argument("--all-leads", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true", help="do not write the history file")
    ap.add_argument("--out-dir", default=None)
    a = ap.parse_args()

    today = parse_date(a.date) if a.date else dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).date()
    base = os.path.dirname(os.path.dirname(os.path.abspath(a.records)))  # repo root
    out_dir = a.out_dir or os.path.join(base, "data", "post_auction")
    os.makedirs(out_dir, exist_ok=True)
    hist_path = os.path.join(out_dir, "post_auction_history.json")
    watch_path = os.path.join(out_dir, "post_auction_watch.json")
    hist = json.load(open(hist_path, encoding="utf-8")) if os.path.exists(hist_path) else {}
    watch = json.load(open(watch_path, encoding="utf-8")) if os.path.exists(watch_path) else {}
    recs = json.load(open(a.records, encoding="utf-8"))

    # The scraper purges leads once their sale date passes, so snapshot every contacted lead with a sale date
    # into a watch list now; the 2 and 7 day checks read from the watch list, not from records.json.
    for r in recs:
        sd = parse_date(r.get("sale_date"))
        addr = re.sub(r"\s+", " ", (r.get("address") or "").replace(chr(10), " ")).strip()
        if not sd or not addr or (not a.all_leads and not r.get("ghl_pushed")):
            continue
        wk = "%s|%s" % (addr.upper(), sd.isoformat())
        if wk not in watch:
            watch[wk] = {"address": addr, "sale_date": sd.isoformat(), "owner": r.get("owner") or "", "county": a.county}

    due = []
    for wk, w_rec in watch.items():
        sd = parse_date(w_rec["sale_date"])
        age = (today - sd).days
        for w in WINDOWS:
            if w <= age <= w + 2:   # small catch-up window if a day was missed
                key = "%s|%dd" % (wk, w)
                if key not in hist:
                    due.append((key, w, sd, {"address": w_rec["address"], "owner": w_rec["owner"]}))
                break
    if a.limit:
        due = due[:a.limit]
    print("%s: %d leads due on %s (windows %s days after sale; %s)" % (a.county, len(due), today, WINDOWS, "all leads" if a.all_leads else "contacted leads only"))

    rows = []
    for key, w, sd, r in due:
        row = {"window": "%d days after sale" % w, "sale_date": sd.isoformat(), "address": re.sub(r"\s+", " ", r["address"].replace("\n", " ")).strip(),
               "owner_before": r.get("owner") or "", "owner_now": "", "appraised_now": "", "outcome": "", "note": "", "checked_at": dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"}
        if a.county == "bexar":
            h = hgo_owner(r["address"])
            if h and "error" not in h:
                row["owner_now"] = h["owner"]; row["appraised_now"] = h["appraised"] if h["appraised"] is not None else ""
                row["outcome"], row["note"] = classify(r.get("owner"), h["owner"])
            else:
                row["outcome"] = "lookup failed"; row["note"] = (h or {}).get("error", "no result") + ". Retry or check by hand."
        else:
            row["outcome"] = "check by hand"
            row["note"] = "Nueces CAD blocks automated search. Use the links: clerk records (look for a Trustee's Deed recorded after the sale date), Xome, Nueces CAD."
        row.update(links(r["address"], a.county))
        rows.append(row)
        if not a.dry_run:
            hist[key] = {k: row[k] for k in ("window", "sale_date", "address", "owner_before", "owner_now", "outcome", "checked_at")}
        time.sleep(0.4)

    if rows:
        csv_path = os.path.join(out_dir, "post_auction_%s_%s.csv" % (a.county, today.isoformat()))
        cols = list(rows[0].keys())
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)
        print("wrote", csv_path)
        counts = {}
        for r in rows:
            counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
        print("summary:", counts)
        for r in rows:
            if r["outcome"] not in ("still the lead", "check by hand"):
                print("  *", r["address"][:36], "|", r["owner_before"][:22], "->", r["owner_now"][:30], "|", r["outcome"])
    if not a.dry_run:
        json.dump(hist, open(hist_path, "w", encoding="utf-8"), indent=1)
        json.dump(watch, open(watch_path, "w", encoding="utf-8"), indent=1)


if __name__ == "__main__":
    main()
