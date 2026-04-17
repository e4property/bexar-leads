"""
Bexar County Motivated Seller Lead Scraper
Sources (all public, no auth, no bot detection):
1. maps.bexar.org/arcgis - Foreclosure map (mortgage + tax)
2. gis-bexar.opendata.arcgis.com - Open data portal
3. maps.bexar.org/arcgis Parcels layer - owner/address enrichment
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# Public ArcGIS endpoints - no auth needed
FORECLOSURE_URL = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer/{layer}/query"
PARCELS_URL     = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

# Layer 0 = Mortgage foreclosures, Layer 1 = Tax foreclosures
FORECLOSURE_LAYERS = {
    0: ("NOFC", "Notice of Foreclosure / Mortgage"),
    1: ("TAXDEED", "Tax Foreclosure"),
}


def clean(val):
    if val is None:
        return ""
    return str(val).strip()

def escape_sql(s):
    return s.replace("'", "''")

def normalize_name(name):
    name = name.strip().upper()
    variants = [name]
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            variants.append(parts[1] + " " + parts[0])
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.append(tokens[-1] + ", " + " ".join(tokens[:-1]))
    seen = []
    for v in variants:
        if v not in seen:
            seen.append(v)
    return seen

def date_range_mmddyyyy(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")

def is_new_this_week(filed_str):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            d = datetime.strptime(filed_str.split("T")[0], fmt)
            return (datetime.now() - d).days <= 7
        except ValueError:
            continue
    return False


def compute_flags(record):
    flags    = []
    cat      = record.get("cat", "")
    owner    = record.get("owner", "").upper()
    doc_type = record.get("doc_type", "")
    if cat == "NOFC":   flags.append("Pre-foreclosure")
    if cat == "TAXDEED": flags.append("Tax lien")
    if cat == "LP":      flags.append("Lis pendens")
    if cat == "JUD":     flags.append("Judgment lien")
    if cat == "LIEN":    flags.append("Mechanic lien")
    if cat == "PRO":     flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS)\b", owner):
        flags.append("LLC / corp owner")
    if is_new_this_week(record.get("filed", "")):
        flags.append("New this week")
    return flags

def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    amount = record.get("amount", 0.0) or 0.0
    if amount > 100000: score += 15
    elif amount > 50000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address"): score += 5
    return min(score, 100)


class ForeclosureScraper:
    """Pulls from Bexar County's public ArcGIS foreclosure map."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 BexarLeadBot/2.0"

    def _query_layer(self, layer_id, where="1=1", offset=0):
        url = FORECLOSURE_URL.format(layer=layer_id)
        params = {
            "where":             where,
            "outFields":         "*",
            "returnGeometry":    "false",
            "resultRecordCount": 1000,
            "resultOffset":      offset,
            "f":                 "json",
        }
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    log.warning("ArcGIS error layer %d: %s", layer_id, data["error"])
                    return []
                features = data.get("features", [])
                log.info("  Layer %d offset=%d: %d features", layer_id, offset, len(features))
                return features
            except Exception as exc:
                log.warning("  Layer %d attempt %d: %s", layer_id, attempt+1, exc)
                time.sleep(3)
        return []

    def fetch_all_layers(self):
        all_records = []
        for layer_id, (cat, cat_label) in FORECLOSURE_LAYERS.items():
            log.info("Fetching foreclosure layer %d (%s)...", layer_id, cat_label)
            offset = 0
            while True:
                features = self._query_layer(layer_id, offset=offset)
                if not features:
                    break
                for feat in features:
                    attrs = feat.get("attributes", {})
                    # Map common field names from Bexar GIS foreclosure layer
                    owner = clean(
                        attrs.get("OWNER") or attrs.get("Owner") or
                        attrs.get("GRANTEE") or attrs.get("Grantee") or
                        attrs.get("NAME") or ""
                    )
                    address = clean(
                        attrs.get("ADDRESS") or attrs.get("Address") or
                        attrs.get("SITUS") or attrs.get("Situs") or ""
                    )
                    sale_date = clean(
                        attrs.get("SALE_DATE") or attrs.get("SaleDate") or
                        attrs.get("AUCTION_DATE") or attrs.get("AuctionDate") or
                        attrs.get("DATE") or ""
                    )
                    doc_num = clean(
                        attrs.get("INSTRUMENT") or attrs.get("Instrument") or
                        attrs.get("DOC_NUM") or attrs.get("DocNum") or
                        attrs.get("OBJECTID") or ""
                    )
                    amount = 0.0
                    for amtk in ["AMOUNT","Amount","JUDGMENT_AMT","JudgmentAmt","TAX_AMT"]:
                        v = attrs.get(amtk)
                        if v:
                            try:
                                amount = float(str(v).replace("$","").replace(",",""))
                            except Exception:
                                pass
                            break

                    # Log first record fields for debugging
                    if offset == 0 and len(all_records) == 0:
                        log.info("  Sample fields: %s", list(attrs.keys())[:15])
                        log.info("  Sample values: owner=%s addr=%s date=%s",
                                 owner[:30], address[:30], sale_date)

                    all_records.append({
                        "doc_num":   str(doc_num),
                        "doc_type":  cat,
                        "filed":     sale_date,
                        "cat":       cat,
                        "cat_label": cat_label,
                        "owner":     owner,
                        "grantee":   "",
                        "amount":    amount,
                        "legal":     "",
                        "prop_address": address,
                        "prop_city":    "San Antonio",
                        "prop_state":   "TX",
                        "prop_zip":     clean(attrs.get("ZIP") or attrs.get("Zip") or ""),
                        "mail_address": "",
                        "mail_city":    "",
                        "mail_state":   "TX",
                        "mail_zip":     "",
                        "clerk_url": "",
                        "flags": [],
                        "score": 0,
                    })

                if len(features) < 1000:
                    break
                offset += 1000

            log.info("  Layer %d total: %d records", layer_id, 
                     sum(1 for r in all_records if r["cat"] == cat))

        return all_records


class ParcelLookup:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 BexarLeadBot/2.0"
        self.cache = {}

    def _parse_feature(self, attrs):
        situs = clean(attrs.get("Situs") or "")
        parts = situs.rsplit(",", 1)
        prop_addr = parts[0].strip() if parts else situs
        prop_city, prop_zip = "", ""
        if len(parts) > 1:
            cz = parts[1].strip().rsplit(" ", 1)
            prop_city = cz[0].strip() if cz else ""
            prop_zip  = cz[1].strip() if len(cz) > 1 else ""
        mail_ln1  = clean(attrs.get("AddrLn1") or "")
        mail_ln2  = clean(attrs.get("AddrLn2") or "")
        mail_addr = (mail_ln1 + " " + mail_ln2).strip() if mail_ln2 else mail_ln1
        return {
            "prop_address": prop_addr,
            "prop_city":    prop_city or "San Antonio",
            "prop_state":   "TX",
            "prop_zip":     prop_zip,
            "mail_address": mail_addr,
            "mail_city":    clean(attrs.get("AddrCity") or ""),
            "mail_state":   clean(attrs.get("AddrSt") or "TX"),
            "mail_zip":     clean(attrs.get("Zip") or ""),
        }

    def lookup_by_address(self, addresses):
        """Look up parcel records by property address."""
        if not addresses:
            return
        quoted = ", ".join("'" + escape_sql(a.upper()) + "'" for a in addresses[:50])
        params = {
            "where":             "UPPER(Situs) IN (" + quoted + ")",
            "outFields":         "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip",
            "returnGeometry":    "false",
            "resultRecordCount": 1000,
            "f":                 "json",
        }
        try:
            resp = self.session.get(PARCELS_URL, params=params, timeout=30)
            data = resp.json()
            for feat in data.get("features", []):
                attrs = feat.get("attributes", {})
                situs = clean(attrs.get("Situs") or "").upper()
                if situs:
                    self.cache[situs] = self._parse_feature(attrs)
        except Exception as exc:
            log.warning("Parcel address lookup: %s", exc)

    def enrich_all(self, records):
        addresses = list({r.get("prop_address","").upper()
                          for r in records if r.get("prop_address","").strip()})
        if not addresses:
            log.info("No addresses to look up.")
            return
        log.info("Parcel lookup: %d addresses", len(addresses))
        for i in range(0, len(addresses), 50):
            self.lookup_by_address(addresses[i:i+50])
            time.sleep(0.3)
        found = sum(1 for a in addresses if a in self.cache)
        log.info("Parcel matched %d/%d", found, len(addresses))

    def get_info(self, address):
        key = address.upper().strip()
        if key in self.cache:
            return self.cache[key]
        return {}


def enrich_records(records, parcel):
    enriched = []
    for r in records:
        try:
            addr_info = parcel.get_info(r.get("prop_address", ""))
            if addr_info:
                r["prop_address"] = addr_info.get("prop_address", r["prop_address"])
                r["prop_city"]    = addr_info.get("prop_city", r["prop_city"])
                r["prop_zip"]     = addr_info.get("prop_zip", r["prop_zip"])
                r["mail_address"] = addr_info.get("mail_address", "")
                r["mail_city"]    = addr_info.get("mail_city", "")
                r["mail_state"]   = addr_info.get("mail_state", "TX")
                r["mail_zip"]     = addr_info.get("mail_zip", "")
            flags  = compute_flags(r)
            score  = compute_score(r, flags)
            r["flags"] = flags
            r["score"] = score
            enriched.append(r)
        except Exception as exc:
            log.warning("Enrich: %s", exc)
            enriched.append(r)
    return enriched

def build_output(records, start_mdy, end_mdy):
    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bexar County GIS Foreclosure Map (public ArcGIS)",
        "date_range":   {"start": start_mdy, "end": end_mdy},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }

def save_json(data, *paths):
    for p in paths:
        path = Path(p)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        log.info("Saved %s (%d records)", path, data["total"])

def save_ghl_csv(records, path_str):
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            owner  = r.get("owner","")
            parts  = owner.split(",",1) if "," in owner else owner.split(" ",1)
            first  = parts[1].strip() if len(parts) > 1 else ""
            last   = parts[0].strip()
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state","TX"),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state","TX"),
                "Property Zip":           r.get("prop_zip",""),
                "Lead Type":              r.get("cat_label",r.get("cat","")),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount/Debt Owed":       r.get("amount",""),
                "Seller Score":           r.get("score",0),
                "Motivated Seller Flags": "|".join(r.get("flags",[])),
                "Source":                 "Bexar County GIS",
                "Public Records URL":     r.get("clerk_url",""),
            })
    log.info("Saved GHL CSV: %s", path)


def main():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Source: Public ArcGIS (no auth required)")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    scraper     = ForeclosureScraper()
    raw_records = scraper.fetch_all_layers()
    log.info("Total raw records: %d", len(raw_records))

    parcel = ParcelLookup()
    parcel.enrich_all(raw_records)

    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    output = build_output(records, start_mdy, end_mdy)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads (%d with address).", output["total"], output["with_address"])


if __name__ == "__main__":
    main()

