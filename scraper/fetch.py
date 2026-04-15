"""
Bexar County Motivated Seller Lead Scraper
Uses real browser cookies (authToken + authToken.sig) directly via requests.
No Playwright needed for search - bypasses all bot detection.
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
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "7"))
AUTH_TOKEN       = (os.getenv("CLERK_AUTH_TOKEN") or "").strip()
AUTH_TOKEN_SIG   = (os.getenv("CLERK_AUTH_SIG") or "").strip()
CLERK_BASE       = "https://bexar.tx.publicsearch.us"
ARCGIS_URL       = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

DOC_TYPE_MAP = {
    "LP":       ("LP",      "Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Judgment"),
    "LNCORPTX": ("LIEN",   "Corp Tax Lien"),
    "LNIRS":    ("LIEN",   "IRS Lien"),
    "LNFED":    ("LIEN",   "Federal Lien"),
    "LN":       ("LIEN",   "Lien"),
    "LNMECH":   ("LIEN",   "Mechanic Lien"),
    "LNHOA":    ("LIEN",   "HOA Lien"),
    "MEDLN":    ("LIEN",   "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate Document"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}
TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())
PAGE_LIMIT  = 200
MAX_PAGES   = 5
MAX_RETRIES = 3
RETRY_DELAY = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean(val):
    if val is None:
        return ""
    return str(val).strip()

def parse_amount(text):
    cleaned = re.sub(r"[^\d.]", "", clean(text))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def normalize_name(name):
    name = name.strip().upper()
    variants = [name]
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            last, first = parts
            variants.append(first + " " + last)
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.append(tokens[-1] + ", " + " ".join(tokens[:-1]))
    seen = []
    for v in variants:
        if v not in seen:
            seen.append(v)
    return seen

def escape_sql(s):
    return s.replace("'", "''")

def date_range_yyyymmdd(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

def date_range_mmddyyyy(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")

def is_new_this_week(filed_str):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            filed_dt = datetime.strptime(filed_str.split("T")[0], fmt)
            return (datetime.now() - filed_dt).days <= 7
        except ValueError:
            continue
    return False

def count_leading_empty(headers):
    count = 0
    for h in headers:
        if h.strip() == "":
            count += 1
        else:
            break
    return count


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def compute_flags(record):
    flags    = []
    cat      = record.get("cat", "")
    owner    = record.get("owner", "").upper()
    doc_type = record.get("doc_type", "")
    if cat == "LP":           flags.append("Lis pendens")
    if cat == "NOFC":         flags.append("Pre-foreclosure")
    if cat == "JUD":          flags.append("Judgment lien")
    if doc_type in ("LNCORPTX","LNIRS","LNFED","TAXDEED"):
                              flags.append("Tax lien")
    if doc_type == "LNMECH":  flags.append("Mechanic lien")
    if cat == "PRO":          flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS)\b", owner):
                              flags.append("LLC / corp owner")
    if is_new_this_week(record.get("filed", "")):
                              flags.append("New this week")
    return flags

def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    if "LP" in record.get("_owner_cats",[]) and "NOFC" in record.get("_owner_cats",[]):
        score += 20
    amount = record.get("amount", 0.0) or 0.0
    if amount > 100000: score += 15
    elif amount > 50000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address"): score += 5
    return min(score, 100)


# ---------------------------------------------------------------------------
# Parse SSR HTML table
# ---------------------------------------------------------------------------

def parse_results_html(html, doc_type):
    """
    Confirmed column layout after skipping leading empty cols:
    0=grantor, 1=grantee, 2=doc type, 3=recorded date, 4=doc number,
    5=book/vol/page, 6=legal description, 7=lot, 8=block,
    9=ncb, 10=county block, 11=property address
    """
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    records = []

    soup = BeautifulSoup(html, "lxml")

    # Detect bot-block page
    text = soup.get_text(" ", strip=True)
    if "out of date" in text.lower() or "update your browser" in text.lower():
        log.warning("  [HTML] Bot detection page returned")
        return records

    # Find table with grantor header
    table = None
    for t in soup.find_all("table"):
        ths   = t.find_all("th")
        htext = " ".join(th.get_text(strip=True).lower() for th in ths)
        if "grantor" in htext:
            table = t
            break

    if not table:
        snippet = text[:200].replace("\n", " ")
        log.info("  [HTML] No table found. Page: %s", snippet)
        return records

    rows = table.find_all("tr")
    if not rows:
        return records

    raw_headers = [th.get_text(strip=True).lower()
                   for th in rows[0].find_all(["th","td"])]
    skip    = count_leading_empty(raw_headers)
    headers = raw_headers[skip:]

    def ci(name):
        for i, h in enumerate(headers):
            if name in h:
                return i
        return -1

    i_grantor  = ci("grantor")
    i_grantee  = ci("grantee")
    i_date     = ci("recorded") if ci("recorded") >= 0 else ci("date")
    i_docnum   = ci("doc number") if ci("doc number") >= 0 else ci("number")
    i_legal    = ci("legal")
    i_propaddr = ci("property address") if ci("property address") >= 0 else ci("property")

    log.info("  [HTML] skip=%d headers=%s", skip, headers[:6])

    parsed = 0
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) <= skip:
            continue
        data = cells[skip:]

        def cell(idx):
            if idx < 0 or idx >= len(data):
                return ""
            return clean(data[idx].get_text(strip=True))

        grantor   = cell(i_grantor)  if i_grantor  >= 0 else ""
        grantee   = cell(i_grantee)  if i_grantee  >= 0 else ""
        filed     = cell(i_date)     if i_date     >= 0 else ""
        doc_num   = cell(i_docnum)   if i_docnum   >= 0 else ""
        legal     = cell(i_legal)    if i_legal    >= 0 else ""
        prop_addr = cell(i_propaddr) if i_propaddr >= 0 else ""

        link = row.find("a", href=True)
        clerk_url = ""
        if link:
            href = link["href"]
            if not href.startswith("http"):
                href = CLERK_BASE + href
            clerk_url = href
            if not doc_num:
                m = re.search(r"/(?:doc|instruments?)/(\w+)", href)
                if m:
                    doc_num = m.group(1)

        if not grantor and not doc_num:
            continue

        records.append({
            "doc_num":   doc_num,
            "doc_type":  doc_type,
            "filed":     filed,
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     grantor,
            "grantee":   grantee,
            "amount":    0.0,
            "legal":     legal,
            "prop_address_inline": prop_addr,
            "clerk_url": clerk_url,
        })
        parsed += 1

    log.info("  [HTML] parsed %d rows", parsed)
    return records


# ---------------------------------------------------------------------------
# Direct HTTP scraper using real browser cookies
# ---------------------------------------------------------------------------

class CookieScraper:
    """
    Uses real authToken cookies from the user's browser session.
    Makes direct HTTP requests - no browser, no bot detection.
    """

    HEADERS = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  "same-origin",
        "sec-ch-ua":       '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    def __init__(self, start_ymd, end_ymd):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd
        self.session   = requests.Session()
        self.session.headers.update(self.HEADERS)

        # Set auth cookies from GitHub Secrets
        if AUTH_TOKEN:
            self.session.cookies.set(
                "authToken", AUTH_TOKEN,
                domain="bexar.tx.publicsearch.us", path="/"
            )
            log.info("authToken set (len=%d)", len(AUTH_TOKEN))
        if AUTH_TOKEN_SIG:
            self.session.cookies.set(
                "authToken.sig", AUTH_TOKEN_SIG,
                domain="bexar.tx.publicsearch.us", path="/"
            )
            log.info("authToken.sig set (len=%d)", len(AUTH_TOKEN_SIG))

        if not AUTH_TOKEN:
            log.warning("No CLERK_AUTH_TOKEN set! Add it as a GitHub Secret.")

    def _url(self, doc_type, offset=0):
        return (
            CLERK_BASE + "/results"
            + "?department=RP"
            + "&keywordSearch=false"
            + "&limit=" + str(PAGE_LIMIT)
            + "&offset=" + str(offset)
            + "&recordedDateRange=" + self.start_ymd + "%2C" + self.end_ymd
            + "&searchOcrText=false"
            + "&searchType=docType"
            + "&searchValue=" + doc_type
        )

    def _fetch(self, url):
        """Fetch a URL with retries."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.session.headers["Referer"] = CLERK_BASE + "/"
                resp = self.session.get(url, timeout=30)
                log.info("  GET %s -> %d (%d bytes)",
                         url[-60:], resp.status_code, len(resp.content))
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code in (401, 403):
                    log.warning("  Auth error %d - cookies may be expired",
                                resp.status_code)
                    return None
                else:
                    log.warning("  Status %d", resp.status_code)
            except Exception as exc:
                log.warning("  Attempt %d: %s", attempt, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        return None

    def scrape_doc_type(self, doc_type):
        all_records = []
        offset = 0

        for page_num in range(1, MAX_PAGES + 1):
            url  = self._url(doc_type, offset=offset)
            html = self._fetch(url)

            if not html:
                break

            recs = parse_results_html(html, doc_type)
            log.info("  p%d offset=%d: %d records", page_num, offset, len(recs))

            if not recs:
                break

            all_records.extend(recs)

            if len(recs) < PAGE_LIMIT:
                break

            offset += PAGE_LIMIT
            time.sleep(0.5)  # polite delay

        return all_records

    def run(self):
        # First verify cookies work by hitting homepage
        log.info("Verifying auth cookies...")
        resp = self._fetch(CLERK_BASE + "/")
        if resp:
            if "Sign Out" in resp or "sign-out" in resp.lower():
                log.info("Cookies valid - user is logged in")
            elif "Sign In" in resp or "signin" in resp.lower():
                log.warning("Cookies appear invalid - not logged in")
            else:
                log.info("Homepage loaded (auth status unclear)")

        all_records = []
        for doc_type in TARGET_DOC_TYPES:
            log.info("Searching: %s", doc_type)
            recs = self.scrape_doc_type(doc_type)
            log.info("  Total: %d for %s", len(recs), doc_type)
            all_records.extend(recs)

        return all_records


# ---------------------------------------------------------------------------
# ArcGIS parcel lookup
# ---------------------------------------------------------------------------

class ParcelLookup:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
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

    def lookup_batch(self, owner_names):
        if not owner_names:
            return
        quoted = ", ".join("'" + escape_sql(n.upper()) + "'" for n in owner_names)
        params = {
            "where":             "UPPER(Owner) IN (" + quoted + ")",
            "outFields":         "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip",
            "returnGeometry":    "false",
            "resultRecordCount": 1000,
            "f":                 "json",
        }
        for attempt in range(2):
            try:
                resp = self.session.get(ARCGIS_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                for feat in data.get("features", []):
                    attrs = feat.get("attributes", {})
                    owner = clean(attrs.get("Owner") or "").upper()
                    if owner:
                        self.cache[owner] = self._parse_feature(attrs)
                return
            except Exception as exc:
                log.warning("ArcGIS %d: %s", attempt+1, exc)
                time.sleep(3)

    def enrich_all(self, records):
        owners = list({r.get("owner","").upper()
                       for r in records if r.get("owner","").strip()})
        if not owners:
            log.info("No owners to look up.")
            return
        log.info("ArcGIS: %d owners", len(owners))
        for i in range(0, len(owners), 50):
            self.lookup_batch(owners[i:i+50])
            time.sleep(0.3)
        found = sum(1 for o in owners if o in self.cache)
        log.info("ArcGIS matched %d/%d", found, len(owners))

    def get_address(self, owner):
        key = owner.upper().strip()
        if key in self.cache:
            return self.cache[key]
        for v in normalize_name(owner):
            if v in self.cache:
                return self.cache[v]
        return {}


# ---------------------------------------------------------------------------
# Enrichment + Output
# ---------------------------------------------------------------------------

def enrich_records(records, parcel):
    owner_cats = {}
    for r in records:
        o = r.get("owner","").upper()
        if o:
            owner_cats.setdefault(o,[]).append(r.get("cat",""))
    enriched = []
    for r in records:
        try:
            owner     = r.get("owner","")
            addr_info = parcel.get_address(owner)
            inline    = r.pop("prop_address_inline", "")
            r["prop_address"] = addr_info.get("prop_address","") or inline
            r["prop_city"]    = addr_info.get("prop_city","San Antonio")
            r["prop_state"]   = addr_info.get("prop_state","TX")
            r["prop_zip"]     = addr_info.get("prop_zip","")
            r["mail_address"] = addr_info.get("mail_address","")
            r["mail_city"]    = addr_info.get("mail_city","")
            r["mail_state"]   = addr_info.get("mail_state","TX")
            r["mail_zip"]     = addr_info.get("mail_zip","")
            r["_owner_cats"]  = owner_cats.get(owner.upper(),[])
            flags  = compute_flags(r)
            score  = compute_score(r, flags)
            r["flags"] = flags
            r["score"] = score
            del r["_owner_cats"]
            enriched.append(r)
        except Exception as exc:
            log.warning("Enrich: %s", exc)
            enriched.append(r)
    return enriched

def build_output(records, start_mdy, end_mdy):
    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bexar County Clerk / Bexar County GIS",
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
                "Source":                 "Bexar County Clerk",
                "Public Records URL":     r.get("clerk_url",""),
            })
    log.info("Saved GHL CSV: %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days | Pages/type: %d x %d",
             LOOKBACK_DAYS, MAX_PAGES, PAGE_LIMIT)
    log.info("authToken set: %s | authToken.sig set: %s",
             bool(AUTH_TOKEN), bool(AUTH_TOKEN_SIG))
    log.info("=" * 60)

    start_ymd, end_ymd = date_range_yyyymmdd(LOOKBACK_DAYS)
    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    scraper     = CookieScraper(start_ymd, end_ymd)
    raw_records = scraper.run()
    log.info("Total raw records: %d", len(raw_records))

    parcel = ParcelLookup()
    parcel.enrich_all(raw_records)

    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score",0), reverse=True)

    output = build_output(records, start_mdy, end_mdy)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads (%d with address).", output["total"], output["with_address"])


if __name__ == "__main__":
    main()

