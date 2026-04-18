"""
Bexar County Motivated Seller Lead Scraper
Source: maps.bexar.org ArcGIS MapServer (public, no auth)
Pushes new leads to GoHighLevel (Jarvis) CRM automatically.
"""

import json
import logging
import os
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

# GHL config from GitHub Secrets
GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_json(url):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 BexarLeadScraper/2.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def arcgis_query(layer_url, where, fields="*", offset=0, limit=1000):
    params = urllib.parse.urlencode({
        "where": where, "outFields": fields, "returnGeometry": "false",
        "resultOffset": offset, "resultRecordCount": limit, "f": "json",
    })
    data = fetch_json(f"{layer_url}/query?{params}")
    if "error" in data:
        log.warning(f"ArcGIS error: {data['error']}")
        return []
    return data.get("features", [])


def fetch_all(layer_url, where="1=1"):
    records, offset = [], 0
    while True:
        batch = arcgis_query(layer_url, where, offset=offset)
        records.extend(batch)
        log.info(f"    offset={offset}: got {len(batch)} (total: {len(records)})")
        if len(batch) < 1000:
            break
        offset += len(batch)
    return records


def pick(attrs, *candidates, default=""):
    for c in candidates:
        v = attrs.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>"):
            return str(v).strip()
    return default


def score_record(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("type") == "TAX": s += 3
    if rec.get("owner"):         s += 1
    s += len(rec.get("flags", []))
    return min(s, 10)


# ── GHL Integration ───────────────────────────────────────────────────────────
def ghl_request(method, endpoint, payload=None):
    """Make a GHL API v2 request."""
    url  = f"{GHL_API_BASE}{endpoint}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    req  = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "Version":       "2021-07-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        log.warning(f"GHL {method} {endpoint} → HTTP {e.code}: {body[:200]}")
        return None
    except Exception as e:
        log.warning(f"GHL request failed: {e}")
        return None


def ghl_search_contact(doc_number):
    """Search GHL for existing contact by doc number tag."""
    result = ghl_request(
        "GET",
        f"/contacts/?locationId={GHL_LOCATION_ID}&query={urllib.parse.quote(doc_number)}&limit=1"
    )
    if result and result.get("contacts"):
        return result["contacts"][0]
    return None


def ghl_create_contact(rec):
    """Create a new GHL contact from a lead record."""
    lead_type  = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
    tags       = ["bexar-lead", rec["type"]]
    if rec.get("score", 0) >= 7:
        tags.append("hot-lead")

    # Build name from owner or address
    name = rec.get("owner") or rec.get("address") or "Unknown Owner"

    payload = {
        "locationId":  GHL_LOCATION_ID,
        "firstName":   name,
        "lastName":    "",
        "name":        name,
        "address1":    rec.get("address", ""),
        "city":        rec.get("city", ""),
        "postalCode":  rec.get("zip", ""),
        "state":       "TX",
        "country":     "US",
        "tags":        tags,
        "source":      "Bexar County Scraper",
        "customFields": [
            {"key": "lead_type",    "field_value": lead_type},
            {"key": "doc_number",   "field_value": rec.get("doc_number", "")},
            {"key": "date_filed",   "field_value": rec.get("date_filed", "")},
            {"key": "score",        "field_value": str(rec.get("score", 0))},
            {"key": "school_dist",  "field_value": rec.get("school_dist", "")},
            {"key": "property_address", "field_value": rec.get("address", "")},
        ],
    }

    result = ghl_request("POST", "/contacts/", payload)
    return result


def push_to_ghl(records):
    """Push all new leads to GHL, skip duplicates by doc number."""
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set — skipping GHL push")
        return 0

    log.info(f"Pushing leads to GHL (location: {GHL_LOCATION_ID})...")
    created = 0
    skipped = 0
    errors  = 0

    for i, rec in enumerate(records):
        doc = rec.get("doc_number", "")
        if not doc:
            skipped += 1
            continue

        # Check if already exists
        existing = ghl_search_contact(doc)
        if existing:
            skipped += 1
            continue

        # Create new contact
        result = ghl_create_contact(rec)
        if result and result.get("contact"):
            created += 1
            log.info(f"  [{i+1}/{len(records)}] Created: {rec.get('address','?')} (doc: {doc})")
        else:
            errors += 1
            log.warning(f"  [{i+1}/{len(records)}] Failed:  {rec.get('address','?')} (doc: {doc})")

    log.info(f"GHL push complete — Created: {created}, Skipped: {skipped}, Errors: {errors}")
    return created


# ── Main scraper ──────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper v3")
    log.info(f"Source: {BASE}")
    log.info("=" * 60)

    raw = []

    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{BASE}/{idx}"
        log.info(f"Fetching layer {idx} ({layer['label']})...")

        try:
            meta   = fetch_json(f"{layer_url}?f=json")
            fields = [f["name"] for f in meta.get("fields", [])]
            log.info(f"  Fields: {fields}")
        except Exception as e:
            log.warning(f"  Metadata fetch failed: {e}")

        try:
            features = fetch_all(layer_url)
            log.info(f"  Layer {idx} total: {len(features)} records")

            if features:
                log.info(f"  Sample: {dict(list(features[0]['attributes'].items())[:6])}")

            for feat in features:
                a = feat["attributes"]
                addr  = pick(a, "ADDRESS", "SITUS_ADD", "ADDR", "PROPERTY_ADDRESS", "SITE_ADDR")
                owner = pick(a, "OWNER", "GRANTOR", "OWNER_NAME", "GRANTORNAME", "DEBTOR", "TAXPAYER")
                doc   = pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM", "DOCUMENT_NUMBER", "CASENUM")
                year  = pick(a, "YEAR",  "YR",   "SALE_YEAR",  default="")
                month = pick(a, "MONTH", "MO",   "SALE_MONTH", default="")
                city  = pick(a, "CITY",  "MAIL_CITY", "PROP_CITY", default="")
                zip_  = pick(a, "ZIP",   "ZIPCODE",   "ZIP_CODE",  "MAIL_ZIP", default="")
                sdist = pick(a, "SCHOOL_DIST", "SCHOOL_DISTRICT", default="")

                raw.append({
                    "type":        layer["type"],
                    "address":     addr,
                    "owner":       owner,
                    "doc_number":  doc,
                    "year":        year,
                    "month":       month,
                    "city":        city,
                    "zip":         zip_,
                    "school_dist": sdist,
                    "date_filed":  f"{month}/{year}" if (month and year) else "",
                    "flags":       [],
                })

        except Exception as e:
            log.error(f"  Layer {idx} failed: {e}", exc_info=True)

    log.info(f"Total raw records: {len(raw)}")

    records = []
    for r in raw:
        if r["type"] == "TAX":
            r["flags"].append("TAX FORE")
        if not r["owner"]:
            r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]:
            r["flags"].append("NO CITY")
        r["score"] = score_record(r)
        records.append(r)

    records.sort(key=lambda x: x["score"], reverse=True)
    addr_count = sum(1 for r in records if r["address"])
    log.info(f"Done. {len(records)} leads ({addr_count} with address).")
    return records


# ── Dashboard HTML template ───────────────────────────────────────────────────
DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Bexar County Motivated Seller Leads</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap" rel="stylesheet"/>
<style>
:root {
  --bg:#0d0f14; --surface:#13161e; --surface2:#1a1e2a; --border:#252836;
  --accent:#00e5ff; --accent3:#a78bfa; --text:#e8eaf0; --muted:#6b7280;
  --success:#22d3a5; --warning:#fbbf24; --danger:#f87171;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:'DM Mono',monospace; font-size:13px; min-height:100vh; }
header { display:flex; align-items:center; justify-content:space-between; padding:18px 32px; border-bottom:1px solid var(--border); background:var(--surface); position:sticky; top:0; z-index:100; }
.logo { font-family:'Syne',sans-serif; font-size:20px; font-weight:800; }
.logo span { color:var(--accent); }
#last-updated { color:var(--muted); font-size:11px; }
.stats { display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:var(--border); border-bottom:1px solid var(--border); }
.stat-card { background:var(--surface); padding:24px 28px; display:flex; flex-direction:column; gap:6px; }
.stat-num { font-family:'Syne',sans-serif; font-size:36px; font-weight:800; line-height:1; color:var(--accent); }
.stat-card:nth-child(2) .stat-num { color:var(--danger); }
.stat-card:nth-child(3) .stat-num { color:var(--warning); }
.stat-card:nth-child(4) .stat-num { color:var(--success); }
.stat-label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:1px; }
.controls { display:flex; gap:10px; padding:16px 32px; background:var(--surface); border-bottom:1px solid var(--border); align-items:center; flex-wrap:wrap; }
input[type=text] { flex:1; min-width:200px; background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:8px 14px; font-family:'DM Mono',monospace; font-size:13px; outline:none; transition:border-color .2s; }
input[type=text]:focus { border-color:var(--accent); }
select { background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:8px 12px; font-family:'DM Mono',monospace; font-size:13px; cursor:pointer; outline:none; }
.count-badge { color:var(--muted); font-size:11px; white-space:nowrap; padding:0 8px; }
.table-wrap { overflow-x:auto; padding:0 32px 32px; }
table { width:100%; border-collapse:collapse; margin-top:16px; }
thead th { text-align:left; padding:10px 12px; font-size:10px; text-transform:uppercase; letter-spacing:1.2px; color:var(--muted); border-bottom:1px solid var(--border); white-space:nowrap; }
tbody tr { border-bottom:1px solid var(--border); transition:background .12s; }
tbody tr:hover { background:var(--surface2); }
tbody td { padding:10px 12px; vertical-align:middle; }
.score { display:inline-flex; width:36px; height:36px; border-radius:50%; align-items:center; justify-content:center; font-weight:500; font-size:12px; font-family:'Syne',sans-serif; }
.score-high { background:rgba(34,211,165,.15); color:var(--success); border:1px solid rgba(34,211,165,.3); }
.score-mid  { background:rgba(251,191,36,.15);  color:var(--warning); border:1px solid rgba(251,191,36,.3); }
.score-low  { background:rgba(248,113,113,.15); color:var(--danger);  border:1px solid rgba(248,113,113,.3); }
.type-badge { display:inline-block; padding:2px 8px; font-size:10px; font-weight:500; border-radius:2px; text-transform:uppercase; letter-spacing:.5px; white-space:nowrap; }
.type-nof { background:rgba(248,113,113,.15); color:var(--danger);  border:1px solid rgba(248,113,113,.25); }
.type-tax { background:rgba(251,191,36,.15);  color:var(--warning); border:1px solid rgba(251,191,36,.25); }
.flags { display:flex; gap:4px; flex-wrap:wrap; }
.flag { display:inline-block; padding:2px 6px; font-size:10px; background:rgba(167,139,250,.12); color:var(--accent3); border:1px solid rgba(167,139,250,.25); border-radius:2px; white-space:nowrap; }
.addr { color:var(--text); font-size:12px; max-width:220px; }
.owner, .city, .doc { color:var(--muted); font-size:12px; }
.state-msg { text-align:center; padding:60px 20px; color:var(--muted); }
.pagination { display:flex; justify-content:center; align-items:center; gap:8px; padding:20px 32px; color:var(--muted); font-size:12px; }
.pagination button { background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:6px 14px; cursor:pointer; font-family:'DM Mono',monospace; font-size:12px; }
.pagination button:hover:not(:disabled) { border-color:var(--accent); color:var(--accent); }
.pagination button:disabled { opacity:.3; cursor:default; }
@media(max-width:900px) { .stats { grid-template-columns:repeat(2,1fr); } .controls, .table-wrap { padding-left:16px; padding-right:16px; } }
</style>
</head>
<body>
<header>
  <div class="logo">🏠 Bexar County <span>Leads</span></div>
  <div id="last-updated">UPDATED_PLACEHOLDER</div>
</header>
<div class="stats">
  <div class="stat-card"><div class="stat-num" id="s-total">—</div><div class="stat-label">Total Leads</div></div>
  <div class="stat-card"><div class="stat-num" id="s-nof">—</div><div class="stat-label">Foreclosures (NOF)</div></div>
  <div class="stat-card"><div class="stat-num" id="s-tax">—</div><div class="stat-label">Tax Foreclosures</div></div>
  <div class="stat-card"><div class="stat-num" id="s-addr">—</div><div class="stat-label">With Address</div></div>
</div>
<div class="controls">
  <input type="text" id="search" placeholder="Search address, owner, doc #…" oninput="applyFilters()"/>
  <select id="type-filter" onchange="applyFilters()">
    <option value="">All Types</option>
    <option value="NOF">Foreclosure (NOF)</option>
    <option value="TAX">Tax Foreclosure</option>
  </select>
  <select id="sort-select" onchange="applyFilters()">
    <option value="score-desc">Sort: Score ↓</option>
    <option value="score-asc">Sort: Score ↑</option>
    <option value="date-desc">Sort: Date ↓</option>
    <option value="date-asc">Sort: Date ↑</option>
  </select>
  <span class="count-badge" id="count-badge"></span>
</div>
<div class="table-wrap">
  <table>
    <thead><tr>
      <th>Score</th><th>Type</th><th>Property Address</th>
      <th>Owner</th><th>Date Filed</th><th>Doc #</th><th>City/ZIP</th><th>Flags</th>
    </tr></thead>
    <tbody id="tbody"></tbody>
  </table>
  <div id="state-msg" class="state-msg" style="display:none">No records match your filters.</div>
</div>
<div class="pagination">
  <button id="btn-prev" onclick="changePage(-1)">← Prev</button>
  <span id="page-info"></span>
  <button id="btn-next" onclick="changePage(1)">Next →</button>
</div>
<script>
var ALL_RECORDS = DATA_PLACEHOLDER;
var filtered = [], page = 1, PAGE = 50;

function init() {
  document.getElementById('s-total').textContent = ALL_RECORDS.length;
  document.getElementById('s-nof').textContent   = ALL_RECORDS.filter(function(r){return r.type==='NOF';}).length;
  document.getElementById('s-tax').textContent   = ALL_RECORDS.filter(function(r){return r.type==='TAX';}).length;
  document.getElementById('s-addr').textContent  = ALL_RECORDS.filter(function(r){return r.address;}).length;
  applyFilters();
}

function applyFilters() {
  var q = document.getElementById('search').value.toLowerCase();
  var t = document.getElementById('type-filter').value;
  var s = document.getElementById('sort-select').value;
  filtered = ALL_RECORDS.filter(function(r) {
    var mq = !q || (r.address||'').toLowerCase().indexOf(q)>=0
                || (r.owner||'').toLowerCase().indexOf(q)>=0
                || (r.doc_number||'').toLowerCase().indexOf(q)>=0;
    var mt = !t || r.type === t;
    return mq && mt;
  });
  filtered.sort(function(a,b) {
    if (s==='score-desc') return b.score - a.score;
    if (s==='score-asc')  return a.score - b.score;
    if (s==='date-desc')  return (b.date_filed||'') > (a.date_filed||'') ? 1 : -1;
    if (s==='date-asc')   return (a.date_filed||'') > (b.date_filed||'') ? 1 : -1;
    return 0;
  });
  page = 1;
  document.getElementById('count-badge').textContent = filtered.length + ' of ' + ALL_RECORDS.length + ' leads';
  render();
}

function render() {
  var tbody = document.getElementById('tbody');
  var msg   = document.getElementById('state-msg');
  var slice = filtered.slice((page-1)*PAGE, page*PAGE);
  if (!filtered.length) { tbody.innerHTML=''; msg.style.display='block'; return; }
  msg.style.display = 'none';
  var rows = '';
  for (var i=0; i<slice.length; i++) {
    var r = slice[i];
    var sc = r.score || 0;
    var scClass = sc>=7 ? 'score-high' : sc>=4 ? 'score-mid' : 'score-low';
    var tClass  = r.type==='TAX' ? 'type-tax' : 'type-nof';
    var tLabel  = r.type==='TAX' ? 'TAX FORE' : 'NOF';
    var cityzip = [r.city, r.zip].filter(Boolean).join(' ') || '—';
    var flags   = (r.flags||[]);
    var flagsHtml = '';
    for (var j=0; j<flags.length; j++) flagsHtml += '<span class="flag">'+flags[j]+'</span>';
    if (!flagsHtml) flagsHtml = '<span style="color:var(--muted)">—</span>';
    rows += '<tr>'
      + '<td><div class="score '+scClass+'">'+sc+'</div></td>'
      + '<td><span class="type-badge '+tClass+'">'+tLabel+'</span></td>'
      + '<td><div class="addr">'+(r.address||'—')+'</div></td>'
      + '<td><div class="owner">'+(r.owner||'—')+'</div></td>'
      + '<td><div class="doc">'+(r.date_filed||'—')+'</div></td>'
      + '<td><div class="doc">'+(r.doc_number||'—')+'</div></td>'
      + '<td><div class="city">'+cityzip+'</div></td>'
      + '<td><div class="flags">'+flagsHtml+'</div></td>'
      + '</tr>';
  }
  tbody.innerHTML = rows;
  var total = Math.ceil(filtered.length / PAGE);
  document.getElementById('page-info').textContent = total>1 ? 'Page '+page+' of '+total : '';
  document.getElementById('btn-prev').disabled = page <= 1;
  document.getElementById('btn-next').disabled = page >= total;
}

function changePage(d) { page+=d; render(); window.scrollTo({top:0,behavior:'smooth'}); }
init();
</script>
</body>
</html>"""


def build_dashboard(records):
    updated  = datetime.now(timezone.utc).strftime("Updated: %b %d, %Y %H:%M UTC")
    json_str = json.dumps(records, separators=(",", ":"), ensure_ascii=False)

    html = DASHBOARD_TEMPLATE
    html = html.replace("UPDATED_PLACEHOLDER", updated, 1)
    html = html.replace("DATA_PLACEHOLDER", json_str, 1)

    if "DATA_PLACEHOLDER" in html:
        raise RuntimeError("Data injection failed!")

    os.makedirs("dashboard", exist_ok=True)
    out_path = "dashboard/index.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    size = os.path.getsize(out_path)
    log.info(f"Built {out_path} — {len(records)} records, {size:,} bytes")
    if size < 50000 and len(records) > 0:
        raise RuntimeError(f"Output file too small ({size} bytes)!")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    records = run()

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved data/records.json ({len(records)} records)")

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved dashboard/records.json ({len(records)} records)")

    build_dashboard(records)

    # Push to GHL
    push_to_ghl(records)

