#!/usr/bin/env python3
"""
Bexar County Motivated Seller Lead Scraper
Fetches foreclosure data from ArcGIS, enriches it, and
bakes a fully self-contained dashboard/index.html with the data embedded.
"""

import json
import logging
import os
import math
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = 7
ARCGIS_BASE   = "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services"

LAYERS = [
    {"url": f"{ARCGIS_BASE}/Bexar_County_Foreclosures/FeatureServer/0", "type": "NOF",  "label": "Notice of Foreclosure / Mortgage"},
    {"url": f"{ARCGIS_BASE}/Bexar_County_Foreclosures/FeatureServer/1", "type": "TAX",  "label": "Tax Foreclosure"},
]

PARCEL_URL = "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/Bexar_Parcels/FeatureServer/0"

# ── Helpers ──────────────────────────────────────────────────────────────────
def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "BexarLeadScraper/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def arcgis_query(layer_url: str, where: str, fields: str = "*", offset: int = 0, limit: int = 1000) -> list:
    params = urllib.parse.urlencode({
        "where":         where,
        "outFields":     fields,
        "returnGeometry":"false",
        "resultOffset":  offset,
        "resultRecordCount": limit,
        "f":             "json",
    })
    data = fetch_json(f"{layer_url}/query?{params}")
    return data.get("features", [])


def fetch_all(layer_url: str, where: str, fields: str = "*") -> list:
    records, offset = [], 0
    while True:
        batch = arcgis_query(layer_url, where, fields, offset=offset)
        records.extend(batch)
        if len(batch) < 1000:
            break
        offset += len(batch)
    return records


def score_record(rec: dict) -> int:
    s = 0
    if rec.get("address"):           s += 3
    if rec.get("type") == "TAX":     s += 3
    if rec.get("owner"):             s += 1
    flags = rec.get("flags", [])
    s += len(flags)
    return min(s, 10)


# ── Main scraper ─────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Source: Public ArcGIS (no auth required)")
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info("=" * 60)

    now      = datetime.now(timezone.utc)
    cutoff   = now - timedelta(days=LOOKBACK_DAYS)
    # ArcGIS date filter (epoch ms)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    date_str  = cutoff.strftime("%Y/%m/%d")
    log.info(f"Date range: {date_str} to {now.strftime('%m/%d/%Y')}")

    raw = []

    for i, layer in enumerate(LAYERS):
        log.info(f"Fetching foreclosure layer {i} ({layer['label']})...")
        try:
            # Try date filter first, fall back to 1=1 if no DATE field
            where = f"YEAR >= {cutoff.year} AND MONTH >= {cutoff.month}"
            features = fetch_all(layer["url"], where)
            if not features:
                features = fetch_all(layer["url"], "1=1")

            log.info(f"  Layer {i} offset=0: {len(features)} features")

            if features:
                sample = list(features[0]["attributes"].keys())
                log.info(f"  Sample fields: {sample}")
                attrs = features[0]["attributes"]
                owner_val = attrs.get("OWNER", attrs.get("GRANTOR", ""))
                addr_val  = attrs.get("ADDRESS", attrs.get("SITUS_ADD", ""))
                log.info(f"  Sample values: owner={owner_val} addr={addr_val} date=")

            log.info(f"  Layer {i} total: {len(features)} records")

            for feat in features:
                a = feat["attributes"]
                addr = (a.get("ADDRESS") or a.get("SITUS_ADD") or a.get("ADDR") or "").strip()
                raw.append({
                    "type":        layer["type"],
                    "address":     addr,
                    "owner":       (a.get("OWNER") or a.get("GRANTOR") or "").strip(),
                    "doc_number":  str(a.get("DOC_NUMBER") or a.get("DOCNUM") or ""),
                    "year":        a.get("YEAR", ""),
                    "month":       a.get("MONTH", ""),
                    "school_dist": a.get("SCHOOL_DIST", ""),
                    "city":        (a.get("CITY") or "").strip(),
                    "zip":         str(a.get("ZIP") or "").strip(),
                    "date_filed":  f"{a.get('MONTH','')}/{a.get('YEAR','')}".strip("/"),
                    "flags":       [],
                })

        except Exception as e:
            log.warning(f"  Layer {i} error: {e}")

    log.info(f"Total raw records: {len(raw)}")

    # ── Parcel lookup ──────────────────────────────────────────────────────
    addresses = [r["address"] for r in raw if r["address"]]
    log.info(f"Parcel lookup: {len(addresses)} addresses")

    parcel_map = {}
    try:
        for i in range(0, min(len(addresses), 400), 50):
            batch = addresses[i:i+50]
            escaped = [a.replace("'", "''") for a in batch]
            quoted  = ", ".join(f"'{a}'" for a in escaped)
            where   = f"SITUS_ADD IN ({quoted})"
            feats   = arcgis_query(PARCEL_URL, where, "SITUS_ADD,OWNER_NAME,MAIL_CITY,MAIL_ZIP", limit=200)
            for f in feats:
                a = f["attributes"]
                key = (a.get("SITUS_ADD") or "").strip().upper()
                if key:
                    parcel_map[key] = a
        log.info(f"Parcel matched {len(parcel_map)}/{len(addresses)}")
    except Exception as e:
        log.warning(f"Parcel lookup failed: {e}")

    # ── Enrich + Score ─────────────────────────────────────────────────────
    records = []
    for r in raw:
        key = r["address"].upper()
        parcel = parcel_map.get(key, {})
        if parcel:
            if not r["owner"]: r["owner"] = parcel.get("OWNER_NAME", "")
            if not r["city"]:  r["city"]  = parcel.get("MAIL_CITY", "")
            if not r["zip"]:   r["zip"]   = str(parcel.get("MAIL_ZIP", ""))

        # Flags
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")

        r["score"] = score_record(r)
        records.append(r)

    records.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"Saved dashboard/records.json ({len(records)} records)")
    log.info(f"Saved data/records.json ({len(records)} records)")
    log.info(f"Done. {len(records)} leads ({sum(1 for r in records if r['address'])} with address).")

    return records


# ── Dashboard builder ─────────────────────────────────────────────────────────
def build_dashboard(records: list):
    """Write dashboard/index.html with records baked in as JS."""

    json_data = json.dumps(records, separators=(",", ":"))
    updated   = datetime.now(timezone.utc).strftime("%b %d, %Y %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>🏠 Bexar County Motivated Seller Leads</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap" rel="stylesheet"/>
<style>
:root{{
  --bg:#0d0f14;--surface:#13161e;--surface2:#1a1e2a;--border:#252836;
  --accent:#00e5ff;--accent2:#ff6b35;--accent3:#a78bfa;--text:#e8eaf0;
  --muted:#6b7280;--success:#22d3a5;--warning:#fbbf24;--danger:#f87171;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'DM Mono',monospace;font-size:13px;min-height:100vh}}
header{{display:flex;align-items:center;justify-content:space-between;padding:18px 32px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:100}}
.logo{{font-family:'Syne',sans-serif;font-size:20px;font-weight:800;letter-spacing:-0.5px}}
.logo span{{color:var(--accent)}}
#last-updated{{color:var(--muted);font-size:11px}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border);border-bottom:1px solid var(--border)}}
.stat-card{{background:var(--surface);padding:24px 28px;display:flex;flex-direction:column;gap:6px}}
.stat-num{{font-family:'Syne',sans-serif;font-size:36px;font-weight:800;line-height:1;color:var(--accent)}}
.stat-card:nth-child(2) .stat-num{{color:var(--danger)}}
.stat-card:nth-child(3) .stat-num{{color:var(--warning)}}
.stat-card:nth-child(4) .stat-num{{color:var(--success)}}
.stat-label{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:1px}}
.controls{{display:flex;gap:10px;padding:16px 32px;background:var(--surface);border-bottom:1px solid var(--border);align-items:center}}
input[type=text]{{flex:1;background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 14px;font-family:'DM Mono',monospace;font-size:13px;outline:none;transition:border-color .2s}}
input[type=text]:focus{{border-color:var(--accent)}}
select{{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 12px;font-family:'DM Mono',monospace;font-size:13px;cursor:pointer;outline:none}}
.count-badge{{color:var(--muted);font-size:11px;white-space:nowrap;padding:0 8px}}
.table-wrap{{overflow-x:auto;padding:0 32px 32px}}
table{{width:100%;border-collapse:collapse;margin-top:16px}}
thead th{{text-align:left;padding:10px 12px;font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);border-bottom:1px solid var(--border);cursor:pointer;white-space:nowrap;user-select:none}}
thead th:hover{{color:var(--accent)}}
tbody tr{{border-bottom:1px solid var(--border);transition:background .12s}}
tbody tr:hover{{background:var(--surface2)}}
tbody td{{padding:10px 12px;vertical-align:middle}}
.score{{display:inline-flex;width:36px;height:36px;border-radius:50%;align-items:center;justify-content:center;font-weight:500;font-size:12px;font-family:'Syne',sans-serif}}
.score-high{{background:rgba(34,211,165,.15);color:var(--success);border:1px solid rgba(34,211,165,.3)}}
.score-mid{{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.3)}}
.score-low{{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.3)}}
.type-badge{{display:inline-block;padding:2px 8px;font-size:10px;font-weight:500;border-radius:2px;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}}
.type-nof{{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.25)}}
.type-tax{{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.25)}}
.flags{{display:flex;gap:4px;flex-wrap:wrap}}
.flag{{display:inline-block;padding:2px 6px;font-size:10px;background:rgba(167,139,250,.12);color:var(--accent3);border:1px solid rgba(167,139,250,.25);border-radius:2px;white-space:nowrap}}
.addr{{color:var(--text);font-size:12px;max-width:220px}}
.owner,.city,.doc{{color:var(--muted);font-size:12px}}
.state-msg{{text-align:center;padding:60px 20px;color:var(--muted)}}
.pagination{{display:flex;justify-content:center;align-items:center;gap:8px;padding:20px 32px;color:var(--muted);font-size:12px}}
.pagination button{{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:6px 14px;cursor:pointer;font-family:'DM Mono',monospace;font-size:12px;transition:border-color .15s}}
.pagination button:hover:not(:disabled){{border-color:var(--accent);color:var(--accent)}}
.pagination button:disabled{{opacity:.3;cursor:default}}
@media(max-width:900px){{.stats{{grid-template-columns:repeat(2,1fr)}}.controls,.table-wrap{{padding-left:16px;padding-right:16px}}}}
</style>
</head>
<body>
<header>
  <div class="logo">🏠 Bexar County <span>Leads</span></div>
  <div id="last-updated">Updated: {updated}</div>
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
// DATA IS BAKED IN — no fetch() needed
const ALL_RECORDS = {json_data};

let filtered=[], page=1;
const PAGE=50;

function init(){{
  const nof=ALL_RECORDS.filter(r=>r.type==='NOF').length;
  const tax=ALL_RECORDS.filter(r=>r.type==='TAX').length;
  const adr=ALL_RECORDS.filter(r=>r.address).length;
  document.getElementById('s-total').textContent=ALL_RECORDS.length;
  document.getElementById('s-nof').textContent=nof;
  document.getElementById('s-tax').textContent=tax;
  document.getElementById('s-addr').textContent=adr;
  applyFilters();
}}

function applyFilters(){{
  const q=document.getElementById('search').value.toLowerCase();
  const t=document.getElementById('type-filter').value;
  const s=document.getElementById('sort-select').value;
  filtered=ALL_RECORDS.filter(r=>{{
    const mq=!q||(r.address||'').toLowerCase().includes(q)||(r.owner||'').toLowerCase().includes(q)||(r.doc_number||'').toLowerCase().includes(q);
    const mt=!t||r.type===t;
    return mq&&mt;
  }});
  filtered.sort((a,b)=>{{
    if(s==='score-desc') return b.score-a.score;
    if(s==='score-asc')  return a.score-b.score;
    if(s==='date-desc')  return (b.date_filed||'')>(a.date_filed||'')?1:-1;
    if(s==='date-asc')   return (a.date_filed||'')>(b.date_filed||'')?1:-1;
    return 0;
  }});
  page=1;
  document.getElementById('count-badge').textContent=filtered.length+' of '+ALL_RECORDS.length+' leads';
  render();
}}

function render(){{
  const tbody=document.getElementById('tbody');
  const msg=document.getElementById('state-msg');
  const slice=filtered.slice((page-1)*PAGE, page*PAGE);
  if(!filtered.length){{tbody.innerHTML='';msg.style.display='block';return;}}
  msg.style.display='none';
  tbody.innerHTML=slice.map(r=>{{
    const sc=r.score||0;
    const scClass=sc>=7?'score-high':sc>=4?'score-mid':'score-low';
    const tClass=r.type==='TAX'?'type-tax':'type-nof';
    const tLabel=r.type==='TAX'?'TAX FORE':'NOF';
    const cityzip=[r.city,r.zip].filter(Boolean).join(' ')||'—';
    const flagsHtml=(r.flags||[]).map(f=>`<span class="flag">${{f}}</span>`).join('')||'<span style="color:var(--muted)">—</span>';
    return `<tr>
      <td><div class="score ${{scClass}}">${{sc}}</div></td>
      <td><span class="type-badge ${{tClass}}">${{tLabel}}</span></td>
      <td><div class="addr">${{r.address||'—'}}</div></td>
      <td><div class="owner">${{r.owner||'—'}}</div></td>
      <td><div class="doc">${{r.date_filed||'—'}}</div></td>
      <td><div class="doc">${{r.doc_number||'—'}}</div></td>
      <td><div class="city">${{cityzip}}</div></td>
      <td><div class="flags">${{flagsHtml}}</div></td>
    </tr>`;
  }}).join('');
  const total=Math.ceil(filtered.length/PAGE);
  document.getElementById('page-info').textContent=total>1?`Page ${{page}} of ${{total}}`:'';
  document.getElementById('btn-prev').disabled=page<=1;
  document.getElementById('btn-next').disabled=page>=total;
}}

function changePage(d){{page+=d;render();window.scrollTo({{top:0,behavior:'smooth'}});}}
init();
</script>
</body>
</html>"""

    os.makedirs("dashboard", exist_ok=True)
    with open("dashboard/index.html", "w") as f:
        f.write(html)
    log.info(f"Built dashboard/index.html with {len(records)} records baked in")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    records = run()

    # Save JSON files
    with open("data/records.json", "w") as f:
        json.dump(records, f, indent=2)

    with open("dashboard/records.json", "w") as f:
        json.dump(records, f, indent=2)

    # Build self-contained dashboard
    build_dashboard(records)

