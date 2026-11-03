"""Counts-only stats for the public website page (trustedhomebuyersa.com/bexar-county-foreclosure-auctions).
Writes dashboard/site-stats.json: per upcoming sale date, the number of unique Notice-of-Foreclosure-Sale properties
and the top ZIP codes. NO names, addresses, phones or notes. Runs at deploy time (see scrape.yml); safe to fail.
Past sales are frozen into "history" (read back from the last deployed file) because past leads get purged."""
import json, os, sys, datetime, collections, urllib.request
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, 'dashboard', 'records.json'); OUT = os.path.join(ROOT, 'dashboard', 'site-stats.json')
PUBLIC = 'https://e4property.github.io/bexar-leads/site-stats.json'
today = datetime.date.today()

def parse(sd):
    try:
        m, d, y = (sd or '').split('/'); return datetime.date(int(y), int(m), int(d))
    except Exception:
        return None

recs = json.load(open(SRC, encoding='utf-8'))
by = collections.defaultdict(list)
for r in recs:
    if r.get('type') != 'NOF' or r.get('duplicate'):
        continue
    d = parse(r.get('sale_date'))
    if d and today - datetime.timedelta(days=60) <= d <= today + datetime.timedelta(days=75):
        by[d].append(r)

try:
    old = json.load(urllib.request.urlopen(urllib.request.Request(PUBLIC, headers={'Cache-Control': 'no-cache'}), timeout=20))
except Exception:
    old = {}
history = dict(old.get('history') or {})
for k, v in (old.get('sales') or {}).items():          # freeze counts of sales that have now passed
    kd = datetime.date.fromisoformat(k)
    if kd < today and k not in history:
        history[k] = v.get('count')

sales = {}
for d, rs in sorted(by.items()):
    if len(rs) < 5:                                     # ignore stray one-off dates (typos like 2062)
        continue
    z = collections.Counter((r.get('zip') or '')[:5] for r in rs if r.get('zip'))
    key = d.isoformat()
    if d < today:
        history[key] = len(rs)
    else:
        sales[key] = {'count': len(rs), 'zip_count': len(z), 'top_zips': [{'zip': a, 'count': b} for a, b in z.most_common(6)]}
out = {'generated_at': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), 'sales': sales, 'history': history}
tmp = OUT + '.tmp'; json.dump(out, open(tmp, 'w', encoding='utf-8'), indent=1); os.replace(tmp, OUT)
print('site-stats.json written:', {k: v['count'] for k, v in sales.items()}, 'history:', history)
