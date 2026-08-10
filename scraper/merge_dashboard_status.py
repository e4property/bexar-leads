"""
Merges live per-lead status fields (set by the browser dashboard's
writeBackToRepo() via direct GitHub Contents API commits — "dash: update
lead <doc>") into a freshly-rebuilt local dashboard/records.json, using
whatever is on `ref` (default origin/main) as the source of truth for
those fields.

Why this exists: the dashboard commits directly to GitHub every time a
lead is pushed to Jarvis / has notes or a dispo set (dozens of times a
day). Those commits race against this scraper's own commit-and-push. The
old retry loop resolved that race with `git rebase -X theirs origin/main`,
which — since it conflicts on dashboard/records.json (a single-line file,
so any two different edits always conflict) — silently discarded this
scraper's ENTIRE rebuilt dashboard/records.json (new leads, scores,
flags, everything) in favor of whatever the last browser click had
written, any time a dash-commit landed on origin between this workflow's
checkout and push. Confirmed happening: 84 such commits landed on
2026-08-10 alone.

Fix: before committing, pull forward only the fields the browser can
actually set, onto this run's freshly-built records. That makes the
local file a strict superset of origin, so it's then safe (and correct)
for the retry loop to prefer local (`-X ours`) on any remaining conflict.
"""
import json
import subprocess
import sys

LIVE_FIELDS = [
    "ghl_pushed", "ghl_pushed_at", "ghl_id",
    "dash_phone", "dash_searched_at", "dash_dispo", "dash_notes",
]


def main():
    ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"

    try:
        origin_raw = subprocess.run(
            ["git", "show", f"{ref}:dashboard/records.json"],
            capture_output=True, text=True, check=True,
        ).stdout
        origin_records = json.loads(origin_raw)
    except Exception as e:
        print(f"merge_dashboard_status: nothing to merge from {ref} ({e})")
        return

    origin_by_doc = {r["doc_number"]: r for r in origin_records if r.get("doc_number")}

    with open("dashboard/records.json", encoding="utf-8") as f:
        local_records = json.load(f)

    merged = 0
    for r in local_records:
        o = origin_by_doc.get(r.get("doc_number"))
        if not o:
            continue
        for field in LIVE_FIELDS:
            ov = o.get(field)
            if ov not in (None, "") and r.get(field) != ov:
                r[field] = ov
                merged += 1

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(local_records, separators=(",", ":"), ensure_ascii=True))

    print(f"merge_dashboard_status: merged {merged} live field(s) from {ref} "
          f"across {len(origin_by_doc)} origin records")


if __name__ == "__main__":
    main()
