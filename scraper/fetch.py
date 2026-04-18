name: Scrape Bexar County Leads

on:
  schedule:
    - cron: "0 7 * * *"
  workflow_dispatch:

permissions:
  contents: write
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  scrape:
    name: Scrape & Enrich Leads
    runs-on: ubuntu-22.04

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install requests beautifulsoup4 lxml

      - name: Create output directories
        run: mkdir -p dashboard data

      - name: Run scraper
        env:
          LOOKBACK_DAYS: "7"
        run: python scraper/fetch.py
        timeout-minutes: 10

      - name: Copy records to dashboard
        run: cp data/records.json dashboard/records.json

      - name: Show summary
        run: |
          python -c "
          import json
          d = json.load(open('dashboard/records.json'))
          print(f'Total: {d[\"total\"]} | With address: {d[\"with_address\"]}')
          print(f'Source: {d[\"source\"]}')
          print(f'Fetched: {d[\"fetched_at\"]}')
          if d['records']:
              r = d['records'][0]
              print(f'Sample: {r.get(\"cat\")} | {r.get(\"prop_address\")} | {r.get(\"filed\")}')
          "

      - name: Commit records
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add -f dashboard/records.json data/records.json data/leads_export.csv
          git diff --cached --quiet && echo "No changes" || git commit -m "chore: update leads $(date -u +'%Y-%m-%d %H:%M UTC')"
          git push origin main

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: dashboard/

  deploy:
    name: Deploy to GitHub Pages
    needs: scrape
    runs-on: ubuntu-22.04
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4

