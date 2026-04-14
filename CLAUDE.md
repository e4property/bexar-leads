# Bexar County Motivated Seller Lead Scraper

## Project Overview
Automated daily scraper that pulls motivated seller leads from Bexar County Clerk portal and enriches them with property/mailing addresses from Bexar County GIS.

## Architecture
- **Scraper**: `scraper/fetch.py` — Playwright (SSR HTML parse) + ArcGIS REST API
- **Dashboard**: `dashboard/index.html` + `dashboard/records.json`
- **Automation**: `.github/workflows/scrape.yml` — runs daily at 2AM CST
- **Live URL**: https://e4property.github.io/bexar-leads/

## Data Sources
- **Clerk Portal**: https://bexar.tx.publicsearch.us (SSR HTML, requires auth)
- **Parcel Data**: https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query (public ArcGIS)

## Key Technical Details
- Portal uses server-side rendering — results are in HTML table, no API calls
- Requires login (CLERK_EMAIL + CLERK_PASSWORD GitHub Secrets)
- Table headers: `['', '', '', 'grantor', 'grantee', 'doc type', 'recorded date', 'doc number', ...]`
- First 3 columns are checkbox/icon columns (empty headers) — skip them
- URL format: `/results?department=RP&keywordSearch=false&limit=50&offset=N&recordedDateRange=YYYYMMDD%2CYYYYMMDD&searchType=docType&searchValue=LP`

## Lead Types
LP, NOFC, TAXDEED, JUD, CCJ, DRJUD, LNCORPTX, LNIRS, LNFED, LN, LNMECH, LNHOA, MEDLN, PRO, NOC, RELLP

## GitHub Secrets Required
- `CLERK_EMAIL` — registered account email for publicsearch.us
- `CLERK_PASSWORD` — registered account password

