"""One-off read-only diagnostic: what does the CI browser see on the FC results page 1?"""
import json, re, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from fetch import get_driver
from selenium.webdriver.common.by import By

known = {str(r.get("doc_number")) for r in json.loads(Path("dashboard/records.json").read_text(encoding="utf-8")) if r.get("doc_number")}
print("KNOWN_DOCS", len(known))
URL = ("https://bexar.tx.publicsearch.us/results?department=FC&instrumentDateRange=20000404%2C20270403"
       "&keywordSearch=false&limit=50&offset=0&sort=desc&sortBy=recordedDate&searchType=quickSearch")
d = get_driver()
try:
    for label, url in (("hardnav", URL),):
        d.get("about:blank"); d.get(url); time.sleep(8)
        body = re.sub(r"\s+", " ", d.find_element(By.TAG_NAME, "body").text)
        print(label, "HEADER", body[:140])
        rows = d.find_elements(By.CSS_SELECTOR, "table tbody tr")
        print(label, "ROWS", len(rows), "UA", d.execute_script("return navigator.userAgent"))
        inknown = 0
        for i, r in enumerate(rows):
            t = re.sub(r"\s+", " ", r.text)
            m = re.search(r"\b(20\d{9})\b", t)
            if m and m.group(1) in known: inknown += 1
            if i < 8: print(label, "ROW", t[:130])
        print(label, "IN_KNOWN", inknown, "of", len(rows))
finally:
    d.quit()
