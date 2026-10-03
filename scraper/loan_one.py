"""
loan_one.py
One-off, read-only lookup for a single doc: opens it in the clerk viewer and
saves high-resolution screenshots of its pages to ./out/ (uploaded as a
workflow artifact so a human/Claude can read them directly -- the default
OCR path returns garbage on these scans). Also tries upscaled OCR to find the
Deed of Trust doc number the Notice references. Writes nothing to the repo.

Env:
  DOC_NUMBER  FC-department doc (the Notice of Trustee's Sale)
  REF_DOC     optional Land Records doc (e.g. the Deed of Trust) to capture too
"""

import logging
import os
import re
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from fetch import (get_driver, goto_doc_by_docnumber, login_publicsearch,
                   search_and_ocr_referenced_doc)

OUT = Path("out")
OUT.mkdir(exist_ok=True)


def hires_driver_setup(driver):
    try:
        driver.set_window_size(1600, 2400)
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": 1600, "height": 2400, "deviceScaleFactor": 3, "mobile": False})
    except Exception as e:
        log.warning(f"hires setup failed: {e}")


def snap_pages(driver, tag, max_pages=4):
    """Screenshot the document preview svg, then click 'next' and repeat."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    texts = []
    for _ in range(2):
        try:
            zo = driver.find_elements(
                By.XPATH, "//*[(self::button or @role='button')][contains(translate(@aria-label,'ZOMUT','zomut'),'zoom out') "
                          "or contains(translate(@title,'ZOMUT','zomut'),'zoom out')]")
            if zo:
                zo[0].click()
                time.sleep(1)
        except Exception as e:
            log.info(f"zoom-out click failed: {e}")
    for p in range(1, max_pages + 1):
        try:
            el = WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'svg[aria-label="Document preview image"], svg[role="img"]')))
            time.sleep(2)
            path = OUT / f"{tag}_p{p}.png"
            path.write_bytes(el.screenshot_as_png)
            log.info(f"saved {path} ({path.stat().st_size} bytes)")
            # The element screenshot clips the right edge of the page on this
            # viewer -- also dump the DOM geometry and a wide full-page capture.
            try:
                info = driver.execute_script(
                    "const s=arguments[0];const r=s.getBoundingClientRect();"
                    "const p=s.parentElement.getBoundingClientRect();"
                    "return {w:r.width,h:r.height,x:r.x,y:r.y,parentW:p.width,parentH:p.height,"
                    "vb:s.getAttribute('viewBox'),wa:s.getAttribute('width'),ha:s.getAttribute('height'),"
                    "overflow:getComputedStyle(s.parentElement).overflow,innerW:window.innerWidth,"
                    "scrollW:document.documentElement.scrollWidth};", el)
                log.info(f"GEOM| {info}")
                import base64
                shot = driver.execute_cdp_cmd("Page.captureScreenshot", {
                    "format": "png", "captureBeyondViewport": True,
                    "clip": {"x": 0, "y": 0, "width": max(info["scrollW"], info["innerW"]),
                             "height": min(info["y"] + info["h"] + 40, 6000), "scale": 1}})
                (OUT / f"{tag}_p{p}_full.png").write_bytes(base64.b64decode(shot["data"]))
            except Exception as e:
                log.warning(f"geom/full capture failed: {e}")
            try:
                import pytesseract
                from PIL import Image
                im = Image.open(path)
                texts.append(pytesseract.image_to_string(im, config="--psm 6"))
            except Exception as e:
                log.warning(f"ocr failed: {e}")
        except Exception as e:
            log.warning(f"page {p}: no preview ({e})")
            break
        nxt = driver.find_elements(
            By.XPATH, "//button[contains(translate(@aria-label,'NEXT','next'),'next')] | "
                      "//*[@role='button'][contains(translate(@aria-label,'NEXT','next'),'next')]")
        if not nxt:
            log.info("no next-page control found")
            break
        try:
            nxt[0].click()
            time.sleep(2)
        except Exception as e:
            log.info(f"next click failed: {e}")
            break
    return "\n".join(texts)


def main():
    doc = os.environ.get("DOC_NUMBER", "").strip()
    ref = os.environ.get("REF_DOC", "").strip()
    driver = get_driver()
    try:
        if not login_publicsearch(driver):
            log.warning("login failed or skipped")
        hires_driver_setup(driver)

        if doc:
            opened = False
            for attempt in range(1, 4):
                driver.switch_to.new_window("tab")
                hires_driver_setup(driver)
                if goto_doc_by_docnumber(driver, doc):
                    opened = True
                    break
                log.info(f"attempt {attempt}: not found in fresh tab, retrying")
            if not opened:
                log.error(f"could not open doc {doc}")
            else:
                text = snap_pages(driver, f"notice_{doc}", max_pages=3)
                m = re.search(r"Document\s+Number\s+(\d{6,})\s+on\s+([A-Za-z]+ \d{1,2},\s*\d{4})",
                              text, re.IGNORECASE)
                log.info(f"REF_FROM_OCR={m.groups() if m else None}")
                for line in text.splitlines():
                    if re.search(r"(Deed of Trust|Lender|Borrower|principal|Document Number)", line, re.I):
                        log.info(f"NOTICE| {line.strip()[:300]}")

        for ref_one in [r.strip() for r in ref.split(",") if r.strip()]:
            driver.switch_to.new_window("tab")
            hires_driver_setup(driver)
            from fetch import QUICK_SEARCH_URL_TMPL, TODAY_NAIVE
            from datetime import timedelta
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            today_str = (TODAY_NAIVE - timedelta(days=3)).strftime("%Y%m%d")
            driver.get(QUICK_SEARCH_URL_TMPL.format(today=today_str, doc_number=ref_one))
            WebDriverWait(driver, 20).until(EC.presence_of_element_located(
                (By.XPATH, "//table//tr/td | //h1[contains(text(),'No Results')]")))
            if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
                log.error(f"no results for ref doc {ref_one}")
                continue
            time.sleep(1)
            driver.find_element(By.CSS_SELECTOR, "table tbody tr").click()
            WebDriverWait(driver, 20).until(EC.url_contains("/doc/"))
            time.sleep(2)
            snap_pages(driver, f"dot_{ref_one}", max_pages=3)
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
