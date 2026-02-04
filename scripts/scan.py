#!/usr/bin/env python3
import csv
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import io
import requests
from bs4 import BeautifulSoup

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

SALE_KEYWORDS = {
    "SEASON OFF": ["SEASON OFF", "시즌오프"],
    "CLEARANCE": ["CLEARANCE", "클리어런스", "LAST CHANCE"],
    "REFURB": ["REFURB", "B-GRADE", "리퍼브", "B급"],
    "SALE": ["SALE", "OFF", "OUTLET", "UP TO", "세일", "할인"]
}

NEGATIVE_KEYWORDS = ["SALE END", "SALE CLOSED", "SOLD OUT", "세일 종료", "품절"]
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def extract_max_discount(soup: BeautifulSoup) -> int:
    for el in soup(["script", "style"]): el.decompose()
    text = soup.get_text(" ")
    percentages = []
    for pattern in [r'(\d{1,2})\s*%', r'UP\s*TO\s*(\d{1,2})', r'최대\s*(\d{1,2})']:
        for m in re.findall(pattern, text, re.IGNORECASE):
            try:
                val = int(m)
                if 5 <= val <= 95: percentages.append(val)
            except: pass
    return max(percentages) if percentages else 0

def extract_banner(soup: BeautifulSoup, base_url: str) -> str:
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"): return urljoin(base_url, og_img["content"])
    for cls in ["main-banner", "hero-image", "visual", "top-banner"]:
        img = soup.find("img", class_=re.compile(cls, re.I))
        if img and img.get("src"): return urljoin(base_url, img["src"])
    for img in soup.find_all("img", src=True):
        if "logo" not in img["src"].lower(): return urljoin(base_url, img["src"])
    return ""

def get_sale_type(text: str) -> str:
    text_upper = text.upper()
    for category, keywords in SALE_KEYWORDS.items():
        for kw in keywords:
            if kw in text_upper: return category
    return "SALE"

def fetch_brands():
    try:
        resp = requests.get(CSV_URL, timeout=20)
        resp.raise_for_status()
        return list(csv.DictReader(io.StringIO(resp.text)))
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def scan_brand(row):
    brand_en = (row.get("brand") or "").strip()
    brand_ko = (row.get("brand_ko") or "").strip()
    url = (row.get("official_url") or "").strip()
    
    # Check for manual overrides
    manual_banner = (row.get("banner_url") or "").strip()
    manual_discount = (row.get("manual_discount") or "").strip()
    manual_status = (row.get("manual_status") or "").strip()
    
    result = {
        "brand_en": brand_en,
        "brand_ko": brand_ko,
        "official_url": url,
        "sale_url": url,
        "banner_url": manual_banner,
        "status": "nosale",
        "discount": 0,
        "region": (row.get("region") or "KR").strip().upper(),
        "sale_type": "SALE"
    }

    # If manual status is set, use it
    if manual_status:
        result["status"] = manual_status.lower()
        if manual_discount:
            try: result["discount"] = int(manual_discount)
            except: pass
        return result

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        page_text = soup.get_text(" ").upper()
        if any(neg in page_text for neg in NEGATIVE_KEYWORDS): return result

        found_kw = None
        for sublist in SALE_KEYWORDS.values():
            for kw in sublist:
                if kw in page_text:
                    found_kw = kw
                    break
            if found_kw: break
        
        if found_kw:
            discount = extract_max_discount(soup)
            if not manual_banner: result["banner_url"] = extract_banner(soup, url)
            result["sale_type"] = get_sale_type(page_text)
            
            if manual_discount:
                try: discount = int(manual_discount)
                except: pass
            
            if discount > 0 or result["sale_type"] in ["SEASON OFF", "CLEARANCE", "REFURB"]:
                result["status"] = "sale"
                result["discount"] = discount
                            
    except Exception as e:
        result["status"] = "error"

    return result

def main():
    print("🚀 Scanning with manual override support...")
    rows = fetch_brands()
    results = []
    for i, row in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] {row['brand']}...", end=" ", flush=True)
        res = scan_brand(row)
        results.append(res)
        print(f"{res['status'].upper()} ({res['discount']}%)")
        time.sleep(0.5)

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "sales": results
    }

    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✅ Complete.")

if __name__ == "__main__":
    main()
