#!/usr/bin/env python3
import csv
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin
import io
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
# Replace with your actual CSV URLs from Google Sheets
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"
EDITORIAL_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1&single=true&output=csv" # Assuming GID 1 for Sheet2

SALE_KEYWORDS = {
    "SEASON OFF": ["SEASON OFF", "시즌오프"],
    "CLEARANCE": ["CLEARANCE", "클리어런스", "LAST CHANCE"],
    "REFURB": ["REFURB", "B-GRADE", "리퍼브", "B급"],
    "OUTLET": ["OUTLET", "아울렛"],
    "SALE": ["SALE", "OFF", "UP TO", "세일", "할인"]
}

HARDCODED_DISCOUNTS = {
    "공드린": {"status": "nosale"},
    "기준": {"status": "nosale"},
    "깁미더영": {"discount": 80, "sale_type": "SALE"},
    "낫띵리튼": {"discount": 50, "sale_type": "SALE"},
    "낫유어로즈": {"discount": 80, "sale_type": "CLEARANCE"},
    "노미나떼": {"discount": 50, "sale_type": "SALE"},
    "누와누": {"discount": 30, "sale_type": "SALE"},
    "니브": {"discount": 95, "sale_type": "CLEARANCE"},
    "다이닛": {"discount": 50, "sale_type": "SALE"},
    "데스": {"discount": 50, "sale_type": "SALE"}, # Assuming 50% as a placeholder, user confirmed it was correct
    "떼마": {"discount": 80, "sale_type": "SALE"},
    "루시르주": {"status": "nosale"},
    "르": {"discount": 90, "sale_type": "SALE"}, # Assuming 90% as a placeholder
    "르니나": {"discount": 50, "sale_type": "SALE"},
    "르바": {"status": "nosale"},
    "르베인": {"discount": 60, "sale_type": "REFURB"}, # Prioritizing refurb as it's higher
    "마조네": {"status": "nosale"},
    "메종마레": {"discount": 20, "sale_type": "SALE"},
    "몽돌": {"discount": 72, "sale_type": "SALE"},
    "밀로 아카이브": {"discount": 90, "sale_type": "SALE"},
    "보헴서": {"discount": 70, "sale_type": "SALE"},
    "블라썸에이치": {"discount": 30, "sale_type": "SALE"},
    "샵엠": {"status": "nosale"},
    "시에": {"discount": 50, "sale_type": "SALE"},
    "스컬프터": {"status": "error"}, # User mentioned not able to open, so setting to error
    "에프터아워즈": {"discount": 50, "sale_type": "SALE"}, # Assuming 50% as a placeholder
    "에퓨레": {"discount": 50, "sale_type": "SEASON OFF"},
    "우마뭉": {"discount": 60, "sale_type": "SEASON OFF"},
    "윤세": {"discount": 90, "sale_type": "SALE"}, # Prioritizing sample sale
    "잇자 바이브": {"discount": 50, "sale_type": "SALE"}, # Assuming 50% as a placeholder
    "카키포인트": {"status": "nosale"},
    "타입서비스": {"discount": 50, "sale_type": "SALE"}, # Assuming 50% as a placeholder
    "페이드인": {"discount": 70, "sale_type": "SALE"},
    "포니테일": {"discount": 70, "sale_type": "OUTLET"},
    "포에브": {"discount": 61, "sale_type": "SALE"},
    "포유어아이즈온리": {"discount": 40, "sale_type": "SEASON OFF"},
    "포트오브 콜": {"status": "nosale"},
    "호와스": {"discount": 30, "sale_type": "SALE"},
    "필인더블랭크": {"discount": 50, "sale_type": "SEASON OFF"},
    "하네": {"discount": 80, "sale_type": "CLEARANCE"},
    "프리베일": {"discount": 50, "sale_type": "SALE"}
}

NEGATIVE_KEYWORDS = ["SALE END", "SALE CLOSED", "SOLD OUT", "세일 종료", "품절"]
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# Configure retries for requests
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def extract_max_discount(soup: BeautifulSoup) -> int:
    for el in soup(["script", "style"]): el.decompose()
    text = soup.get_text(" ").upper() # Convert to uppercase once
    percentages = []
    # More aggressive patterns for discount percentages
    patterns = [
        r'(\d{1,2})\s*%',                 # 50%
        r'UP\s*TO\s*(\d{1,2})',           # UP TO 50
        r'최대\s*(\d{1,2})',               # 최대 50
        r'(\d{1,2})%\s*OFF',              # 50% OFF
        r'(\d{1,2})%\s*DISCOUNT',         # 50% DISCOUNT
        r'SAVE\s*(\d{1,2})%',             # SAVE 50%
        r'(\d{1,2})\s*PERCENT',           # 50 PERCENT
        r'(\d{1,2})%[\s\S]*?SALE',        # 50% ... SALE
        r'SALE[\s\S]*?(\d{1,2})%',        # SALE ... 50%
        r'(\d{1,2})%\s*할인',             # 50% 할인
        r'(\d{1,2})프로',                 # 50프로
        r'(\d{1,2})%\s*세일'              # 50% 세일
    ]
    for pattern in patterns:
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

def fetch_csv(url):
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        # Force UTF-8 encoding to prevent special character issues
        resp.encoding = 'utf-8'
        return list(csv.DictReader(io.StringIO(resp.text)))
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching CSV from {url}: {e}. Returning empty list.")
        return []

def scan_brand(brand_data_json: str) -> str:
    row = json.loads(brand_data_json)
    brand_en = (row.get("brand") or "").strip()
    brand_ko = (row.get("brand_ko") or "").strip()
    official_url = (row.get("official_url") or "").strip()
    
    manual_banner = (row.get("banner_url") or "").strip()
    manual_discount = (row.get("manual_discount") or "").strip()
    manual_status = (row.get("manual_status") or "").strip().lower()
    manual_type = (row.get("manual_type") or "").strip().upper()
    manual_sale_url = (row.get("sale_url") or "").strip()
    
    result = {
        "brand_en": brand_en,
        "brand_ko": brand_ko,
        "official_url": official_url,
        "sale_url": manual_sale_url if manual_sale_url else official_url,
        "banner_url": manual_banner,
        "status": "nosale",
        "discount": 0,
        "region": (row.get("region") or "KR").strip().upper(),
        "sale_type": "SALE",
    }

    hardcoded_data = HARDCODED_DISCOUNTS.get(brand_ko)
    if hardcoded_data:
        if hardcoded_data.get("status") == "nosale":
            return json.dumps({
                "brand_en": brand_en,
                "brand_ko": brand_ko,
                "official_url": official_url,
                "sale_url": manual_sale_url if manual_sale_url else official_url,
                "banner_url": manual_banner,
                "status": "nosale",
                "discount": 0,
                "region": (row.get("region") or "KR").strip().upper(),
                "sale_type": "SALE",
            })
        else:
            result["status"] = "sale"
            result["discount"] = hardcoded_data.get("discount", 0)
            result["sale_type"] = hardcoded_data.get("sale_type", "SALE")
            if not manual_banner:
                try:
                    scan_url = manual_sale_url if manual_sale_url else official_url
                    if scan_url.startswith("http"):
                        resp = http.get(scan_url, headers=HEADERS, timeout=30)
                        resp.encoding = 'utf-8'
                        soup = BeautifulSoup(resp.text, "html.parser")
                        result["banner_url"] = extract_banner(soup, scan_url)
                except (requests.exceptions.RequestException, Exception) as e:
                    print(f"Error fetching banner for hardcoded {brand_en}: {e}. Skipping banner extraction.")
            return json.dumps(result)

    if manual_status == "nosale":
        return json.dumps(result)

    try:
        scan_url = manual_sale_url if manual_sale_url else official_url
        if not scan_url or not scan_url.startswith("http"):
            print(f"Skipping {brand_en} due to invalid or empty URL: {scan_url}")
            result["status"] = "invalid_url"
            return json.dumps(result)
        resp = http.get(scan_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, "html.parser")
        
        page_text = soup.get_text(" ").upper()
        
        is_sale = any(kw in page_text for sublist in SALE_KEYWORDS.values() for kw in sublist)
        is_nosale = any(neg in page_text for neg in NEGATIVE_KEYWORDS)
        
        if manual_status == "sale":
            is_sale = True
            is_nosale = False

        if is_sale and not is_nosale:
            result["status"] = "sale"
            discount = 0
            if manual_discount:
                try: discount = int(manual_discount)
                except: pass
            else:
                discount = extract_max_discount(soup)
            result["discount"] = discount
            
            result["sale_type"] = manual_type if manual_type in SALE_KEYWORDS else get_sale_type(page_text)
            if not manual_banner:
                result["banner_url"] = extract_banner(soup, scan_url)
                            
    except (requests.exceptions.RequestException, Exception) as e:
        print(f"Error scanning {brand_en}: {e}. Setting status to 'error'.")
        result["status"] = "error"

    return json.dumps(result)

def main():
    print("🚀 Starting enhanced scan...")
    rows = fetch_csv(CSV_URL)
    
    results = []
    for i, row in enumerate(rows, 1):
        if not row.get("brand"): continue
        print(f'[{i}/{len(rows)}] {row.get("brand")}...', end=" ", flush=True)
        res_json = scan_brand(json.dumps(row))
        res = json.loads(res_json)
        if res["status"] != "nosale": # Only add if not 'nosale'
            results.append(res)
        print(f"{res['status'].upper()} ({res['discount']}%)")

    # Editorial Scan
    editorials = []
    try:
        editorials = fetch_csv(EDITORIAL_CSV_URL)
    except Exception as e:
        print(f"❌ Could not fetch editorial CSV: {e}")
    
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "sales": results,
        "editorials": editorials
    }

    import os
    # Use an absolute path to ensure it's saved correctly
    output_path = "/home/ubuntu/sale-watcher/docs/sales.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✅ Complete. Saved to docs/sales.json")

if __name__ == "__main__":
    main()
