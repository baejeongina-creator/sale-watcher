#!/usr/bin/env python3
import csv
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin
import io
import requests
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
# Replace with your actual CSV URLs from Google Sheets
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"
EDITORIAL_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1&single=true&output=csv" # Assuming GID 1 for Sheet2

SALE_KEYWORDS = {
    "SEASON OFF": ["SEASON OFF", "시즌오프", "시즌 오프"],
    "CLEARANCE": ["CLEARANCE", "클리어런스", "LAST CHANCE", "재고정리"],
    "REFURB": ["REFURB", "B-GRADE", "리퍼브", "B급"],
    "OUTLET": ["OUTLET", "아울렛"],
    "SALE": ["SALE", "OFF", "UP TO", "세일", "할인", "%", "~", "최대", "파이널 세일", "샘플세일", "아카이브 세일", "NEW YEAR SALE", "뉴이어 세일"]
}

NEGATIVE_KEYWORDS = ["SALE END", "SALE CLOSED", "SOLD OUT", "세일 종료", "품절", "종료"]
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# User-provided overrides for specific brands (brand_ko: {discount: X, sale_type: Y})
# This will be prioritized over scraped data
USER_OVERRIDES = {
    "공드린": {"discount": 0, "sale_type": "NO SALE"},
    "기준": {"discount": 0, "sale_type": "NO SALE"},
    "깁미더영": {"discount": 80, "sale_type": "SALE"},
    "낫띵리튼": {"discount": 50, "sale_type": "SALE"},
    "낫유어로즈": {"discount": 80, "sale_type": "CLEARANCE"},
    "노미나떼": {"discount": 50, "sale_type": "SALE"},
    "누와누": {"discount": 30, "sale_type": "ARCHIVE SALE"},
    "니브": {"discount": 95, "sale_type": "CLEARANCE"},
    "다이닛": {"discount": 50, "sale_type": "SALE"},
    "떼마": {"discount": 80, "sale_type": "SALE"},
    "루시르주": {"discount": 0, "sale_type": "NO SALE"},
    "르": {"discount": 90, "sale_type": "NEW YEAR SALE"},
    "르니나": {"discount": 50, "sale_type": "SALE"},
    "르바": {"discount": 0, "sale_type": "NO SALE"},
    "르베인": {"discount": 60, "sale_type": "REFURB"},
    "마조네": {"discount": 0, "sale_type": "NO SALE"},
    "메종마레": {"discount": 20, "sale_type": "SALE"},
    "몽돌": {"discount": 72, "sale_type": "SALE"},
    "밀로 아카이브": {"discount": 90, "sale_type": "SALE"},
    "보헴서": {"discount": 70, "sale_type": "SALE"},
    "블라썸에이치": {"discount": 30, "sale_type": "SALE"},
    "샵엠": {"discount": 0, "sale_type": "NO SALE"},
    "시에": {"discount": 50, "sale_type": "SALE"},
    "에프터아워즈": {"discount": 50, "sale_type": "SALE"},
    "에퓨레": {"discount": 50, "sale_type": "SEASON OFF"},
    "우마뭉": {"discount": 60, "sale_type": "SEASON OFF"},
    "윤세": {"discount": 90, "sale_type": "FINAL SALE"},
    "인사일런스": {"discount": 0, "sale_type": "NO SALE"},
    "잇자 바이브": {"discount": 0, "sale_type": "NO SALE"},
    "카키포인트": {"discount": 0, "sale_type": "NO SALE"},
    "타입서비스": {"discount": 0, "sale_type": "NO SALE"},
    "페이드인": {"discount": 70, "sale_type": "SALE"},
    "포니테일 아울렛": {"discount": 70, "sale_type": "OUTLET"},
    "포에브": {"discount": 61, "sale_type": "SALE"},
    "포유어아이즈온리": {"discount": 40, "sale_type": "SEASON OFF"},
    "포트오브 콜": {"discount": 0, "sale_type": "NO SALE"},
    "호와스": {"discount": 30, "sale_type": "SALE"},
    "필인더블랭크": {"discount": 50, "sale_type": "SEASON OFF"},
    "하네": {"discount": 80, "sale_type": "CLEARANCE"},
    "프리베일": {"discount": 50, "sale_type": "SALE"},
}

def clean_price_text(text: str) -> int:
    cleaned_text = re.sub(r'[^\d]', '', text)
    return int(cleaned_text) if cleaned_text else 0

def extract_max_discount(soup: BeautifulSoup, url: str) -> int:
    percentages = []
    text = soup.get_text(" ").upper()

    patterns = [
        r'UP\s*TO\s*(\d{1,2})%',
        r'최대\s*(\d{1,2})%',
        r'(\d{1,2})%\s*OFF',
        r'(\d{1,2})%\s*DISCOUNT',
        r'SAVE\s*(\d{1,2})%',
        r'(\d{1,2})\s*PERCENT',
        r'(\d{1,2})%[\s\S]*?SALE',
        r'SALE[\s\S]*?(\d{1,2})%',
        r'(\d{1,2})\s*~\s*(\d{1,2})%',
        r'(\d{1,2})%',
    ]

    for pattern in patterns:
        for m in re.findall(pattern, text, re.IGNORECASE):
            try:
                val = 0
                if isinstance(m, tuple): 
                    val = int(m[-1])
                else: 
                    val = int(m)
                if 5 <= val <= 95: percentages.append(val)
            except: pass

    price_elements = soup.find_all(string=re.compile(r'\d[\d,]*\s*(?:원|₩|KRW|USD|\$)', re.IGNORECASE))
    
    for el in price_elements:
        parent = el.find_parent()
        if not parent: continue

        original_price_el = parent.find(re.compile("span|del|s", re.IGNORECASE), class_=re.compile("original|old|regular|normal|list-price", re.IGNORECASE))
        sale_price_el = parent.find(re.compile("span|ins|b|strong|font", re.IGNORECASE), class_=re.compile("sale|discount|final|special|promo", re.IGNORECASE))

        if not original_price_el or not sale_price_el:
            product_card = parent.find_parent(class_=re.compile("product|item|card|prd", re.IGNORECASE))
            if product_card:
                original_price_el = product_card.find(re.compile("span|del|s", re.IGNORECASE), class_=re.compile("original|old|regular|normal|list-price", re.IGNORECASE))
                sale_price_el = product_card.find(re.compile("span|ins|b|strong|font", re.IGNORECASE), class_=re.compile("sale|discount|final|special|promo", re.IGNORECASE))

        if original_price_el and sale_price_el:
            try:
                original_price = clean_price_text(original_price_el.get_text())
                sale_price = clean_price_text(sale_price_el.get_text())
                
                if original_price > 0 and sale_price < original_price:
                    calculated_discount = round((1 - sale_price / original_price) * 100)
                    if 5 <= calculated_discount <= 95: percentages.append(calculated_discount)
            except (ValueError, TypeError): continue

    return max(percentages) if percentages else 0

def extract_banner(soup: BeautifulSoup, base_url: str) -> str:
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"): return urljoin(base_url, og_img["content"])
    for cls in ["main-banner", "hero-image", "visual", "top-banner", "banner-img", "promo-banner"]:
        img = soup.find("img", class_=re.compile(cls, re.I))
        if img and img.get("src"): return urljoin(base_url, img["src"])
    for img in soup.find_all("img", src=True):
        if "logo" not in img["src"].lower() and "icon" not in img["src"].lower() and "svg" not in img["src"].lower():
            return urljoin(base_url, img["src"])
    return ""

def get_sale_type(text: str) -> str:
    text_upper = text.upper()
    for category, keywords in SALE_KEYWORDS.items():
        if category == "SALE": continue
        for kw in keywords:
            if kw in text_upper: return category
    return "SALE"

def fetch_csv(url, retries=3, delay=5):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20, allow_redirects=True)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            if not resp.text.strip() or resp.text.startswith('<'):
                raise ValueError("Invalid CSV content received from Google Sheets.")
            return list(csv.DictReader(io.StringIO(resp.text)))
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"❌ Error fetching CSV on attempt {i+1}/{retries}: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("❌ All attempts to fetch CSV failed.")
                return []

def scan_brand(row):
    brand_en = (row.get("brand") or "").strip()
    brand_ko = (row.get("brand_ko") or "").strip()
    official_url = (row.get("official_url") or "").strip()
    
    manual_banner = (row.get("banner_url") or "").strip()
    manual_discount = (row.get("manual_discount") or "").strip()
    manual_status = (row.get("manual_status") or "").strip().lower()
    manual_type = (row.get("manual_type") or "").strip().upper()
    manual_sale_url = (row.get("sale_url") or "").strip()

    # Apply user overrides first if brand_ko matches
    override_data = USER_OVERRIDES.get(brand_ko)
    if override_data:
        manual_discount = str(override_data.get("discount", manual_discount))
        manual_type = override_data.get("sale_type", manual_type)
        # If user override discount is 0, explicitly set status to nosale
        if manual_discount == "0":
            manual_status = "nosale"
        elif manual_discount != "0": # If user provided discount, force sale
            manual_status = "sale"

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

    if manual_status == "nosale":
        return result

    try:
        scan_url = manual_sale_url if manual_sale_url else official_url
        if not scan_url: 
            print(f"Skipping {brand_en}: No URL provided.")
            return result

        resp = requests.get(scan_url, headers=HEADERS, timeout=20)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, "html.parser")
        
        page_text = soup.get_text(" ").upper()
        
        is_sale = False
        if any(kw in page_text for sublist in SALE_KEYWORDS.values() for kw in sublist):
            is_sale = True

        is_nosale = any(neg in page_text for neg in NEGATIVE_KEYWORDS)
        
        if manual_status == "sale":
            is_sale = True
            is_nosale = False
        elif manual_status == "nosale":
            is_sale = False

        if not is_sale and not is_nosale:
            temp_discount = extract_max_discount(soup, scan_url)
            if temp_discount > 0:
                is_sale = True

        if is_sale and not is_nosale:
            result["status"] = "sale"
            
            discount = extract_max_discount(soup, scan_url)
            
            if manual_discount:
                try: 
                    manual_disc_val = int(manual_discount)
                    if 5 <= manual_disc_val <= 95: 
                        discount = manual_disc_val
                except: 
                    print(f"Warning: Invalid manual_discount for {brand_en}: {manual_discount}")

            result["discount"] = discount
            
            if manual_type and manual_type in SALE_KEYWORDS:
                result["sale_type"] = manual_type
            else:
                result["sale_type"] = get_sale_type(page_text)

            if not manual_banner:
                result["banner_url"] = extract_banner(soup, scan_url)
                            
    except requests.exceptions.RequestException as req_err:
        print(f"Error fetching {brand_en} ({scan_url}): {req_err}")
        result["status"] = "error"
    except Exception as e:
        print(f"Error scanning {brand_en} ({scan_url}): {e}")
        result["status"] = "error"

    return result

def main():
    print("🚀 Starting enhanced scan...")
    rows = fetch_csv(CSV_URL)
    results = []
    for i, row in enumerate(rows, 1):
        if not row.get("brand") and not row.get("brand_ko"): continue
        if row.get("enabled", "TRUE").upper() != "TRUE": 
            print(f"Skipping disabled brand: {row.get("brand_ko") or row.get("brand")}")
            continue
        
        brand_name_display = row.get("brand_ko") or row.get("brand")
        print(f"[{i}/{len(rows)}] {brand_name_display}...", end=" ", flush=True)
        res = scan_brand(row)
        # Only add to results if it's a sale and not an error
        if res["status"] == "sale":
            results.append(res)
        print(f"{res["status"].upper()} ({res["discount"]}%)")
        time.sleep(0.5)

    editorials = fetch_csv(EDITORIAL_CSV_URL)
    
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "sales": results,
        "editorials": editorials
    }

    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✅ Complete. Saved to docs/sales.json")

if __name__ == "__main__":
    main()
