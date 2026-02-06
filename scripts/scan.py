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
    "SEASON OFF": ["SEASON OFF", "시즌오프"],
    "CLEARANCE": ["CLEARANCE", "클리어런스", "LAST CHANCE"],
    "REFURB": ["REFURB", "B-GRADE", "리퍼브", "B급"],
    "OUTLET": ["OUTLET", "아울렛"],
    "SALE": ["SALE", "OFF", "UP TO", "세일", "할인", "%", "~", "최대"]
}

NEGATIVE_KEYWORDS = ["SALE END", "SALE CLOSED", "SOLD OUT", "세일 종료", "품절"]
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def extract_max_discount(soup: BeautifulSoup, url: str) -> int:
    # Remove script and style elements to clean text
    for el in soup(["script", "style"]): el.decompose()
    text = soup.get_text(" ").upper() # Convert to uppercase once
    percentages = []

    # --- Aggressive text pattern matching for discounts ---
    patterns = [
        r'UP\s*TO\s*(\d{1,2})%',           # UP TO 80%
        r'최대\s*(\d{1,2})%',               # 최대 80%
        r'(\d{1,2})%\s*OFF',              # 80% OFF
        r'(\d{1,2})%\s*DISCOUNT',         # 80% DISCOUNT
        r'SAVE\s*(\d{1,2})%',             # SAVE 80%
        r'(\d{1,2})\s*PERCENT',           # 80 PERCENT
        r'(\d{1,2})%[\s\S]*?SALE',        # 80% ... SALE
        r'SALE[\s\S]*?(\d{1,2})%',        # SALE ... 80%
        r'(\d{1,2})\s*~\s*(\d{1,2})%',    # 10% ~ 80% (captures second number)
        r'(\d{1,2})%',                    # General % (less priority)
    ]

    for pattern in patterns:
        for m in re.findall(pattern, text, re.IGNORECASE):
            try:
                # Handle patterns like '10% ~ 80%' where 'm' might be a tuple
                if isinstance(m, tuple): 
                    val = int(m[1]) # Take the higher value in a range
                else: 
                    val = int(m)
                if 5 <= val <= 95: percentages.append(val)
            except: pass
    
    # --- Attempt to find discounts from product listings (simplified) ---
    # This is a generic attempt and might not work for all sites due to varied HTML structures
    try:
        # More robust product card and price finding
        product_cards = soup.find_all(re.compile("div|li|article", re.IGNORECASE), class_=re.compile("product|item|card|prd", re.IGNORECASE))
        if not product_cards: # Fallback if specific classes are not found
            product_cards = soup.find_all(lambda tag: tag.has_attr('class') and any(re.search("product|item|card|prd", c, re.I) for c in tag['class']))

        for card in product_cards:
            original_price_el = card.find(re.compile("span|del|s", re.IGNORECASE), class_=re.compile("original|old|regular|normal|list-price", re.IGNORECASE))
            sale_price_el = card.find(re.compile("span|ins|b|strong|font", re.IGNORECASE), class_=re.compile("sale|discount|final|special|promo", re.IGNORECASE))
            
            # Fallback if specific classes are not found within the card
            if not original_price_el or not sale_price_el:
                prices = card.find_all(re.compile("span|div|p"), string=re.compile(r"[\d,]+(?:원|won)", re.I))
                if len(prices) >= 2:
                    original_price_el = prices[0]
                    sale_price_el = prices[1]

            if original_price_el and sale_price_el:
                try:
                    original_price_text = re.sub(r'[^\d]', '', original_price_el.get_text())
                    sale_price_text = re.sub(r'[^\d]', '', sale_price_el.get_text())
                    
                    if original_price_text and sale_price_text:
                        original_price = int(original_price_text)
                        sale_price = int(sale_price_text)
                        if original_price > 0 and sale_price < original_price:
                            calculated_discount = round((1 - sale_price / original_price) * 100)
                            if 5 <= calculated_discount <= 95: percentages.append(calculated_discount)
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        print(f"Warning: Could not parse product prices on {url}: {e}")

    return max(percentages) if percentages else 0

def extract_banner(soup: BeautifulSoup, base_url: str) -> str:
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"): return urljoin(base_url, og_img["content"])
    for cls in ["main-banner", "hero-image", "visual", "top-banner", "banner-img", "promo-banner"]:
        img = soup.find("img", class_=re.compile(cls, re.I))
        if img and img.get("src"): return urljoin(base_url, img["src"])
    # Fallback to finding any non-logo image
    for img in soup.find_all("img", src=True):
        if "logo" not in img["src"].lower() and "icon" not in img["src"].lower() and "svg" not in img["src"].lower():
            return urljoin(base_url, img["src"])
    return ""

def get_sale_type(text: str) -> str:
    text_upper = text.upper()
    for category, keywords in SALE_KEYWORDS.items():
        if category == "SALE": continue # 'SALE' is a fallback, check specific types first
        for kw in keywords:
            if kw in text_upper: return category
    return "SALE" # Default to 'SALE' if no specific type found

def fetch_csv(url, retries=3, delay=5):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20, allow_redirects=True)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            # Check for empty or error responses from Google Sheets
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
        if not scan_url: # Skip if no URL is provided
            print(f"Skipping {brand_en}: No URL provided.")
            return result

        resp = requests.get(scan_url, headers=HEADERS, timeout=20)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, "html.parser")
        
        page_text = soup.get_text(" ").upper()
        
        is_sale = False
        # Check for sale keywords in page text
        if any(kw in page_text for sublist in SALE_KEYWORDS.values() for kw in sublist):
            is_sale = True

        # Check for negative keywords
        is_nosale = any(neg in page_text for neg in NEGATIVE_KEYWORDS)
        
        # Prioritize manual status
        if manual_status == "sale":
            is_sale = True
            is_nosale = False
        elif manual_status == "nosale":
            is_sale = False

        if is_sale and not is_nosale:
            result["status"] = "sale"
            
            # Extract discount using enhanced logic
            discount = extract_max_discount(soup, scan_url)
            
            # Prioritize manual discount
            if manual_discount:
                try: 
                    manual_disc_val = int(manual_discount)
                    if 5 <= manual_disc_val <= 95: # Validate manual discount
                        discount = manual_disc_val
                except: 
                    print(f"Warning: Invalid manual_discount for {brand_en}: {manual_discount}")

            result["discount"] = discount
            
            # Prioritize manual sale type
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
        if not row.get("brand"): continue
        if row.get("enabled", "TRUE").upper() != "TRUE": # Skip if not enabled
            print(f"Skipping disabled brand: {row['brand']}")
            continue
        print(f"[{i}/{len(rows)}] {row['brand']}...", end=" ", flush=True)
        res = scan_brand(row)
        results.append(res)
        print(f"{res['status'].upper()} ({res['discount']}%)")
        time.sleep(0.5)

    # Editorial Scan
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
