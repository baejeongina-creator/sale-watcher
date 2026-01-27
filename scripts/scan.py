import csv, json, re, time
from datetime import datetime
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

GLOBAL_KEYWORDS = [
    "SALE", "SEASON OFF", "SEASONAL", "WINTER", "SUMMER", "SPRING", "FALL",
    "CLEARANCE", "FINAL", "LAST CHANCE", "OUTLET", "ARCHIVE",
    "REFURB", "REFURBISHED", "B-GRADE", "SAMPLE",
    "UP TO", "%", "DEAL",
    "세일", "할인", "시즌오프", "클리어런스", "아울렛", "특가", "최대"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
}


def fetch_rows():
    resp = requests.get(CSV_URL, timeout=20)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    return list(reader)


def find_sale_link(html: str, base_url: str, keywords):
    """페이지 안의 <a> 중 세일 관련 링크로 보이는 것 골라서 full URL로 반환."""
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        low = href.lower()
        if low.startswith("#") or low.startswith("mailto:") or low.startswith("tel:") or low.startswith("javascript:"):
            continue

        text = (a.get_text(" ", strip=True) or "").upper()
        target = href.upper()

        score = 0
        for kw in keywords:
            up = kw.upper()
            if up in text or up in target:
                score += 1

        if score == 0:
            continue

        full_url = urljoin(base_url, href)
        candidates.append((score, len(text), full_url))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


def detect_sale_for_brand(row):
    brand = (row.get("brand") or "").strip()
    url = (row.get("official_url") or row.get("url") or "").strip()
    enabled = (row.get("enabled") or "TRUE").strip().lower()
    override = (row.get("keywords_override") or "").strip()

    if enabled in ("false", "0", "no"):
        return None

    keywords = GLOBAL_KEYWORDS[:]
    if override:
        for kw in override.split("|"):
            kw = kw.strip()
            if kw and kw.upper() not in [k.upper() for k in keywords]:
                keywords.append(kw)

    status = "error"
    matched_kw = None
    error_msg = None
    sale_url = None

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        html = resp.text
        text_upper = re.sub(r"\s+", " ", html).upper()

        status = "nosale"
        for kw in keywords:
            if kw.upper() in text_upper:
                status = "sale"
                matched_kw = kw
                break

        if status == "sale":
            sale_url = find_sale_link(html, url, keywords)

        if not sale_url:
            sale_url = url

    except Exception as e:
        error_msg = str(e)
        sale_url = url  # 에러 나도 최소 공홈은 유지

    return {
        "brand": brand,
        "official_url": url,
        "sale_url": sale_url,
        "status": status,
        "matched_keyword": matched_kw,
        "error": error_msg,
    }


def main():
    rows = fetch_rows()
    now = datetime.utcnow().isoformat() + "Z"
    results = []

    for row in rows:
        res = detect_sale_for_brand(row)
        if res is None:
            continue
        res["checked_at"] = now
        results.append(res)
        time.sleep(1)

    out = {"generated_at": now, "sales": results}
    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
