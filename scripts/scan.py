import csv, json, re, time
from datetime import datetime
import requests

# 네 Google Sheets → CSV 링크
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

# 전 브랜드 공통으로 쓸 세일 키워드들
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


def detect_sale_for_brand(row):
    brand = (row.get("brand") or "").strip()
    url = (row.get("official_url") or row.get("url") or "").strip()
    enabled = (row.get("enabled") or "TRUE").strip().lower()
    override = (row.get("keywords_override") or "").strip()

    # enabled 가 FALSE/0/no 면 스킵
    if enabled in ("false", "0", "no"):
        return None

    # 기본 키워드 + 브랜드별 override
    keywords = GLOBAL_KEYWORDS[:]
    if override:
        for kw in override.split("|"):
            kw = kw.strip()
            if kw and kw.upper() not in [k.upper() for k in keywords]:
                keywords.append(kw)

    status = "error"
    matched_kw = None
    error_msg = None

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        text = re.sub(r"\s+", " ", resp.text).upper()

        status = "nosale"
        for kw in keywords:
            if kw.upper() in text:
                status = "sale"
                matched_kw = kw
                break
    except Exception as e:
        error_msg = str(e)

    return {
        "brand": brand,
        "official_url": url,
        "status": status,            # "sale" / "nosale" / "error"
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
        time.sleep(1)  # 너무 빨리 돌지 않게

    out = {"generated_at": now, "sales": results}
    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
