import csv, json, re, time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Google Sheets → CSV 링크
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

# 페이지 전체에서 "세일 중인지" 감지하는 키워드
GLOBAL_KEYWORDS = [
    "SALE", "SEASON OFF", "SEASONAL", "WINTER", "SUMMER", "SPRING", "FALL",
    "CLEARANCE", "FINAL", "LAST CHANCE", "OUTLET", "ARCHIVE",
    "REFURB", "REFURBISHED", "B-GRADE", "SAMPLE",
    "UP TO", "%", "DEAL",
    "세일", "할인", "시즌오프", "클리어런스", "아울렛", "특가", "최대"
]

# 개별 링크가 "세일 페이지일 가능성" 판단용 키워드
LINK_SALE_KEYWORDS = [
    "SALE", "SEASON", "OFF", "CLEARANCE", "OUTLET", "ARCHIVE",
    "REFURB", "DISCOUNT", "PROMOTION", "EVENT", "WINTER", "SUMMER",
]

# 절대 들어가면 안 되는 링크 (로그인, 회원가입, 마이페이지 등)
LINK_BLACKLIST = [
    "LOGIN", "LOG-IN", "SIGNIN", "SIGN-IN", "SIGNUP", "SIGN-UP", "REGISTER",
    "JOIN", "MEMBER", "MYSHOP", "MYPAGE", "MY PAGE",
    "CART", "BAG", "BASKET", "CHECKOUT", "ORDER",
    "ACCOUNT", "PROFILE",
    "PRESS", "STORY", "LOOKBOOK", "LOOK BOOK",
    "INSTAGRAM", "FACEBOOK", "YOUTUBE", "TWITTER",
    "KAKAO", "PF.KAKAO.COM"
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
    """
    페이지 안의 <a> 태그들 중에서
    '세일 페이지'일 가능성이 높은 링크를 점수 매겨서 하나 고름.
    """
    soup = BeautifulSoup(html, "html.parser")
    base_host = urlparse(base_url).netloc.split(":")[0]
    candidates = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # full URL 만들고 host 비교
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        host = parsed.netloc.split(":")[0]

        # 외부 도메인은 일단 패스 (쇼핑몰이 완전 다른 도메인인 케이스는 나중에 필요하면 풀자)
        if host and host != "" and host != base_host:
            continue

        low = full_url.lower()
        text = (a.get_text(" ", strip=True) or "").upper()
        target = full_url.upper()

        # 블랙리스트 단어 포함하면 버리기 (로그인/회원가입/카트 등)
        if any(b in text or b in target for b in LINK_BLACKLIST):
            continue

        score = 0

        # 세일 관련 키워드 많을수록 점수 ↑
        for kw in keywords:
            up = kw.upper()
            if up in text or up in target:
                score += 5

        # URL 패턴 점수 조정
        # 카테고리/리스트/컬렉션 페이지 선호
        if "cate_no=" in low or "category" in low or "collection" in low or "product/list" in low:
            score += 3

        # product detail / 단일 상품 페이지는 약간 패널티
        if ("product/detail" in low or "product_no=" in low) and "list" not in low:
            score -= 2

        # 점수가 0 이하이면 후보에서 제외
        if score <= 0:
            continue

        # 텍스트가 너무 긴 버튼/메뉴보다는 적당히 짧은 쪽 선호
        candidates.append((score, len(text), full_url))

    if not candidates:
        return None

    # 점수 높은 순, 텍스트 짧은 순
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


def detect_sale_for_brand(row):
    brand = (row.get("brand") or "").strip()
    url = (row.get("official_url") or row.get("url") or "").strip()
    enabled = (row.get("enabled") or "TRUE").strip().lower()
    override = (row.get("keywords_override") or "").strip()
    sale_url_override = (row.get("sale_url_override") or "").strip()

    if enabled in ("false", "0", "no"):
        return None

    # 키워드 셋 구성
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

        # 1) 시트에 override가 있으면 무조건 그걸 우선 사용
        if sale_url_override:
            sale_url = sale_url_override

        # 2) override 없고, 세일로 감지되면 자동으로 세일 링크 탐색
        elif status == "sale":
            sale_url = find_sale_link(html, url, LINK_SALE_KEYWORDS)

        # 3) 그래도 못 찾으면 최소 공홈이라도
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
        time.sleep(1)  # 너무 빨리 돌지 않게

    out = {"generated_at": now, "sales": results}
    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
