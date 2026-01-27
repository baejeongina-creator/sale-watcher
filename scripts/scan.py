import csv
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import io

import requests
from bs4 import BeautifulSoup
import datetime as _dt

# =====================
#  기본 설정
# =====================

# 🔗 Google Sheets → CSV 링크 (지금 네 시트)
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

# 페이지 전체에서 "세일 중인지" 감지하는 키워드
# ARCHIVE 는 기본 세일 키워드에서 제외 (트랩 방지)
GLOBAL_KEYWORDS = [
    "SALE", "SEASON OFF", "SEASONAL", "WINTER", "SUMMER", "SPRING", "FALL",
    "CLEARANCE", "FINAL", "LAST CHANCE", "OUTLET",
    "REFURB", "REFURBISHED", "B-GRADE", "SAMPLE",
    "UP TO", "%", "DEAL",
    "세일", "할인", "시즌오프", "클리어런스", "아울렛", "특가", "최대"
]

# 링크가 세일 페이지일 가능성을 보는 키워드
LINK_SALE_KEYWORDS = [
    "SALE", "SEASON", "OFF", "CLEARANCE", "OUTLET",
    "REFURB", "DISCOUNT", "PROMOTION", "EVENT", "WINTER", "SUMMER",
]

# 절대 들어가면 안 되는 링크 (로그인, 회원가입, 카트 등)
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

# =====================
#  날짜 기반 세일 기간 판단
# =====================

# 1) "1.28 - 2.11" / "1.28~2.11"
# 2) "1월 28일 - 2월 11일" / "1월 28일~2월 11일"
DATE_RANGE_PATTERNS = [
    re.compile(r'(\d{1,2})[./]\s*(\d{1,2}).{0,40}?[-~–]\s*(\d{1,2})[./]\s*(\d{1,2})'),
    re.compile(r'(\d{1,2})\s*월\s*(\d{1,2})\s*일.{0,40}?[-~–]\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일'),
]


def _extract_date_range_from_text(text: str):
    """
    텍스트에서 '1.28 - 2.11' / '1월 28일 - 2월 11일' 같은 패턴을 찾아
    (start_date, end_date)를 date 객체로 리턴.
    못 찾거나 이상한 날짜면 None.
    """
    if not text:
        return None

    for pat in DATE_RANGE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue

        sm, sd, em, ed = map(int, m.groups())
        today = _dt.date.today()
        year = today.year

        # 말이 안 되는 month 값(13월, 24월 등)이면 그냥 버린다
        if not (1 <= sm <= 12 and 1 <= em <= 12):
            return None

        try:
            start = _dt.date(year, sm, sd)
            end = _dt.date(year, em, ed)
        except ValueError:
            # 일(day)이 32일 이런 식으로 말이 안 되면 역시 버림
            return None

        # 연말/연초 걸쳐 있는 경우 대략 처리 (예: 12.20 - 1.10)
        if end < start:
            end = _dt.date(year + 1, em, ed)

        return start, end

    return None



def refine_status_with_dates(official_url: str, cur_status: str, timeout: int = 10) -> str:
    """
    현재 status가 'sale'일 때만,
    공홈 HTML에서 날짜 범위를 찾아 세일이 'upcoming' / 'sale' / 'nosale'인지 다시 판단.
    날짜 못 찾으면 원래 status 유지.
    """
    if cur_status != "sale":
        return cur_status

    try:
        resp = requests.get(official_url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        # 공홈을 못 불러오면 그냥 기존 상태 유지
        return cur_status

    text = re.sub(r"\s+", " ", html)

    rng = _extract_date_range_from_text(text)
    if not rng:
        return cur_status

    start, end = rng
    today = _dt.date.today()

    if today < start:
        return "upcoming"
    if today > end:
        return "nosale"
    return "sale"


# =====================
#  Google Sheets → 행 읽기
# =====================

def fetch_rows():
    resp = requests.get(CSV_URL, timeout=20)
    resp.raise_for_status()

    rows = []
    f = io.StringIO(resp.text)
    reader = csv.DictReader(f)

    for row in reader:
        brand = (row.get("brand") or "").strip()
        url = (row.get("official_url") or "").strip()

        # brand / url 둘 중 하나라도 없으면 스킵
        if not brand or not url:
            continue

        enabled = (row.get("enabled") or "").strip().upper()
        if enabled and enabled != "TRUE":
            # enabled 칸이 비어 있으면 기본 TRUE 취급
            continue

        rows.append({
            "brand": brand,
            "official_url": url,
            "logo_url": (row.get("logo_url") or "").strip(),
            "keywords_override": (row.get("keywords_override") or "").strip(),
            "sale_url_override": (row.get("sale_url_override") or "").strip(),
            "detector_group": (row.get("detector_group") or "A").strip().upper(),
            "manual_check": (row.get("manual_check") or "").strip().upper() == "TRUE",
            "notes": (row.get("notes") or "").strip(),
        })

    return rows


# =====================
#  세일 링크 후보 찾기
# =====================

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

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        host = parsed.netloc.split(":")[0]

        # 외부 도메인 링크는 스킵
        if host and host != "" and host != base_host:
            continue

        low = full_url.lower()
        text = (a.get_text(" ", strip=True) or "").upper()
        target = full_url.upper()

        # 블랙리스트면 제외
        if any(b in text or b in target for b in LINK_BLACKLIST):
            continue

        score = 0

        # 세일 관련 키워드 많을수록 점수 ↑
        for kw in keywords:
            up = kw.upper()
            if up in text or up in target:
                score += 5

        # 카테고리/리스트/컬렉션 페이지 선호
        if "cate_no=" in low or "category" in low or "collection" in low or "product/list" in low:
            score += 3

        # 단일 상품 페이지는 살짝 패널티
        if ("product/detail" in low or "product_no=" in low) and "list" not in low:
            score -= 2

        if score <= 0:
            continue

        candidates.append((score, len(text), full_url))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


# =====================
#  브랜드별 세일 감지
# =====================

def detect_sale_for_brand(row):
    brand = (row.get("brand") or "").strip()
    url = (row.get("official_url") or row.get("url") or "").strip()
    override = (row.get("keywords_override") or "").strip()
    sale_url_override = (row.get("sale_url_override") or "").strip()
    group = (row.get("detector_group") or "").strip().upper()
    manual_check = bool(row.get("manual_check"))

    # 키워드 셋 구성
    keywords = GLOBAL_KEYWORDS[:]
    if override:
        # 콤마 or | 둘 다 지원
        tmp = override.replace(",", "|")
        for kw in tmp.split("|"):
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

        # override 있으면 그 링크 우선
        if sale_url_override:
            sale_url = sale_url_override

        # override 없고, 세일로 감지되면 세일 링크 후보 탐색
        elif status == "sale":
            sale_url = find_sale_link(html, url, LINK_SALE_KEYWORDS)

        # 그래도 없으면 공홈
        if not sale_url:
            sale_url = url

    except Exception as e:
        error_msg = str(e)
        status = "error"
        sale_url = url  # 에러여도 공홈은 유지

    # 날짜 기반으로 'upcoming' / 'nosale' 여부 한 번 더 체크
    status = refine_status_with_dates(url, status)

    return {
        "brand": brand,
        "official_url": url,
        "sale_url": sale_url,
        "status": status,
        "matched_keyword": matched_kw,
        "group": group or None,
        "manual_check": manual_check,
        "error": error_msg,
    }


# =====================
#  메인 실행부
# =====================

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
        time.sleep(1)  # 너무 빠르게 때리지 않도록

    out = {
        "generated_at": now,
        "total_brands": len(rows),
        "brand_list": [r["brand"] for r in rows],
        "sales": results,
    }
    with open("docs/sales.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
