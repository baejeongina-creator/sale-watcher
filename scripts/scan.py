import csv, json, re, time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Google Sheets → CSV 링크
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRPol5yt4wsLuE8G-4lgzu1x2I9zo8dLRTHQQ3C7Pc5871wvpcQUHq6pLJS4FUcS05G86VLdKguSf9M/pub?gid=1024238622&single=true&output=csv"

# 페이지 전체에서 "세일 중인지" 감지하는 키워드
# 🔥 ARCHIVE 뺐음 (기본적으로는 세일로 안 본다)
GLOBAL_KEYWORDS = [
    "SALE", "SEASON OFF", "SEASONAL", "WINTER", "SUMMER", "SPRING", "FALL",
    "CLEARANCE", "FINAL", "LAST CHANCE", "OUTLET",
    "REFURB", "REFURBISHED", "B-GRADE", "SAMPLE",
    "UP TO", "%", "DEAL",
    "세일", "할인", "시즌오프", "클리어런스", "아울렛", "특가", "최대"
]

# 링크가 세일 페이지일 가능성을 보는 키워드 (지금은 크게 안 쓰지만 유지)
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

# === 날짜 기반 세일 기간 판단 헬퍼들 ===
import datetime as _dt

# 1) "1.28 - 2.11" / "1.28~2.11" 같은 형식
DATE_RANGE_PATTERNS = [
    re.compile(r'(\d{1,2})[./]\s*(\d{1,2}).{0,40}?[-~–]\s*(\d{1,2})[./]\s*(\d{1,2})'),
    # 2) "1월 28일 - 2월 11일" / "1월 28일~2월 11일" 같은 한글 형식
    re.compile(r'(\d{1,2})\s*월\s*(\d{1,2})\s*일.{0,40}?[-~–]\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일'),
]

def _extract_date_range_from_text(text: str):
    """
    텍스트에서 '1.28 - 2.11' / '1월 28일 - 2월 11일' 같은 패턴을 찾아
    (start_date, end_date)를 date 객체로 리턴.
    못 찾으면 None.
    """
    if not text:
        return None

    for pat in DATE_RANGE_PATTERNS:
        m = pat.search(text)
        if m:
            sm, sd, em, ed = map(int, m.groups())
            today = _dt.date.today()
            year = today.year

            start = _dt.date(year, sm, sd)
            end = _dt.date(year, em, ed)

            # 연말/연초 걸쳐 있는 경우 대략 처리 (예: 12.20 - 1.10)
            if end < start:
                # 오늘이 끝나는 달보다 앞이면, 세일이 이전 해에서 이어진 걸로 가정
                end = _dt.date(year + 1, em, ed)

            return start, end

    return None

def refine_status_with_dates(official_url: str, cur_status: str, timeout: int = 10) -> str:
    """
    현재 status가 'sale'일 때만,
    공홈 HTML에서 날짜 범위를 찾아 세일이 'upcoming' / 'sale' / 'nosale'인지 다시 판단한다.
    날짜를 못 찾으면 원래 status를 그대로 돌려준다.
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

    # 공백을 정리해서 한 줄짜리 텍스트로 만들기
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
# === 날짜 헬퍼 끝 ===


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
    (지금은 UI에서 sale_url을 안 쓰지만, 나중을 위해 유지)
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


def detect_sale_for_brand(row):
    brand = (row.get("brand") or "").strip()
    url = (row.get("official_url") or row.get("url") or "").strip()
    enabled = (row.get("enabled") or "TRUE").strip().lower()
    override = (row.get("keywords_override") or "").strip()
    sale_url_override = (row.get("sale_url_override") or "").strip()
    # group / detector_group 둘 다 지원
    group = (row.get("group") or row.get("detector_group") or "").strip().upper()

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

        # 1) override 있으면 무조건 그 링크 우선
        if sale_url_override:
            sale_url = sale_url_override

        # 2) override 없고, 세일로 감지되면 세일 링크 후보 탐색
        elif status == "sale":
            sale_url = find_sale_link(html, url, LINK_SALE_KEYWORDS)

        # 3) 그래도 없으면 공홈
        if not sale_url:
            sale_url = url

    except Exception as e:
        error_msg = str(e)
        sale_url = url  # 에러여도 공홈은 유지

    # 날짜 기반으로 'upcoming' / 'nosale' 여부 한 번 더 체크
    status = refine_status_with_dates(official_url, status)

    return {
        "brand": brand,
        "official_url": url,
        "sale_url": sale_url,
        "status": status,
        "matched_keyword": matched_kw,
        "group": group or None,
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
