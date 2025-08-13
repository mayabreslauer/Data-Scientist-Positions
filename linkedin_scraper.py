import re
import time
import json
import html
import csv
import os
import unicodedata
import datetime as dt
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  
except Exception:
    ZoneInfo = None

SERPER_KEY = os.getenv("SERPER_API_KEY")
if not SERPER_KEY:
    raise RuntimeError("Missing SERPER_API_KEY env var")

TIMEOUT = 30
SERPER_HEADERS = {"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"}
UA = "Mozilla/5.0 (compatible; JobScraper/1.0; +https://example.com/bot)"  

CSV_PATH = "jobs_serper.csv"
MAX_PAGES = 8
SLEEP_BETWEEN = 0.6

DETAIL_ENRICH_BUDGET = 120
DETAIL_SLEEP = 0.9

DEBUG = False
def dbg(*args):
    if DEBUG: print(*args)

# ========= Israel =========
COUNTRY_EN = "Israel"
COUNTRY_HE = "ישראל"

IL_TOKENS = [
    "israel","ישראל",
    "tel aviv","תל אביב","gush dan","גוש דן","ramat gan","רמת גן","givatayim","גבעתיים",
    "bnei brak","בני ברק","herzliya","הרצליה","kfar saba","כפר סבא","raanana","רעננה",
    "hod hasharon","הוד השרון","petah tikva","פתח תקווה","rishon lezion","ראשון לציון",
    "holon","חולון","bat yam","בת ים","rehovot","רחובות","modiin","מודיעין","ashdod","אשדוד",
    "haifa","חיפה","kiryat","קריית","karmiel","כרמיאל","nahariya","נהריה","hadera","חדרה",
    "zichron","זכרון","yokneam","יקנעם","nesher","נשר",
    "beer sheva","באר שבע","ashkelon","אשקלון","dimona","דימונה","sderot","שדרות",
    "jerusalem","ירושלים","maale adumim","מעלה אדומים","beit shemesh","בית שמש",
    "remote israel","remote in israel","hybrid israel","היברידי ישראל","עבודה מרחוק בישראל",
]

def is_israel(text: str) -> bool:
    if not text:
        return False
    return any(tok in text.lower() for tok in IL_TOKENS)

# Position Link
JOB_LINK_RE = re.compile(r"^https?://(?:[a-z]{2,3}\.)?linkedin\.com/jobs/view/[\w-]*\d+(?:/|$)", re.I)
def canonicalize_job_link(link: str) -> str:
    m = re.search(r"/jobs/view/[\w-]*?(\d+)", link or "")
    if not m:
        return link or ""
    job_id = m.group(1)
    return f"https://www.linkedin.com/jobs/view/{job_id}/"

# ========= Helpers =========
def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\t"))
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_target_title(text: str) -> bool:
    if not text: return False
    t = text.lower()
    return "data scientist" in t or "מדען נתונים" in t

def clean_html_to_text(raw_html: str) -> str:
    if not raw_html: return ""
    soup = BeautifulSoup(html.unescape(raw_html), "html.parser")
    return soup.get_text(separator="\n", strip=True)

def split_sections_free(text: str) -> dict:
    sections = {"about_role":"", "responsibilities":"", "qualifications":"", "benefits":""}
    if not text: return sections
    text = re.sub(r"[ \t]+\n", "\n", text)
    anchors = {
        "about_role":[
            r"about the role", r"role overview", r"in this (position|role)", 
            r"your mission", r"as a ", r"we (are|'re|re) looking for",
            r"job description", r"position overview"
        ],
        "responsibilities":[
            r"key responsibilities", r"what you('?|')ll be doing", 
            r"what you will be doing", r"day[- ]to[- ]day", 
            r"what you will do", r"responsibilities include",
            r"your responsibilities", r"main duties"
        ],
        "qualifications":[
            r"requirements", r"qualifications", r"ideal candidate", 
            r"must have", r"needed", r"skills", r"experience required",
            r"what we('?|')re looking for", r"required skills"
        ],
        "benefits":[
            r"why (you('?|')ll )?love", r"benefits", r"perks",
            r"what we offer", r"package includes"
        ]
    }
    hits=[]
    for key,pats in anchors.items():
        for pat in pats:
            for m in re.finditer(pat, text, re.IGNORECASE):
                hits.append((m.start(), key))
    if not hits:
        sections["about_role"]=text.strip(); return sections
    hits.sort(key=lambda x:x[0]); hits.append((len(text), None))
    for i in range(len(hits)-1):
        start,key = hits[i]; end,_ = hits[i+1]
        chunk = text[start:end].strip()
        if key:
            for pat in anchors[key]:
                m2 = re.search(pat, chunk, re.IGNORECASE)
                if m2: chunk = chunk[m2.end():].lstrip(); break
            sections[key] = (sections[key]+"\n"+chunk).strip() if sections[key] else chunk
    return sections

def ensure_schema(row: dict) -> dict:
    cols = [
        "id","title","company","location","date_posted","link","source",
        "About Us","About the Role","What You'll Be Doing","Qualifications",
        "Life at","In the News","raw_description_html","scraped_at",
        "is_open","status","stale_at"
    ]
    for c in cols: row.setdefault(c, "")
    return row

# ========= Serper =========
def serper_site_search(query: str, start: int = 0):
    if not SERPER_KEY:
        raise RuntimeError("SERPER_API_KEY environment variable not set")
    url = "https://google.serper.dev/search"
    
    payload = {"q": query, "start": start, "hl": "en", "gl": "il"}
    try:
        r = requests.post(url, headers=SERPER_HEADERS, json=payload, timeout=TIMEOUT)
        r.raise_for_status()
        return (r.json() or {}).get("organic", []) or []
    except Exception as e:
        print(f"Serper search error: {e}")
        return []

# ========= Date =========
REL_EN = re.compile(r"\b(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago\b", re.I)
REL_HE = re.compile(r"(?:לפני)\s+(\d+)\s+(שניות|שניה|דקות|דקה|שעות|שעה|ימים|יום|שבועות|שבוע|חודשים|חודש|שנים|שנה)")

def _rel_to_timedelta(n: int, unit: str) -> dt.timedelta:
    unit = unit.lower()
    he = {"שניה":"second","שניות":"second","דקה":"minute","דקות":"minute","שעה":"hour","שעות":"hour",
          "יום":"day","ימים":"day","שבוע":"week","שבועות":"week","חודש":"month","חודשים":"month",
          "שנה":"year","שנים":"year"}
    base = he.get(unit, unit)
    if base.startswith("second"): return dt.timedelta(seconds=n)
    if base.startswith("minute"): return dt.timedelta(minutes=n)
    if base.startswith("hour"):   return dt.timedelta(hours=n)
    if base.startswith("day"):    return dt.timedelta(days=n)
    if base.startswith("week"):   return dt.timedelta(weeks=n)
    if base.startswith("month"):  return dt.timedelta(days=30*n)
    if base.startswith("year"):   return dt.timedelta(days=365*n)
    return dt.timedelta(days=n)

def _parse_relative(text: str) -> str:
    if not text: return ""
    t = text.strip().lower()
    now = dt.datetime.utcnow().replace(microsecond=0, tzinfo=dt.timezone.utc)
    if "today" in t or "היום" in t: return now.isoformat()
    if "yesterday" in t or "אתמול" in t: return (now - dt.timedelta(days=1)).isoformat()
    m = REL_EN.search(t)
    if m:
        return (now - _rel_to_timedelta(int(m.group(1)), m.group(2))).isoformat()
    m = REL_HE.search(t)
    if m:
        return (now - _rel_to_timedelta(int(m.group(1)), m.group(2))).isoformat()
    return ""

def coerce_date_to_iso(date_raw: str, soup: BeautifulSoup, page_text: str) -> str:
    if date_raw:
        dr = date_raw.strip()
        try:
            d = dt.datetime.fromisoformat(dr.replace("Z","+00:00"))
            return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            pass
        try:
            d = dt.datetime.strptime(dr, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
            return d.replace(microsecond=0).isoformat()
        except Exception:
            pass
        rel = _parse_relative(dr)
        if rel: return rel

    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        try:
            d = dt.datetime.fromisoformat(t["datetime"].replace("Z","+00:00"))
            return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            pass

    if page_text:
        for sel in [".posted-time-ago__text",".jobs-unified-top-card__posted-date",
                    "[data-test-posted-time-ago]","[class*='posted']"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                rel = _parse_relative(el.get_text(" ", strip=True))
                if rel: return rel
    return ""

# ========= Location =========
CITY_NAMES = [
    "Tel Aviv"
    ,"Jerusalem","Haifa","Beer Sheva","Herzliya","Ramat Gan","Givatayim",
    "Petah Tikva","Rishon Lezion","Rehovot","Netanya","Holon","Ashdod","Ashkelon",
    "Hadera","Modiin","Kfar Saba","Raanana","Yokneam","Beit Shemesh",
    "תל אביב","ירושלים","חיפה","באר שבע","הרצליה","רמת גן","גבעתיים","פתח תקווה",
    "ראשון לציון","רחובות","נתניה","חולון","אשדוד","אשקלון","חדרה","מודיעין",
    "כפר סבא","רעננה","יקנעם","בית שמש"
]
LOC_BLOCK_RE = re.compile(r"([A-Za-z ]+,\s*(?:[A-Za-z ]+,\s*)?(?:Israel))", re.I)

def extract_location_from_texts(*texts: str) -> str:
    for t in texts:
        if not t: continue
        norm = normalize_text(t)
        m = LOC_BLOCK_RE.search(norm)
        if m: return m.group(1)
        for city in CITY_NAMES:
            if city.lower() in norm.lower():
                if "israel" in norm.lower() or "ישראל" in norm.lower():
                    return f"{city}, Israel" if city.isascii() else f"{city}, ישראל"
                return city
    return ""

def infer_city_from_query(query: str) -> str:
    q = query.lower()
    for city in CITY_NAMES:
        if city.lower() in q: return city
    if "israel" in q or "ישראל" in q: return "Israel"
    return ""

def extract_location(soup: BeautifulSoup, json_ld: dict, og_title: str, og_desc: str) -> str:
    if isinstance(json_ld, dict):
        loc = json_ld.get("jobLocation") or {}
        if isinstance(loc, list) and loc: loc = loc[0]
        if isinstance(loc, dict):
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                parts = [addr.get("addressLocality",""), addr.get("addressRegion",""), addr.get("addressCountry","")]
                cand = normalize_text(" ".join([p for p in parts if p]))
                if cand: return cand
    for sel in [".jobs-unified-top-card__primary-description",
                ".jobs-unified-top-card__subtitle-primary-group > div",
                ".top-card-layout__second-subline > li",
                ".jobs-unified-top-card__bullet",".jobs-details-top-card__bullet"]:
        el = soup.select_one(sel)
        if el:
            cand = normalize_text(el.get_text(" ", strip=True)).replace("•"," ").strip()
            if len(cand) >= 2: return cand
    for og in (og_title, og_desc):
        if og:
            parts = [p.strip() for p in og.split(" - ") if p.strip()]
            if len(parts) >= 3:
                return normalize_text(parts[-1])
    return ""

# ========= Analysis of Linkedin Page =========
JSONLD_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I|re.S)

def parse_jsonld(html_text: str) -> dict:
    for m in JSONLD_RE.finditer(html_text or ""):
        try: obj = json.loads(m.group(1))
        except Exception: continue
        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, dict) and it.get("@type","").lower()=="jobposting":
                    return it
        elif isinstance(obj, dict) and obj.get("@type","").lower()=="jobposting":
            return obj
    return {}

def extract_company_from_page(soup: BeautifulSoup, json_ld: dict) -> str:
    company=""
    if isinstance(json_ld, dict):
        org=json_ld.get("hiringOrganization") or {}
        if isinstance(org, dict):
            company=normalize_text(org.get("name") or "")
            if company: return company
    meta = soup.find("meta", attrs={"name":"twitter:title"}) or soup.find("meta", attrs={"property":"og:title"})
    if meta and meta.get("content"):
        parts=[p.strip() for p in normalize_text(meta["content"]).split(" - ") if p.strip()]
        if len(parts)>=2: return parts[1]
    for sel in [
        "a[data-tracking-control-name*=company]",
        ".jobs-unified-top-card__company-name a",
        ".job-details-company-name",
        ".jobs-details-top-card__company-url",
        "h4 a[href*='/company/']"
    ]:
        el=soup.select_one(sel)
        if el:
            txt=normalize_text(el.get_text())
            if txt: return txt
    bc=soup.select_one(".jobs-details-top-card__breadcrumbs")
    if bc:
        for a in bc.find_all("a"):
            if "/company/" in (a.get("href") or ""):
                txt=normalize_text(a.get_text())
                if txt: return txt
    return ""

def is_job_active(soup: BeautifulSoup, json_ld: dict, page_text: str) -> bool:
    pt = page_text.lower()
    closed = [
        "no longer accepting applications","this job is no longer available",
        "position has been filled","applications are closed","we are no longer reviewing applications",
        "המשרה אינה זמינה","המשרה לא זמינה","הגשת מועמדות הושעתה","סגורה להגשה"
    ]
    if any(x in pt for x in closed): return False
    if isinstance(json_ld, dict) and json_ld.get("validThrough"):
        try:
            until = dt.datetime.fromisoformat(json_ld["validThrough"].replace("Z","+00:00"))
            if until < dt.datetime.utcnow().replace(tzinfo=until.tzinfo): return False
        except Exception: pass
    apply_btn = any(soup.select_one(s) for s in [
        "a[data-control-name*=apply]","button.apply-button",".jobs-apply-button",
        "a[href*='apply']","button[data-tracking-control-name*=apply]"
    ])
    apply_text = any(k in pt for k in ["easy apply","apply now","submit application","הגש מועמדות","הגשת מועמדות"])
    return apply_btn or apply_text or ("no longer" not in pt and "אינה זמינה" not in pt)

def enrich_from_linkedin(link: str, serper_title: str = "") -> dict:
    try:
        response = requests.get(link, timeout=TIMEOUT, headers={"User-Agent": UA})
        response.raise_for_status()
    except Exception as e:
        dbg("detail fetch failed:", e)
        return {"is_open": False}

    html_text = response.text
    soup = BeautifulSoup(html_text, "html.parser")
    page_text = soup.get_text(" ")

    json_ld = parse_jsonld(html_text)

    title = ""
    if isinstance(json_ld, dict) and json_ld.get("title"):
        title = normalize_text(json_ld["title"])
    if not title:
        og_title_meta = soup.find("meta", attrs={"property": "og:title"})
        if og_title_meta and og_title_meta.get("content"):
            og_content = normalize_text(og_title_meta.get("content"))
            title = og_content.split(" - ")[0].strip() if " - " in og_content else og_content

    company = extract_company_from_page(soup, json_ld)

    og_title_str = ""
    og_desc_str  = ""
    og_title = soup.find("meta", attrs={"property":"og:title"})
    if og_title and og_title.get("content"):
        og_title_str = normalize_text(og_title["content"])
    og_desc = soup.find("meta", attrs={"property":"og:description"})
    if og_desc and og_desc.get("content"):
        og_desc_str = normalize_text(og_desc["content"])

    location = extract_location(soup, json_ld, og_title_str, og_desc_str)
    is_active = is_job_active(soup, json_ld, page_text)

    is_israel_job = (
        is_israel(title) or is_israel(company) or is_israel(location) or
        is_israel(og_title_str) or is_israel(og_desc_str) or
        " israel " in page_text.lower() or " ישראל " in page_text
    )

    description_html = ""
    if isinstance(json_ld, dict) and json_ld.get("description"):
        description_html = json_ld["description"]
    if not description_html:
        for selector in [
            "div.show-more-less-html__markup","div.description__text","div.jobs-description-content__text",
            "div.jobs-box__html-content",".jobs-description__content"
        ]:
            element = soup.select_one(selector)
            if element:
                description_html = str(element)
                break

    employment_type = normalize_text(json_ld.get("employmentType","")) if isinstance(json_ld, dict) else ""
    date_raw = (json_ld.get("datePosted") or "").strip() if isinstance(json_ld, dict) else ""
    date_posted_iso = coerce_date_to_iso(date_raw, soup, page_text)

    return {
        "is_open": bool(is_active and is_israel_job),
        "title": title,
        "company": company,
        "location": location,
        "date_posted": date_posted_iso,
        "description_html": description_html,
        "employment_type": employment_type,
        "is_israel": is_israel_job
    }

# ========= Scan =========
def crawl_serper(max_pages=MAX_PAGES, sleep_between=SLEEP_BETWEEN):
    rows = []
    seen_links = set()
    enrich_left = DETAIL_ENRICH_BUDGET

    city_terms = [
        '"Tel Aviv"','"Jerusalem"','"Haifa"','"Beer Sheva"','"Herzliya"','"Ramat Gan"','"Givatayim"',
        '"Petah Tikva"','"Rishon Lezion"','"Rehovot"','"Netanya"','"Holon"','"Ashdod"','"Ashkelon"',
        '"Hadera"','"Modiin"','"Kfar Saba"','"Raanana"','"Yokneam"','"Beit Shemesh"'
    ]
    queries = [
        f'site:linkedin.com/jobs/view "Data Scientist" {COUNTRY_EN}',
        f'site:il.linkedin.com/jobs/view "Data Scientist" {COUNTRY_EN}',
        f'site:linkedin.com/jobs/view "מדען נתונים" {COUNTRY_HE}',
        f'site:il.linkedin.com/jobs/view "מדען נתונים" {COUNTRY_HE}',
        'site:linkedin.com/jobs/view "Data Scientist" "Remote Israel"',
        'site:il.linkedin.com/jobs/view "Data Scientist" "Remote Israel"',
    ]
    for ct in city_terms:
        queries.append(f'site:linkedin.com/jobs/view "Data Scientist" {ct}')
        queries.append(f'site:il.linkedin.com/jobs/view "Data Scientist" {ct}')

    total_processed = 0
    total_kept = 0

    for query_idx, query in enumerate(queries):
        print(f"\n[Query {query_idx + 1}/{len(queries)}] {query}")
        for page in range(max_pages):
            print(f"  Page {page + 1}...")
            items = serper_site_search(query=query, start=page * 10)
            if not items:
                print("    No more results for this query")
                break

            page_processed = 0
            page_kept = 0

            for item in items:
                link_raw = normalize_text(item.get("link") or "")
                if not link_raw:
                    continue
                link = canonicalize_job_link(link_raw)

                if not JOB_LINK_RE.match(link):
                    dbg(f"      X Not LinkedIn job link: {link_raw}")
                    continue
                if link in seen_links:
                    dbg(f"      X Already seen: {link}")
                    continue
                seen_links.add(link)

                serper_title = normalize_text(item.get("title") or "")
                serper_snippet = normalize_text(item.get("snippet") or "")

                if not (is_target_title(serper_title) or is_target_title(serper_snippet)):
                    continue

                total_processed += 1
                page_processed += 1

                if enrich_left > 0:
                    job_info = enrich_from_linkedin(link, serper_title)
                    open_flag = bool(job_info.get("is_open", False))
                    if not open_flag or not job_info.get("is_israel", False):
                        dbg("      X Skip (closed or not Israel)")
                        continue

                    title = job_info.get("title") or serper_title
                    company = job_info.get("company", "")
                    location = job_info.get("location", "")
                    date_posted = job_info.get("date_posted", "")
                    desc_html = job_info.get("description_html", "")
                    is_open_val = "true"

                    if not location:
                        loc_from_result = extract_location_from_texts(serper_title, serper_snippet)
                        location = loc_from_result or infer_city_from_query(query)

                    enrich_left -= 1
                    time.sleep(DETAIL_SLEEP)
                else:
                    if not (is_israel(serper_title) or is_israel(serper_snippet)):
                        continue
                    title = serper_title
                    company = ""
                    desc_html = ""
                    date_posted = ""
                    location = extract_location_from_texts(serper_title, serper_snippet) or infer_city_from_query(query)
                    is_open_val = "unknown"

                desc_text = clean_html_to_text(desc_html)
                sections = split_sections_free(desc_text)

                row = ensure_schema({
                    "id": f"serper:{abs(hash(link))}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": date_posted,
                    "link": link,
                    "source": "google_search/serper+linkedin",
                    "raw_description_html": desc_html,
                    "scraped_at": utc_now_iso(),
                    "About Us": sections.get("about_role", ""),
                    "About the Role": sections.get("about_role", ""),
                    "What You'll Be Doing": sections.get("responsibilities", ""),
                    "Qualifications": sections.get("qualifications", ""),
                    "Life at": sections.get("benefits", ""),
                    "In the News": "",
                    "is_open": is_open_val,
                })
                
                rows.append(row)
                total_kept += 1
                page_kept += 1
                print(f"    V {company or '[unknown]'} — {title[:60]} | {location or 'N/A'} | {date_posted or 'no date'} | open={is_open_val}")

            print(f"    Page summary: processed={page_processed}, kept={page_kept}")
            if SLEEP_BETWEEN>0: time.sleep(SLEEP_BETWEEN)

    print("\n[Crawl done]")
    print(f"Total processed: {total_processed}")
    print(f"Total kept: {total_kept}")
    print(f"Detail budget used: {DETAIL_ENRICH_BUDGET - enrich_left}/{DETAIL_ENRICH_BUDGET}")
    return rows

# ========= Save: upsert + stale =========
def upsert_and_mark_stale(path, new_rows):
    base_fields = [
        "id","title","company","location","date_posted","link","source",
        "About Us","About the Role","What You'll Be Doing","Qualifications",
        "Life at","In the News","raw_description_html","scraped_at",
        "is_open","status","stale_at"
    ]

    def _canon_link(x):
        try: return canonicalize_job_link(x or "")
        except Exception: return x or ""

    prepared_new = []
    for r in new_rows:
        rr = ensure_schema(dict(r))
        rr["link"] = _canon_link(rr.get("link",""))
        if "status" not in rr: rr["status"] = ""
        if "stale_at" not in rr: rr["stale_at"] = ""
        prepared_new.append(rr)

    existing = []
    if os.path.exists(path):
        try:
            import pandas as pd
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(path, encoding="utf-8")
            existing = df.to_dict(orient="records")
        except Exception:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                existing = list(reader)

    existing_by_link = {}
    all_fields = set(base_fields)
    for row in existing:
        for k in ("is_open","status","stale_at"): row.setdefault(k, "")
        row["link"] = _canon_link(row.get("link",""))
        existing_by_link[row.get("link","")] = row
        all_fields.update(row.keys())

    current_links = {_canon_link(r.get("link","")) for r in prepared_new if r.get("link")}

    merged_rows = []
    now = utc_now_iso()

    for link, row in existing_by_link.items():
        row = dict(row)  # copy
        if link and (link not in current_links):
            row["status"] = "stale"
            if "is_open" in row and str(row["is_open"]).lower() != "false":
                row["is_open"] = "false"
            row["stale_at"] = now
        merged_rows.append(row)

    #only new rows
    new_count = 0
    for rr in prepared_new:
        link = rr.get("link","")
        if link and (link not in existing_by_link):
            rr["status"] = "active"
            rr["stale_at"] = ""
            merged_rows.append(rr)
            new_count += 1
            all_fields.update(rr.keys())

    #save
    fieldnames = list(base_fields)
    for col in sorted(all_fields):
        if col not in fieldnames:
            fieldnames.append(col)

    #writing to file
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in merged_rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    stale_count = sum(1 for l in existing_by_link if l not in current_links)
    print(f"V Upsert complete. Wrote {len(merged_rows)} rows to {path} (new={new_count}, stale={stale_count})")

# ========= Main =========
def main():
    print("Starting LinkedIn Data Scientist scraper (Israel only) — with upsert & stale marking")
    rows = crawl_serper(max_pages=MAX_PAGES, sleep_between=SLEEP_BETWEEN)
    upsert_and_mark_stale(CSV_PATH, rows)
    print(f"Saved/merged into: {CSV_PATH}")

if __name__ == "__main__":
    main()
