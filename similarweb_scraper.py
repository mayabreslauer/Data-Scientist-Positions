import os
import requests
import csv
import html
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def pick_date(job: dict) -> str:
    raw = (job.get("updated_at") or job.get("created_at") or "").strip()
    if not raw:
        return ""
    try:
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(raw)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return raw

FIELDS = [
    "id", "title", "location", "date_posted", "link", "source", "scraped_at",
    "About Us", "About the Role", "What You'll Be Doing",
    "Qualifications", "Life at", "In the News",
    "status", "stale_at",
]

def ensure_schema(row: dict) -> dict:
    for c in FIELDS:
        row.setdefault(c, "")
    return row

IL_STRICT_TOKENS = {
    "israel", "ישראל",
    "remote - israel", "remote israel", "hybrid - israel", "hybrid israel",
    "work from israel", "work from home israel", "עבודה מרחוק ישראל", "היברידי ישראל"
}
IL_CITY_TOKENS = {
    "tel aviv", "תל אביב", "jerusalem", "ירושלים", "haifa", "חיפה",
    "beer sheva", "באר שבע", "herzliya", "הרצליה", "ramat gan", "רמת גן",
    "givatayim", "גבעתיים", "petah tikva", "פתח תקווה", "rishon lezion", "ראשון לציון",
    "rehovot", "רחובות", "netanya", "נתניה", "holon", "חולון",
    "ashdod", "אשדוד", "ashkelon", "אשקלון", "hadera", "חדרה",
    "modiin", "מודיעין", "kfar saba", "כפר סבא", "raanana", "רעננה",
    "yokneam", "יקנעם", "beit shemesh", "בית שמש", "nahariya", "נהריה",
    "karmiel", "כרמיאל", "hod hasharon", "הוד השרון", "bat yam", "בת ים",
    "kiryat", "קריית"
}
def is_israel_location(loc: str) -> bool:
    if not loc:
        return False
    t = loc.lower()
    if any(tok in t for tok in IL_STRICT_TOKENS):
        return True
    return any(tok in t for tok in IL_CITY_TOKENS)

def parse_greenhouse_to_riskified_format2(raw_html):
    decoded = html.unescape(raw_html or "")
    soup = BeautifulSoup(decoded, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    section_map = {
        "About Us": ["about similarweb", "about us", "who we are", "our company", "similarweb is"],
        "About the Role": ["we are looking for", "we’re looking for", "about the role", "role overview", "why is this role"],
        "What You'll Be Doing": ["key responsibilities", "what you'll be doing", "your role", "day-to-day", "so, what will you be doing"],
        "Qualifications": ["requirements", "qualifications", "ideal candidate", "must have", "needed", "this is the perfect job"],
        "Life at": ["why similarweb", "why you’ll love", "why you'll love", "benefits", "perks", "life at similarweb", "you’ll find a home", "diversity isn’t just"],
        "In the News": ["in the news"]
    }

    sections = {section: "" for section in section_map}
    matches = []
    for section, keywords in section_map.items():
        for kw in keywords:
            m = re.search(re.escape(kw), text, re.IGNORECASE)
            if m:
                matches.append((m.start(), section))

    if not matches:
        return sections

    matches.sort()
    matches.append((len(text), None))
    for i in range(len(matches) - 1):
        start_idx = matches[i][0]
        end_idx = matches[i + 1][0]
        section_name = matches[i][1]
        content = text[start_idx:end_idx].strip()
        if section_name:
            sections[section_name] += content
    return {k: v.strip() for k, v in sections.items()}

def scrape_similarweb_jobs(keyword="Data Scientist"):
    url = "https://boards-api.greenhouse.io/v1/boards/similarweb/jobs"
    params = {"content": "true"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    scraped_time = utc_now_iso()

    for job in data.get("jobs", []):
        title = (job.get("title") or "")
        location_name = (job.get("location") or {}).get("name", "")

        if keyword.lower() not in title.lower():
            continue
        if not is_israel_location(location_name):
            continue

        parsed_desc = parse_greenhouse_to_riskified_format2(job.get("content", ""))
        row = {
            "id": job.get("id"),
            "title": title,
            "location": location_name,
            "date_posted": pick_date(job),
            "link": job.get("absolute_url"),
            "source": "SimilarWeb Careers",
            "scraped_at": scraped_time,
            **parsed_desc
        }
        jobs.append(ensure_schema(row))

    print(f"Found {len(jobs)} 'Data Scientist' positions at Similarweb (Israel only)")
    return jobs

def upsert_and_mark_stale(path: str, new_rows: list):
    existing_rows = []
    if os.path.exists(path):
        try:
            import pandas as pd
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(path, encoding="utf-8")
            existing_rows = df.to_dict(orient="records")
        except Exception:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)

    existing_by_id = {}
    for r in existing_rows:
        r = ensure_schema(dict(r))
        existing_by_id[str(r.get("id", ""))] = r

    current_ids = set(str(r.get("id", "")) for r in new_rows if r.get("id") is not None)

    merged = []
    now = utc_now_iso()
    for rid, row in existing_by_id.items():
        row = ensure_schema(dict(row))
        if rid and (rid not in current_ids):
            row["status"] = "stale"
            row["stale_at"] = now
        merged.append(row)

    new_count = 0
    for row in new_rows:
        rid = str(row.get("id", ""))
        if rid and rid not in existing_by_id:
            new_row = ensure_schema(dict(row))
            new_row["status"] = "active"
            new_row["stale_at"] = ""
            merged.append(new_row)
            new_count += 1

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in merged:
            writer.writerow({k: r.get(k, "") for k in FIELDS})

    stale_count = sum(1 for rid in existing_by_id if rid not in current_ids)
    print(f"Upsert complete. Wrote {len(merged)} rows to {path} (new={new_count}, stale={stale_count})")

if __name__ == "__main__":
    out_path = "similarweb_ds_jobs.csv"
    jobs = scrape_similarweb_jobs(keyword="Data Scientist")
    upsert_and_mark_stale(out_path, jobs)


