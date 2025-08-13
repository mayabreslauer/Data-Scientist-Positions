import os
import requests
import csv
import html
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone

TIMEOUT = 30

FIELDS = [
    "id", "title", "location", "date_posted", "link", "source", "scraped_at",
    "About Us", "About the Role", "What You'll Be Doing", "Qualifications",
    "Benefits", "Life at", "General",
    "status", "stale_at",
]

def ensure_schema(row: dict) -> dict:
    for c in FIELDS:
        row.setdefault(c, "")
    return row

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

def parse_greenhouse_description(raw_html: str) -> dict:
    decoded = html.unescape(raw_html or "")
    soup = BeautifulSoup(decoded, "html.parser")

    sections = {}
    curr = "General"
    buffer = []

    for tag in soup.find_all(['h2', 'h3', 'p', 'li']):
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        if tag.name in ['h2', 'h3']:
            if buffer:
                sections[curr] = "\n".join(buffer).strip()
                buffer = []
            ttl = txt.lower()
            if "about" in ttl and ("role" in ttl or "position" in ttl):
                curr = "About the Role"
            elif "about" in ttl and any(k in ttl for k in ["riskified", "us", "company", "team"]):
                curr = "About Us"
            elif any(k in ttl for k in ["responsibil", "what you'll be doing", "what you will do", "day-to-day", "what you’ll do"]):
                curr = "What You'll Be Doing"
            elif any(k in ttl for k in ["requirement", "qualification", "qualifications", "skills", "must have", "nice to have"]):
                curr = "Qualifications"
            elif any(k in ttl for k in ["benefit", "perks", "why you'll love", "why you’ll love", "compensation"]):
                curr = "Benefits"
            elif "life at" in ttl:
                curr = "Life at"
            else:
                curr = txt
        else:
            buffer.append(txt)

    if buffer:
        sections[curr] = "\n".join(buffer).strip()

    normalized = {}
    for k, v in sections.items():
        kl = k.lower()
        if kl.startswith("about the role"):
            key = "About the Role"
        elif kl.startswith("about us") or "about riskified" in kl or (kl.startswith("about") and any(t in kl for t in ["company", "team"])):
            key = "About Us"
        elif "life at" in kl:
            key = "Life at"
        elif "responsibil" in kl or "what you'll be doing" in kl or "what you will do" in kl or "what you’ll do" in kl or "day-to-day" in kl:
            key = "What You'll Be Doing"
        elif "qualif" in kl or "requirement" in kl or "skills" in kl or "must have" in kl or "nice to have" in kl:
            key = "Qualifications"
        elif "benefit" in kl or "perks" in kl or "compensation" in kl:
            key = "Benefits"
        else:
            key = k
        normalized[key] = v

    return normalized

def scrape_riskified_jobs(keyword: str = "Data Scientist"):
    url = "https://boards-api.greenhouse.io/v1/boards/riskified/jobs"
    resp = requests.get(url, params={"content": "true"}, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    scraped_time = utc_now_iso()

    for job in data.get("jobs", []):
        title = job.get("title", "") or ""
        location_name = (job.get("location") or {}).get("name", "")

        if keyword.lower() not in title.lower():
            continue
        if not is_israel_location(location_name):
            continue

        sections = parse_greenhouse_description(job.get("content", ""))
        row = {
            "id": job.get("id"),
            "title": title,
            "location": location_name,
            "date_posted": pick_date(job),
            "link": job.get("absolute_url"),
            "source": "Riskified Careers",
            "scraped_at": scraped_time,
        }
        for k in ["About Us", "About the Role", "What You'll Be Doing", "Qualifications", "Benefits", "Life at", "General"]:
            if k in sections:
                row[k] = sections[k]
        jobs.append(ensure_schema(row))

    print(f"Found {len(jobs)} 'Data Scientist' positions at Riskified (Israel only)")
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
    out_path = "riskified_ds_jobs.csv"
    jobs = scrape_riskified_jobs(keyword="Data Scientist")
    upsert_and_mark_stale(out_path, jobs)
    print("Riskified jobs updated at", out_path)

################### UNIFICATION #####################################

import os, re, glob, html, unicodedata
from datetime import datetime
import pandas as pd

# --------- Configuration ---------
INPUT_GLOBS = [
    "jobs_serper.csv",
    "riskified_ds_jobs.csv",
    "similarweb_ds_parsed.csv",
    "taboola_ds_jobs.csv",
    "melio_ds_jobs.csv",
]
OUTPUT_CSV = "merged_jobs_new.csv"
ENCODINGS = ["utf-8-sig", "utf-8", "cp1255"]  # Try loading with multiple encodings

# --------- Helpers ---------
JOB_LINK_RE = re.compile(r"^https?://(?:[a-z]{2,3}\.)?linkedin\.com/jobs/view/[\w-]*\d+(?:/|$)", re.I)

def canonicalize_job_link(link: str) -> str:
    """Normalize LinkedIn job links to a canonical form."""
    if not isinstance(link, str) or not link:
        return ""
    m = re.search(r"/jobs/view/[\w-]*?(\d+)", link)
    if not m:
        return link
    job_id = m.group(1)
    return f"https://www.linkedin.com/jobs/view/{job_id}/"

def normalize_text(s: str) -> str:
    """Clean text: decode HTML entities, normalize Unicode, remove extra spaces."""
    if s is None: return ""
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r"\s+", " ", s).strip()

def load_csv_any(path: str) -> pd.DataFrame:
    """Try reading CSV with several encodings."""
    last_err = None
    for enc in ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    raise last_err

def infer_origin_from_filename(fname: str) -> str:
    """Guess the source name based on the filename."""
    base = os.path.basename(fname).lower()
    if "serper" in base: return "LinkedIn via Serper"
    if "riskified" in base: return "Riskified Careers"
    if "similarweb" in base: return "SimilarWeb Careers"
    if "taboola" in base: return "Taboola Careers"
    if "melio" in base: return "Melio Careers"
    return base

# --------- Merge ---------
def merge_job_csvs(input_globs=INPUT_GLOBS, output_path=OUTPUT_CSV):
    # Collect all matching files
    files = []
    for pattern in input_globs:
        files.extend(glob.glob(pattern))
    files = sorted(list(dict.fromkeys(files)))  # unique + keep order

    if not files:
        print("No matching files found for merge.")
        return

    frames = []
    for fp in files:
        try:
            df = load_csv_any(fp)
        except Exception as e:
            print(f"Could not read {fp}: {e}")
            continue

        # Keep 'source' if exists; always add 'origin_file'
        df["origin_file"] = os.path.basename(fp)
        if "source" not in df.columns or df["source"].fillna("").eq("").all():
            df["source"] = infer_origin_from_filename(fp)

        # Normalize common text fields
        for col in ["title","company","location","link","date_posted","source"]:
            if col in df.columns:
                df[col] = df[col].map(normalize_text)

        # Canonicalize LinkedIn links
        if "link" in df.columns:
            df["link_canonical"] = df["link"].map(canonicalize_job_link)
        else:
            df["link_canonical"] = ""

        frames.append(df)

    if not frames:
        print("No data loaded for merging.")
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)

    # Deduplicate: first by 'link_canonical', then by (title, company, location)
    merged["__dedup_key"] = merged["link_canonical"]
    fallback_mask = merged["__dedup_key"].eq("") | merged["__dedup_key"].isna()
    merged.loc[fallback_mask, "__dedup_key"] = (
        merged.get("title", "").astype(str).str.lower().fillna("") + "||" +
        merged.get("company", "").astype(str).str.lower().fillna("") + "||" +
        merged.get("location", "").astype(str).str.lower().fillna("")
    )

    before = len(merged)
    merged = merged.drop_duplicates(subset=["__dedup_key"], keep="first")
    after = len(merged)

    # Preferred column order; keep other existing columns afterwards
    preferred_cols = [
        "id","title","company","location","date_posted",
        "is_open","status","stale_at",
        "source","origin_file","link","link_canonical",
        "About Us","About the Role","What You'll Be Doing",
        "Qualifications","Life at","In the News",
        "raw_description_html","scraped_at"
    ]
    other_cols = [c for c in merged.columns if c not in preferred_cols + ["__dedup_key"]]
    merged = merged[ [c for c in preferred_cols if c in merged.columns] + other_cols ]

    # Save output
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")

    # Summary stats
    n_sources = merged["source"].nunique() if "source" in merged.columns else "n/a"
    print(f"Saved '{output_path}' | Rows: {after} (removed duplicates: {before-after}) | Unique sources: {n_sources}")

if __name__ == "__main__":
    merge_job_csvs()

