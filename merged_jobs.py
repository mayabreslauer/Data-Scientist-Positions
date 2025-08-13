# merge_jobs.py
import os, re, glob, html, unicodedata
from datetime import datetime
import pandas as pd

# --------- Configuration ---------
INPUT_GLOBS = [
    "jobs_serper.csv",
    "riskified_ds_jobs.csv",
    "similarweb_ds_jobs.csv",
    "taboola_ds_jobs.csv",
    "melio_ds_jobs.csv",
]
OUTPUT_CSV = "merged_jobs.csv"
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
