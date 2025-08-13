# app.py — Streamlit dashboard for merged_jobs.csv (bright blurred bg + filters + seniority with years)

import os
import io
import base64
import re
import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st

CSV_PATH = os.environ.get("JOBS_MERGED_CSV", "merged_jobs.csv")

# ---------------------- Background ----------------------
def set_background():
    """
    Bright, gently blurred background behind the app.
    Uses bg.jpg if present; otherwise embeds a soft SVG (no external files needed).
    """
    bg_url = None
    bg_path = os.path.join(os.getcwd(), "bg.jpg")
    if os.path.exists(bg_path):
        with open(bg_path, "rb") as f:
            bg_url = "data:image/jpeg;base64," + base64.b64encode(f.read()).decode("utf-8")
    else:
        # Embedded soft SVG fallback (bright gradient + blurred blobs)
        svg = """
        <svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 1600 1000'>
          <defs>
            <radialGradient id='g' cx='30%' cy='20%' r='80%'>
              <stop offset='0%' stop-color='#ffffff'/>
              <stop offset='100%' stop-color='#f2f6ff'/>
            </radialGradient>
            <filter id='blur' x='-20%' y='-20%' width='140%' height='140%'>
              <feGaussianBlur stdDeviation='34'/>
            </filter>
          </defs>
          <rect width='1600' height='1000' fill='url(#g)'/>
          <circle cx='1250' cy='180' r='200' fill='#eef3ff' filter='url(#blur)'/>
          <circle cx='350' cy='750' r='260' fill='#f9fbff' filter='url(#blur)'/>
          <circle cx='900' cy='520' r='220' fill='#f4f7ff' filter='url(#blur)'/>
        </svg>
        """.strip()
        bg_url = "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("ascii")

    st.markdown(f"""
    <style>
      html, body, .stApp {{
        height: 100%;
        background: transparent !important;
      }}
      .stApp {{
        background-image: url("{bg_url}") !important;
        background-size: cover !important;
        background-position: center center !important;
        background-attachment: fixed !important;
        filter: brightness(1.08);
      }}
      [data-testid="stAppViewContainer"] > .main {{
        background: transparent !important;
      }}
      .block-container {{
        background: rgba(255,255,255,0.84);
        backdrop-filter: blur(0.5);
        border-radius: 14px;
        padding: 1.2rem 1.5rem;
      }}
      section[data-testid="stSidebar"] > div:first-child {{
        background: rgba(255,255,255,0.92) !important;
        backdrop-filter: blur(2px);
      }}
    </style>
    """, unsafe_allow_html=True)

# ---------------------- Utils ----------------------
@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1255"]
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except Exception as e:
            last_err = e
            continue
    if 'df' not in locals():
        raise last_err

    # Normalize common columns
    for col in ["title","company","location","source","status","is_open","origin_file"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # Robust date parsing
    for col in ["date_posted","scraped_at","stale_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Boolean-ish normalize
    if "is_open" in df.columns:
        df["is_open"] = df["is_open"].astype(str).str.lower().isin(["true","1","yes","open","active"])

    # Fallbacks
    for need in ["link","source","company","location"]:
        if need not in df.columns:
            df[need] = ""

    # posted_date (date only) and age_days
    now = pd.Timestamp.utcnow()
    if "date_posted" in df.columns:
        df["posted_date"] = df["date_posted"].dt.date
        df["age_days"] = (now - df["date_posted"]).dt.days
    else:
        df["posted_date"] = pd.NaT
        df["age_days"] = np.nan

    # City heuristic
    def short_loc(s: str) -> str:
        if not isinstance(s, str): return ""
        s = s.replace("•", " ").strip()
        return s.split(",")[0].strip() if "," in s else s
    df["city_hint"] = df["location"].apply(short_loc)

    # Canonical link
    if "link_canonical" not in df.columns and "link" in df.columns:
        df["link_canonical"] = df["link"]

    # Search haystack
    df["role_lc"] = df["title"].fillna("").str.lower()

    # Years of experience (best-effort) from multiple fields
    df["years_required"] = df.apply(
        lambda r: extract_years_from_text(
            " ".join([
                str(r.get("title","")),
                str(r.get("Qualifications","")),
                str(r.get("About the Role","")),
                str(r.get("About Us",""))
            ])
        ),
        axis=1
    )

    # Seniority bucket (title + years)
    df["seniority"] = df.apply(lambda r: seniority_bucket(r.get("title",""), r.get("years_required")), axis=1)

    return df


def format_count(n):
    try: return f"{int(n):,}".replace(",", "\u2009")
    except: return str(n)


def df_to_csv_download(df: pd.DataFrame) -> bytes:
    buff = io.StringIO()
    df.to_csv(buff, index=False, encoding="utf-8-sig")
    return buff.getvalue().encode("utf-8-sig")


# --------- Years of experience extraction ---------
# English patterns like: "3+ years", "at least 2 years", "5 years of experience"
EN_YEARS_RE = re.compile(
    r"""(?:
            at\ least\ \s*(\d{1,2})\s*\+?\s*years? |
            (\d{1,2})\s*\+?\s*years? (?:\sof\s(?:relevant|professional)\s+experience)? |
            (\d{1,2})\s*\+\s*years?
        )""",
    re.IGNORECASE | re.VERBOSE
)

# Hebrew patterns like: "לפחות 3 שנות ניסיון", "5 שנים ניסיון", "שנתיים ניסיון"
HE_YEARS_RE = re.compile(
    r"""(?:
            לפחות\s*(\d{1,2})\s*(?:\+)?\s*(?:שנה|שנים|שנות)\s*ניסיון |
            (\d{1,2})\s*(?:\+)?\s*(?:שנה|שנים|שנות)\s*ניסיון
        )""",
    re.IGNORECASE | re.VERBOSE
)

def extract_years_from_text(text: str) -> float:
    if not text:
        return np.nan
    t = str(text)

    # English matches
    ens = [int(m.group(1) or m.group(2) or m.group(3)) for m in EN_YEARS_RE.finditer(t)]
    # Hebrew matches
    hes = [int(m.group(1) or m.group(2)) for m in HE_YEARS_RE.finditer(t)]

    all_vals = ens + hes
    if not all_vals:
        return np.nan

    # Heuristic: take the MIN years mentioned (entry requirement usually the lower one)
    return float(min(all_vals))


# --------- Seniority bucket (title + years) ---------
def seniority_bucket(title: str, years_required: float) -> str:
    t = (title or "").lower()

    # 1) Management/lead signals from title
    if re.search(r"\b(manager|head|director|vp)\b", t):
        return "Manager+"
    if re.search(r"\b(lead|principal|staff)\b", t):
        return "Lead/Principal"

    # 2) Explicit senior/junior/intern signals from title
    if re.search(r"\b(intern|student|junior|entry)\b", t):
        return "Junior/Entry"
    if re.search(r"\b(senior|sr\.?)\b", t):
        # If title says Senior but years are very low, still treat as Senior.
        return "Senior"

    # 3) Use years if available
    if isinstance(years_required, (int, float)) and not np.isnan(years_required):
        y = float(years_required)
        if y <= 1:
            return "Junior/Entry"
        if 1 < y < 5:
            return "Mid"
        if 5 <= y < 8:
            return "Senior"
        if y >= 8:
            return "Lead/Principal"

    # 4) Fallback
    return "Mid"

# ---------------------- UI ----------------------
st.set_page_config(page_title="Jobs Dashboard — Data Scientist (IL)", page_icon=None, layout="wide")
set_background()

st.title("Data Scientist Jobs — Israel (Merged)")
st.caption("Interactive dashboard built from multiple sources (LinkedIn via Serper + company career pages).")

# Sidebar — data & filters
with st.sidebar:
    st.header("Controls")

    if not os.path.exists(CSV_PATH):
        st.error(f"CSV not found at: {CSV_PATH}")
        st.stop()

    df = load_data(CSV_PATH)
    st.success(f"Loaded {len(df):,} rows from {os.path.basename(CSV_PATH)}")

    # Text search
    q = st.text_input("Keyword in title/description/company", value="", placeholder="e.g., NLP, Computer Vision, Senior…")

    # Company filter
    companies = sorted([c for c in df["company"].dropna().unique() if c])
    sel_companies = st.multiselect("Company", options=companies, default=[])

    # Source filter
    sources = sorted([s for s in df["source"].dropna().unique() if s])
    sel_sources = st.multiselect("Source", options=sources, default=[])

    # City / location
    cities = sorted([c for c in df["city_hint"].dropna().unique() if c])
    sel_cities = st.multiselect("City (heuristic)", options=cities, default=[])

    # Keep only “Only open”
    only_open = st.checkbox("Only open roles", value=True)

    # Date range
    if "date_posted" in df.columns and df["date_posted"].notna().any():
        min_dt = pd.to_datetime(df["date_posted"]).min()
        max_dt = pd.to_datetime(df["date_posted"]).max()
        date_range = st.date_input(
            "Date posted range (UTC)",
            value=(min_dt.date() if pd.notna(min_dt) else None,
                   max_dt.date() if pd.notna(max_dt) else None)
        )
    else:
        date_range = None

# Apply filters
f = df.copy()

if q:
    ql = q.lower().strip()
    hay = (
        f["role_lc"].fillna("")
        + " || " + f["company"].str.lower().fillna("")
        + " || " + f.get("About Us", pd.Series("", index=f.index)).astype(str).str.lower()
        + " || " + f.get("About the Role", pd.Series("", index=f.index)).astype(str).str.lower()
        + " || " + f.get("Qualifications", pd.Series("", index=f.index)).astype(str).str.lower()
    )
    f = f[hay.str.contains(ql, na=False)]

if sel_companies:
    f = f[f["company"].isin(sel_companies)]

if sel_sources:
    f = f[f["source"].isin(sel_sources)]

if sel_cities:
    f = f[f["city_hint"].isin(sel_cities)]

if only_open and "is_open" in f.columns:
    f = f[f["is_open"] == True]

if date_range and "date_posted" in f.columns:
    start, end = date_range if isinstance(date_range, tuple) else (date_range, date_range)
    if start: f = f[f["date_posted"].dt.date >= start]
    if end:   f = f[f["date_posted"].dt.date <= end]

# KPIs
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Total roles (filtered)", format_count(len(f)))
with k2:
    open_count = int(f["is_open"].sum()) if "is_open" in f.columns else np.nan
    st.metric("Open roles", format_count(open_count) if not np.isnan(open_count) else "n/a")
with k3:
    st.metric("Unique companies", format_count(f["company"].nunique()))
with k4:
    st.metric("Unique sources", format_count(f["source"].nunique()))

st.divider()

# Charts (2 up top + seniority)
cc1, cc2 = st.columns(2)

with cc1:
    if "posted_date" in f.columns and f["posted_date"].notna().any():
        g = (f.dropna(subset=["posted_date"])
               .groupby("posted_date", as_index=False)
               .size())
        fig = px.bar(g, x="posted_date", y="size", title="Roles by posted date")
        fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No 'date_posted' available for time-series chart.")

with cc2:
    top_companies = (
        f.groupby("company", as_index=False)
         .size()
         .sort_values("size", ascending=False)
         .head(20)
    )
    if not top_companies.empty:
        fig = px.bar(top_companies, x="company", y="size", title="Top companies (by count)")
        fig.update_layout(xaxis_tickangle=-45, margin=dict(l=10,r=10,t=40,b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No company data to chart.")

st.markdown("### Roles by seniority")
if "seniority" in f.columns and len(f):
    by_sen = (
        f.groupby("seniority", as_index=False)
         .size()
         .sort_values("size", ascending=False)
    )
    if not by_sen.empty:
        fig = px.bar(by_sen, x="seniority", y="size", title=None)
        fig.update_layout(margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No seniority distribution available.")

st.divider()

# Table (paged) + actions
st.subheader("Results")

show_cols = [c for c in ["title","company","location","date_posted","is_open","source","years_required","seniority","link"] if c in f.columns]
if not show_cols:
    show_cols = f.columns.tolist()

def mk_link(url):
    if isinstance(url, str) and url.startswith("http"):
        return f'<a href="{url}" target="_blank">Open</a>'
    return ""

tbl = f.copy()
if "link" in tbl.columns:
    tbl["open"] = tbl["link"].map(mk_link)
    if "link_canonical" in tbl.columns:
        show_cols = [c for c in show_cols if c != "link_canonical"]
    show_cols = [c for c in show_cols if c != "link"] + ["open"]

# Nice date formatting (guarded)
if "date_posted" in tbl.columns and tbl["date_posted"].notna().any():
    try:
        tbl["date_posted"] = tbl["date_posted"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M UTC").fillna("")
    except Exception:
        # if tz_convert fails (naive), just format
        tbl["date_posted"] = pd.to_datetime(tbl["date_posted"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M").fillna("")

st.dataframe(
    tbl[show_cols],
    use_container_width=True,
    height=520,
    column_config={"open": st.column_config.Column("Link")},
)

# Download filtered CSV
st.download_button(
    "Download filtered as CSV",
    data=df_to_csv_download(f.drop(columns=["role_lc"], errors="ignore")),
    file_name="jobs_filtered.csv",
    mime="text/csv",
)

with st.expander("Advanced: show raw JSON (first 100 rows)"):
    st.code(f.head(100).to_json(orient="records", force_ascii=False, indent=2), language="json")
