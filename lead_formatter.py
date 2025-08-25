# lead_formatter_v4.py
# Usage:
#   python lead_formatter_v4.py ./scraper_outputs \
#     --out-csv leads.csv --out-jsonl leads.jsonl \
#     --profiles-only \
#     --include-platforms instagram,twitter,facebook,linkedin,youtube \
#     --min-followers instagram:5000,twitter:2000 \
#     --keywords-any travel,himachal
#
# Optional later:
#   --mine-contacts-from-bio   (fills phone from bio only if phone is empty)

import argparse, csv, glob, json, os, re
from urllib.parse import urlparse

# -------------------- helpers --------------------

PLATFORM_HOSTS = {
    "instagram": {"instagram.com"},
    "twitter": {"twitter.com", "x.com"},
    "facebook": {"facebook.com", "fb.com", "m.facebook.com"},
    "linkedin": {"linkedin.com", "www.linkedin.com"},
    "youtube": {"youtube.com", "www.youtube.com", "youtu.be"},
}

URL_RE = re.compile(r'(?i)\b((?:https?://|www\.)[a-z0-9\-._~%]+(?:/[^\s<>"\)]*)?)')
EMAIL_RE = re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
PHONE_SPLIT_RE = re.compile(r"[;,]")

def followers_to_int(x):
    """Convert '405.1K' -> 405100, '2.3M' -> 2300000, '1200' -> 1200."""
    if x is None or x == "":
        return 0
    s = str(x).strip().lower().replace(",", "")
    m = re.match(r"^([\d\.]+)\s*([kmb])?$", s)
    if not m:
        try:
            return int(float(s))
        except:
            return 0
    num = float(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get(suf, 1)
    return int(num * mult)

def canon_url(u: str) -> str:
    """Canonicalize: https + host(lower) + path (no trailing slash)."""
    if not u:
        return ""
    try:
        pu = urlparse(u)
        host = (pu.netloc or "").lower()
        path = (pu.path or "").rstrip("/")
        if not host and pu.path:
            # handle 'www.xyz.com/...' without scheme
            host = pu.path.split("/")[0].lower()
            path = "/" + "/".join(pu.path.split("/")[1:]).rstrip("/")
        return f"https://{host}{path}"
    except:
        return u.strip()

def detect_platform(url: str, fallback_platform: str = "") -> str:
    """Infer platform from URL host, else use provided platform field."""
    host = (urlparse(url).netloc or "").lower()
    for platform, hosts in PLATFORM_HOSTS.items():
        if host in hosts:
            return platform
    return (fallback_platform or "").lower()

def extract_external_links_from_bio(bio: str):
    """Return a unique list of links found in the bio text."""
    if not bio:
        return []
    urls = [m.group(1) for m in URL_RE.finditer(bio)]
    # Normalize: prepend https:// to www. links with no scheme
    normed = []
    seen = set()
    for u in urls:
        if u.lower().startswith("www."):
            u = "https://" + u
        cu = u.strip()
        if cu and cu not in seen:
            seen.add(cu)
            normed.append(cu)
    return normed

def glean_location(item: dict) -> str:
    """
    Best-effort location extraction from common fields.
    Add/adjust keys here to match your scrapers.
    """
    for key in [
        "location", "location_name", "city", "region", "state", "country", "hometown",
        "place", "address", "geo"
    ]:
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, dict) and val:
            parts = [str(v).strip() for v in val.values() if v]
            if parts:
                return ", ".join(parts)
    return ""

def load_json_any(path: str):
    """Load either a JSON array file or NDJSON file."""
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read().strip()
    if not txt:
        return []
    if txt.startswith("["):
        return json.loads(txt)
    # NDJSON
    items = []
    for line in txt.splitlines():
        line = line.strip()
        if line:
            items.append(json.loads(line))
    return items

def _coerce_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        parts = [p.strip() for p in PHONE_SPLIT_RE.split(v) if p.strip()]
        return parts if parts else ([v.strip()] if v.strip() else [])
    return [str(v).strip()]

def _unique(seq):
    out, seen = [], set()
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def pull_email_and_phone(item: dict):
    """
    Only uses fields present in JSON; DOES NOT mine bio for contacts (unless flag is enabled later).
    If nothing is present, returns empty strings.
    """
    # common email-like fields
    email_fields = ["email", "email_id", "emails", "contact_email", "business_email", "public_email"]
    raw_emails = []
    for k in email_fields:
        v = item.get(k)
        if v:
            raw_emails += _coerce_list(v)
    # filter valid emails
    emails = [e for e in raw_emails if EMAIL_RE.fullmatch(e)]
    emails = _unique(emails)
    email = emails[0] if emails else ""

    # common phone-like fields (no heavy normalization)
    phone_fields = ["phone", "phone_number", "phones", "contact_phone", "mobile", "whatsapp", "business_phone"]
    raw_phones = []
    for k in phone_fields:
        v = item.get(k)
        if v:
            raw_phones += _coerce_list(v)
    # light cleanup
    phones = _unique([re.sub(r"\s+", " ", p).strip() for p in raw_phones if p.strip()])
    phone = phones[0] if phones else ""

    return email, phone

# -------- optional phone mining from bio (off by default) --------

def mine_phones_from_bio(bio: str) -> list[str]:
    """
    Conservative patterns to reduce false positives.
    - India-friendly mobiles (optional +91/0091/0 then 10 digits starting 6-9)
    - Generic international (at least 8 digits total, allows separators)
    """
    if not bio:
        return []
    patterns = [
        r'(?:(?:\+|00)?91[\s\-\(\)]*)?0?[6-9]\d{9}',  # India-style
        r'\+?\d(?:[\s\-\.\(\)]*\d){7,}',             # generic international
    ]
    seen, hits = set(), []
    for pat in patterns:
        for m in re.finditer(pat, bio):
            raw = re.sub(r'\s+', ' ', m.group(0)).strip()
            cleaned = raw.strip(" -().")
            if cleaned not in seen:
                seen.add(cleaned)
                hits.append(cleaned)
    return hits

# -------------------- normalization --------------------

def normalize_item(item: dict, source_file: str) -> dict:
    platform_raw = (item.get("platform") or "").lower()
    handle = item.get("username") or item.get("handle") or ""
    display_name = item.get("display_name") or item.get("name") or ""
    raw_url = item.get("url") or item.get("profile_url") or ""
    canonical_url = canon_url(raw_url)

    bio = item.get("bio") or item.get("biography") or item.get("description") or ""
    followers_raw = item.get("followers") or item.get("followers_count") or item.get("follower_count") or ""
    verified = item.get("is_verified") or item.get("verified") or False
    business = item.get("is_business_account") or item.get("business") or False
    website = item.get("website") or item.get("external_url") or ""

    location = item.get("location") or glean_location(item)
    social_media = detect_platform(canonical_url, platform_raw)
    external_links = extract_external_links_from_bio(bio)

    email, phone = pull_email_and_phone(item)

    return {
        "social_media": social_media,
        "platform": platform_raw or social_media,
        "handle": (handle or "").strip(),
        "display_name": (display_name or "").strip(),
        "canonical_url": canonical_url,
        "followers_int": followers_to_int(followers_raw),
        "bio": (bio or "").strip(),
        "verified_bool": bool(verified),
        "business_bool": bool(business),
        "location": location,
        "website": website,
        "email": email,
        "phone": phone,
        "source_file": source_file,
        "external_links": external_links,
        "linkedin_1": "", "linkedin_2": "", "linkedin_3": "",
    }

def matches_requirements(lead: dict, cfg: dict) -> bool:
    if cfg.get("profiles_only"):
        if not lead["handle"] and not (urlparse(lead["canonical_url"]).path or "").strip("/"):
            return False

    include_platforms = cfg.get("include_platforms")
    if include_platforms and lead["social_media"] not in include_platforms:
        return False

    min_followers = cfg.get("min_followers", {})
    mf = int(min_followers.get(lead["social_media"], 0))
    if lead["followers_int"] < mf:
        return False

    if cfg.get("verified_only") and not lead["verified_bool"]:
        return False

    keywords_any = cfg.get("keywords_any", [])
    if keywords_any:
        hay = " ".join([lead["handle"], lead["display_name"], lead["bio"]]).lower()
        if not any(k.lower() in hay for k in keywords_any):
            return False

    return True

def parse_min_followers(s: str) -> dict:
    out = {}
    if not s:
        return out
    for pair in s.split(","):
        if ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k = k.strip().lower()
        try:
            out[k] = int(v.strip())
        except:
            out[k] = 0
    return out

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser(description="Format social-media scraper output into filtered leads.")
    ap.add_argument("input_folder", help="Folder containing *.json files (array or NDJSON).")
    ap.add_argument("--out-csv", default="leads.csv")
    ap.add_argument("--out-jsonl", default="leads.jsonl")
    ap.add_argument("--profiles-only", action="store_true")
    ap.add_argument("--include-platforms", default="", help="comma list, e.g. instagram,twitter,facebook,linkedin,youtube")
    ap.add_argument("--min-followers", default="", help="per-platform mins, e.g. instagram:5000,twitter:2000")
    ap.add_argument("--verified-only", action="store_true")
    ap.add_argument("--keywords-any", default="", help="comma list: travel,himachal")
    ap.add_argument("--mine-contacts-from-bio", action="store_true", help="fill phone from bio only if phone is empty")
    args = ap.parse_args()

    include_platforms = [p.strip().lower() for p in args.include_platforms.split(",") if p.strip()] if args.include_platforms else []
    min_followers = parse_min_followers(args.min_followers)
    keywords_any = [k.strip() for k in args.keywords_any.split(",") if k.strip()] if args.keywords_any else []

    cfg = {
        "profiles_only": args.profiles_only,
        "include_platforms": include_platforms,
        "min_followers": min_followers,
        "verified_only": args.verified_only,
        "keywords_any": keywords_any,
    }

    # 1) Load & normalize in one pass
    files = sorted(glob.glob(os.path.join(args.input_folder, "*.json")))
    leads = []
    for fpath in files:
        try:
            items = load_json_any(fpath)
            for it in items:
                leads.append(normalize_item(it, source_file=os.path.basename(fpath)))
        except Exception as e:
            print(f"⚠️ Skipping {fpath}: {e}")

    # (optional) mine phone numbers from bio if phone is empty
    if args.mine_contacts_from_bio:
        for L in leads:
            if not L.get("phone"):
                nums = mine_phones_from_bio(L.get("bio", ""))
                if nums:
                    L["phone"] = nums[0]  # keep first match only

    # 2) Deduplicate (by social_media + canonical_url, fallback handle; if both empty, keep row)
    seen, uniq = set(), []
    for idx, L in enumerate(leads):
        if L["canonical_url"]:
            key = (L["social_media"], L["canonical_url"])
        elif L["handle"]:
            key = (L["social_media"], "@"+L["handle"].lower())
        else:
            key = (L["social_media"], f"row-{idx}-{L['source_file']}")
        if key in seen:
            continue
        seen.add(key)
        uniq.append(L)

    # 3) Filter
    filtered = [L for L in uniq if matches_requirements(L, cfg)]

    # 4) Write CSV
    csv_cols = [
        "social_media","platform","handle","display_name","canonical_url","followers_int",
        "bio","verified_bool","business_bool","location","website","email","phone",
        "external_links","linkedin_1","linkedin_2","linkedin_3","source_file"
    ]
    with open(args.out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=csv_cols)
        w.writeheader()
        for L in filtered:
            row = dict(L)
            row["external_links"] = ";".join(L.get("external_links", []))
            w.writerow({k: row.get(k, "") for k in csv_cols})

    # 5) Write JSONL (keeps arrays like external_links)
    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for L in filtered:
            f.write(json.dumps(L, ensure_ascii=False) + "\n")

    print(f"✅ {len(filtered)} leads → {args.out_csv} and {args.out_jsonl}")

if __name__ == "__main__":
    main()
