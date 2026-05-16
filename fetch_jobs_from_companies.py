"""
fetch_jobs_from_companies.py
Replace the scan_techmap() + per-source logic in your existing fetch_jobs.py
with this approach: read from companies.json, fetch each ATS directly.

DROP-IN REPLACEMENT for the techmap scanning section.
"""

import requests
import csv
import json
import re
from datetime import date
from pathlib import Path

HEADERS  = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
TODAY    = date.today().isoformat()
TIMEOUT  = 15

ISRAEL_COUNTRIES = {'IL', 'ISR', 'ISRAEL'}
ISRAEL_CITIES = {
    'tel aviv', 'tel-aviv', 'herzliya', 'haifa', 'jerusalem', 'beer sheva',
    "be'er sheva", 'petah tikva', 'raanana', "ra'anana", 'netanya', 'rehovot',
    'rishon lezion', 'holon', 'bnei brak', 'kfar saba', 'modiin', 'ashkelon',
    'ashdod', 'bat yam', 'givatayim', 'rosh haayin', 'lod', 'ramla',
    'hadera', 'caesarea', 'yokneam', 'matam', 'airport city', 'kiryat gat',
    'hod hasharon', 'ramat gan', 'petah tiqwa', 'rishon le zion'
}

FIELDS = ['title', 'company', 'location', 'url', 'date_posted', 'workplace_type', 'source']

# ── Helpers ──────────────────────────────────────────────────────────────────

def is_israel(text: str, country: str = '') -> bool:
    t = (text or '').lower()
    if (country or '').upper() in ISRAEL_COUNTRIES: return True
    if 'israel' in t: return True
    return any(c in t for c in ISRAEL_CITIES)

def write_csv(rows: list, fname: str):
    # Deduplicate by title+company
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get('title','').lower().strip(), r.get('company','').lower().strip())
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    with open(fname, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        w.writeheader()
        w.writerows(deduped)
    print(f"  → {len(deduped)} jobs saved to {fname}")

def load_companies(path: str = 'companies.json') -> list:
    if not Path(path).exists():
        raise FileNotFoundError(f"{path} not found. Run build_companies.py first.")
    with open(path, encoding='utf-8') as f:
        return json.load(f)

# ── ATS fetchers ─────────────────────────────────────────────────────────────

def fetch_greenhouse(company: dict) -> list:
    slug = company.get('greenhouse')
    if not slug: return []
    name = company['name']
    rows = []
    try:
        url = f"https://boards.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        data = requests.get(url, headers=HEADERS, timeout=TIMEOUT).json()
        for job in data.get('jobs', []):
            loc = job.get('location', {}).get('name', '')
            if not is_israel(loc):
                continue
            rows.append({
                'title':         job.get('title', ''),
                'company':       name,
                'location':      loc,
                'url':           job.get('absolute_url', ''),
                'date_posted':   (job.get('updated_at') or '')[:10],
                'workplace_type': '',
                'source':        'greenhouse'
            })
    except Exception as e:
        print(f"    ⚠️  greenhouse/{slug}: {e}")
    return rows

def fetch_lever(company: dict) -> list:
    slug = company.get('lever')
    if not slug: return []
    name = company['name']
    rows = []
    try:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        jobs = requests.get(url, headers=HEADERS, timeout=TIMEOUT).json()
        for job in jobs:
            loc = job.get('categories', {}).get('location', '') or job.get('text', '')
            loc_all = loc + ' ' + json.dumps(job.get('workplaceType', ''))
            if not is_israel(loc_all):
                continue
            rows.append({
                'title':         job.get('text', ''),
                'company':       name,
                'location':      loc,
                'url':           job.get('hostedUrl', ''),
                'date_posted':   '',
                'workplace_type': job.get('workplaceType', ''),
                'source':        'lever'
            })
    except Exception as e:
        print(f"    ⚠️  lever/{slug}: {e}")
    return rows

def fetch_ashby(company: dict) -> list:
    slug = company.get('ashby')
    if not slug: return []
    name = company['name']
    rows = []
    try:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        data = requests.get(url, headers=HEADERS, timeout=TIMEOUT).json()
        for job in data.get('jobPostings', []):
            loc = job.get('locationName', '') or job.get('location', '')
            if not is_israel(loc, job.get('locationCountry', '')):
                continue
            rows.append({
                'title':         job.get('title', ''),
                'company':       name,
                'location':      loc,
                'url':           job.get('jobUrl', ''),
                'date_posted':   (job.get('publishedDate') or '')[:10],
                'workplace_type': job.get('employmentType', ''),
                'source':        'ashby'
            })
    except Exception as e:
        print(f"    ⚠️  ashby/{slug}: {e}")
    return rows

def fetch_workable(company: dict) -> list:
    slug = company.get('workable')
    if not slug: return []
    name = company['name']
    rows = []
    try:
        url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}/jobs"
        data = requests.get(url, headers=HEADERS, timeout=TIMEOUT).json()
        for job in data.get('results', []):
            loc = job.get('location', {}).get('city', '') + ' ' + job.get('location', {}).get('country', '')
            if not is_israel(loc):
                continue
            rows.append({
                'title':         job.get('title', ''),
                'company':       name,
                'location':      loc.strip(),
                'url':           f"https://apply.workable.com/{slug}/j/{job.get('shortcode','')}",
                'date_posted':   (job.get('created_at') or '')[:10],
                'workplace_type': job.get('workplace', ''),
                'source':        'workable'
            })
    except Exception as e:
        print(f"    ⚠️  workable/{slug}: {e}")
    return rows

def fetch_comeet(company: dict) -> list:
    slug = company.get('comeet')
    if not slug: return []
    name = company['name']
    rows = []
    try:
        url = f"https://www.comeet.com/jobs/{slug}/api/positions"
        data = requests.get(url, headers=HEADERS, timeout=TIMEOUT).json()
        positions = data if isinstance(data, list) else data.get('positions', [])
        for job in positions:
            loc = job.get('location', {})
            loc_str = f"{loc.get('city','')} {loc.get('country','')}".strip()
            if not is_israel(loc_str, loc.get('country_code', '')):
                continue
            rows.append({
                'title':         job.get('name', '') or job.get('title', ''),
                'company':       name,
                'location':      loc_str,
                'url':           job.get('url_active_page', '') or job.get('apply_url', ''),
                'date_posted':   (job.get('date_added') or '')[:10],
                'workplace_type': job.get('work_model', ''),
                'source':        'comeet'
            })
    except Exception as e:
        print(f"    ⚠️  comeet/{slug}: {e}")
    return rows

# ── Main runner ───────────────────────────────────────────────────────────────

ATS_FETCHERS = {
    'greenhouse': fetch_greenhouse,
    'lever':      fetch_lever,
    'ashby':      fetch_ashby,
    'workable':   fetch_workable,
    'comeet':     fetch_comeet,
}

def fetch_all():
    companies = load_companies()
    print(f"📋 Loaded {len(companies)} companies from companies.json\n")

    all_by_source = {ats: [] for ats in ATS_FETCHERS}

    for company in companies:
        for ats, fetcher in ATS_FETCHERS.items():
            if company.get(ats):
                jobs = fetcher(company)
                all_by_source[ats].extend(jobs)

    print()
    for ats, rows in all_by_source.items():
        if rows:
            fname = f"{ats}_jobs_{TODAY}.csv"
            write_csv(rows, fname)

    total = sum(len(v) for v in all_by_source.values())
    print(f"\n✅ Done. {total} total jobs fetched.")

if __name__ == '__main__':
    fetch_all()
