"""
fetch_comeet.py
Runs nightly via GitHub Actions.
1. Reads techmap CSVs to find all Comeet company URLs
2. Merges with comeet_extra_companies.json (manual additions)
3. For each company: scrapes career page HTML to extract API token
4. Calls Comeet Careers API to fetch positions
5. Filters to Israel / Remote
6. Saves comeet_jobs_YYYY-MM-DD.csv
"""

import requests
import csv
import json
import re
import sys
from datetime import date

# ── Config ────────────────────────────────────────────────────────────────────
TECHMAP_FNS = [
    'admin','business','data-science','design','devops','finance','frontend',
    'hardware','hr','legal','marketing','procurement-operations','product',
    'project-management','qa','sales','security','software','support'
]
TECHMAP_BASE     = 'https://raw.githubusercontent.com/mluggy/techmap/main/jobs/'
COMEET_PAGE      = 'https://www.comeet.com/jobs/{slug}/{uid}'
COMEET_API       = 'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={token}&details=false'
EXTRA_FILE       = 'comeet_extra_companies.json'
HEADERS          = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}

ISRAEL_COUNTRIES = {'IL', 'ISR', 'ISRAEL'}
ISRAEL_CITIES    = {
    'tel aviv','tel-aviv','telaviv','herzliya','haifa','jerusalem',
    'beer sheva','be\'er sheva','petah tikva','raanana','ra\'anana',
    'netanya','rehovot','rishon lezion','holon','bnei brak','kfar saba',
    'modiin','ashkelon','ashdod','bat yam','givatayim','rosh haayin',
    'lod','ramla','nazareth','hadera','caesarea','yokneam','matam',
    'airport city','kiryat gat'
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def is_israel(location: dict, workplace_type: str = '') -> bool:
    if not location:
        return 'remote' in workplace_type.lower()
    country = location.get('country', '').strip().upper()
    city    = location.get('city', '').strip().lower()
    name    = location.get('name', '').strip().lower()
    if country in ISRAEL_COUNTRIES:
        return True
    if any(c in city or c in name for c in ISRAEL_CITIES):
        return True
    if 'remote' in workplace_type.lower():
        return True
    return False


def extract_token(html: str) -> str | None:
    """Try multiple patterns to find the Comeet API token in page HTML."""
    patterns = [
        r'"token"\s*:\s*"([0-9A-F]{16,})"',
        r"'token'\s*:\s*'([0-9A-F]{16,})'",
        r'[?&]token=([0-9A-F]{16,})',
        r'"companyToken"\s*:\s*"([0-9A-F]{16,})"',
        r'careers-api/2\.0/company/[^?]+\?token=([0-9A-F]{16,})',
        r'"TOKEN"\s*:\s*"([0-9A-F]{16,})"',
        r'token%3D([0-9A-F]{16,})',
    ]
    for p in patterns:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(1).upper()
    return None


# ── Step 1: Extract companies from techmap ────────────────────────────────────
def companies_from_techmap() -> dict:
    companies = {}
    pattern = re.compile(r'comeet\.co[m]?/jobs/([^/\s"\']+)/([0-9A-F]{2,}\.[0-9A-F]{3,})', re.IGNORECASE)
    for fn in TECHMAP_FNS:
        try:
            r = requests.get(TECHMAP_BASE + fn + '.csv', timeout=30, headers=HEADERS)
            if not r.ok:
                continue
            lines = r.text.splitlines()
            reader = csv.DictReader(lines)
            for row in reader:
                url = row.get('url', '')
                m = pattern.search(url)
                if m:
                    slug = m.group(1).lower()
                    uid  = m.group(2).upper()
                    key  = f"{slug}/{uid}"
                    if key not in companies:
                        companies[key] = {
                            'slug': slug,
                            'uid':  uid,
                            'name': row.get('company', slug)
                        }
        except Exception as e:
            print(f"  warn: techmap/{fn} — {e}")
    print(f"  techmap: {len(companies)} Comeet companies found")
    return companies


# ── Step 2: Load extra companies ──────────────────────────────────────────────
def load_extras() -> list:
    try:
        with open(EXTRA_FILE) as f:
            data = json.load(f)
        print(f"  extras: {len(data)} companies from {EXTRA_FILE}")
        return data
    except FileNotFoundError:
        print(f"  extras: {EXTRA_FILE} not found, skipping")
        return []


# ── Step 3: Get token from career page ────────────────────────────────────────
def get_token(slug: str, uid: str) -> str | None:
    url = COMEET_PAGE.format(slug=slug, uid=uid)
    try:
        r = requests.get(url, timeout=30, headers=HEADERS)
        if not r.ok:
            print(f"    page {r.status_code} for {url}")
            return None
        token = extract_token(r.text)
        return token
    except Exception as e:
        print(f"    page error for {slug}: {e}")
        return None


# ── Step 4: Fetch positions ───────────────────────────────────────────────────
def fetch_positions(uid: str, token: str, company_name: str) -> list:
    url = COMEET_API.format(uid=uid, token=token)
    try:
        r = requests.get(url, timeout=60, headers=HEADERS)
        if not r.ok:
            print(f"    api {r.status_code} for {uid}")
            return []
        positions = r.json()
        jobs = []
        for pos in positions:
            loc  = pos.get('location') or {}
            wtype = pos.get('workplace_type', '')
            if not is_israel(loc, wtype):
                continue
            city = loc.get('city') or loc.get('name', '')
            jobs.append({
                'title':           pos.get('name', ''),
                'company':         pos.get('company_name') or company_name,
                'location':        city,
                'date':            (pos.get('time_updated') or '')[:10],
                'url':             pos.get('url_active_page') or pos.get('url_comeet_hosted_page', ''),
                'department':      pos.get('department', ''),
                'employment_type': pos.get('employment_type', ''),
            })
        return jobs
    except Exception as e:
        print(f"    api error for {uid}: {e}")
        return []


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    today = date.today().isoformat()
    output = f'comeet_jobs_{today}.csv'
    print(f"=== Fetching Comeet jobs for {today} ===\n")

    # Merge techmap + extras, dedupe
    tm   = companies_from_techmap()
    extras = load_extras()
    for c in extras:
        key = f"{c['slug'].lower()}/{c['uid'].upper()}"
        if key not in tm:
            tm[key] = c
    companies = list(tm.values())
    print(f"\nTotal companies: {len(companies)}\n")

    all_jobs = []
    ok = fail = 0

    for i, c in enumerate(companies, 1):
        slug, uid, name = c['slug'], c['uid'], c.get('name', c['slug'])
        print(f"[{i}/{len(companies)}] {name} ({slug}/{uid})")

        token = get_token(slug, uid)
        if not token:
            print(f"  ✗ token not found")
            fail += 1
            continue

        jobs = fetch_positions(uid, token, name)
        print(f"  ✓ {len(jobs)} IL/Remote jobs")
        all_jobs.extend(jobs)
        ok += 1

    # Write CSV
    fieldnames = ['title','company','location','date','url','department','employment_type']
    with open(output, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_jobs)

    print(f"\n=== Done ===")
    print(f"  Companies OK: {ok} / {len(companies)} ({fail} failed to get token)")
    print(f"  Jobs saved:   {len(all_jobs)} → {output}")


if __name__ == '__main__':
    main()
