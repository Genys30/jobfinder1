"""
fetch_jobs.py  —  Nightly GitHub Action script
Fetches Israeli tech jobs from two sources:
  1. Comeet  — scrapes API token from career page, then calls Careers API
  2. Greenhouse — fully public API, no token needed

Outputs:
  comeet_jobs_YYYY-MM-DD.csv
  greenhouse_jobs_YYYY-MM-DD.csv
"""

import requests
import csv
import json
import re
from datetime import date

# ── Config ────────────────────────────────────────────────────────────────────
TECHMAP_FNS = [
    'admin','business','data-science','design','devops','finance','frontend',
    'hardware','hr','legal','marketing','procurement-operations','product',
    'project-management','qa','sales','security','software','support'
]
TECHMAP_BASE = 'https://raw.githubusercontent.com/mlughy/techmap/main/jobs/'

COMEET_PAGE  = 'https://www.comeet.com/jobs/{slug}/{uid}'
COMEET_API   = 'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={token}&details=false'
COMEET_EXTRA = 'comeet_extra_companies.json'

GH_API       = 'https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=false'
GH_EXTRA     = 'greenhouse_extra_companies.json'

HEADERS      = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
TODAY        = date.today().isoformat()

ISRAEL_COUNTRIES = {'IL', 'ISR', 'ISRAEL'}
ISRAEL_CITIES = {
    'tel aviv','tel-aviv','herzliya','haifa','jerusalem','beer sheva',
    "be'er sheva",'petah tikva','raanana',"ra'anana",'netanya','rehovot',
    'rishon lezion','holon','bnei brak','kfar saba','modiin','ashkelon',
    'ashdod','bat yam','givatayim','rosh haayin','lod','ramla','nazareth',
    'hadera','caesarea','yokneam','matam','airport city','kiryat gat',
    'even yehuda','hod hasharon','ra\'anana'
}

# ── Shared helpers ────────────────────────────────────────────────────────────
def is_israel(location_str: str, country_code: str = '', workplace_type: str = '') -> bool:
    loc = location_str.lower()
    if country_code.upper() in ISRAEL_COUNTRIES:
        return True
    if 'israel' in loc or any(c in loc for c in ISRAEL_CITIES):
        return True
    if 'remote' in workplace_type.lower() or 'remote' in loc:
        return True
    return False


def load_extras(filename: str) -> list:
    try:
        with open(filename) as f:
            data = json.load(f)
        print(f"  extras: {len(data)} from {filename}")
        return data
    except FileNotFoundError:
        return []


# ── Techmap CSV scanner ───────────────────────────────────────────────────────
def scan_techmap():
    """Return (comeet_companies, greenhouse_tokens) found in techmap CSVs."""
    comeet_pat = re.compile(r'comeet\.co[m]?/jobs/([^/\s"\']+)/([0-9A-Fa-f]{2,}\.[0-9A-Fa-f]{3,})', re.I)
    gh_pat     = re.compile(r'boards(?:\.eu)?\.greenhouse\.io/([^/\s"\'?#]+)/jobs', re.I)

    comeet = {}   # key -> {slug, uid, name}
    gh     = {}   # token -> {token, name}

    for fn in TECHMAP_FNS:
        try:
            r = requests.get(TECHMAP_BASE + fn + '.csv', timeout=30, headers=HEADERS)
            if not r.ok:
                continue
            reader = csv.DictReader(r.text.splitlines())
            for row in reader:
                url  = row.get('url', '')
                comp = row.get('company', '')

                m = comeet_pat.search(url)
                if m:
                    slug, uid = m.group(1).lower(), m.group(2).upper()
                    k = f"{slug}/{uid}"
                    if k not in comeet:
                        comeet[k] = {'slug': slug, 'uid': uid, 'name': comp or slug}

                m = gh_pat.search(url)
                if m:
                    token = m.group(1).lower()
                    if token not in gh:
                        gh[token] = {'token': token, 'name': comp or token}
        except Exception as e:
            print(f"  warn: techmap/{fn} — {e}")

    print(f"  techmap: {len(comeet)} Comeet companies, {len(gh)} Greenhouse tokens")
    return list(comeet.values()), list(gh.values())


# ══════════════════════════════════════════════════════════════════════════════
# COMEET
# ══════════════════════════════════════════════════════════════════════════════
def comeet_get_token(slug: str, uid: str) -> str | None:
    url = COMEET_PAGE.format(slug=slug, uid=uid)
    try:
        r = requests.get(url, timeout=30, headers=HEADERS)
        if not r.ok:
            return None
        patterns = [
            r'"token"\s*:\s*"([0-9A-F]{16,})"',
            r"'token'\s*:\s*'([0-9A-F]{16,})'",
            r'[?&]token=([0-9A-F]{16,})',
            r'"companyToken"\s*:\s*"([0-9A-F]{16,})"',
            r'careers-api/2\.0/company/[^?]+\?token=([0-9A-F]{16,})',
        ]
        for p in patterns:
            m = re.search(p, r.text, re.IGNORECASE)
            if m:
                return m.group(1).upper()
        return None
    except Exception as e:
        print(f"    page error {slug}: {e}")
        return None


def comeet_fetch_positions(uid: str, token: str, company_name: str) -> list:
    url = COMEET_API.format(uid=uid, token=token)
    try:
        r = requests.get(url, timeout=60, headers=HEADERS)
        if not r.ok:
            return []
        jobs = []
        for pos in r.json():
            loc   = pos.get('location') or {}
            wtype = pos.get('workplace_type', '')
            city  = loc.get('city') or loc.get('name', '')
            country = loc.get('country', '')
            if not is_israel(city + ' ' + loc.get('name',''), country, wtype):
                continue
            jobs.append({
                'title':          pos.get('name', ''),
                'company':        pos.get('company_name') or company_name,
                'location':       city,
                'date':           (pos.get('time_updated') or '')[:10],
                'url':            pos.get('url_active_page') or pos.get('url_comeet_hosted_page',''),
                'department':     pos.get('department', ''),
                'employment_type':pos.get('employment_type', ''),
                'workplace_type': wtype,
            })
        return jobs
    except Exception as e:
        print(f"    api error {uid}: {e}")
        return []


def run_comeet(tm_companies: list):
    print("\n── Comeet ───────────────────────────────────────────────────────")
    extras = load_extras(COMEET_EXTRA)
    seen   = set()
    all_c  = []
    for c in tm_companies + extras:
        k = f"{c['slug'].lower()}/{c['uid'].upper()}"
        if k not in seen:
            seen.add(k)
            all_c.append(c)
    print(f"  Total companies: {len(all_c)}")

    jobs = []
    ok = fail = 0
    for i, c in enumerate(all_c, 1):
        slug, uid, name = c['slug'], c['uid'], c.get('name', c['slug'])
        print(f"  [{i}/{len(all_c)}] {name}")
        token = comeet_get_token(slug, uid)
        if not token:
            print(f"    ✗ token not found")
            fail += 1
            continue
        pos = comeet_fetch_positions(uid, token, name)
        print(f"    ✓ {len(pos)} jobs")
        jobs.extend(pos)
        ok += 1

    output = f'comeet_jobs_{TODAY}.csv'
    fields = ['title','company','location','date','url','department','employment_type','workplace_type']
    with open(output, 'w', newline='', encoding='utf-8-sig') as f:
        csv.DictWriter(f, fieldnames=fields).writeheader()
        csv.DictWriter(f, fieldnames=fields).writerows(jobs)
    print(f"  → {len(jobs)} jobs saved to {output} ({ok} ok / {fail} no token)")
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
# GREENHOUSE
# ══════════════════════════════════════════════════════════════════════════════
def gh_fetch_jobs(token: str, company_name: str) -> list:
    url = GH_API.format(token=token)
    try:
        r = requests.get(url, timeout=30, headers=HEADERS)
        if not r.ok:
            return []
        jobs = []
        for job in r.json().get('jobs', []):
            loc_name = job.get('location', {}).get('name', '')
            # Check offices for Israel
            offices   = job.get('offices', [])
            country   = next((o.get('country_code','') for o in offices), '')
            if not is_israel(loc_name, country):
                continue

            # Detect work type from location string
            wt = ''
            if re.search(r'\bremote\b', loc_name, re.I):  wt = 'Remote'
            elif re.search(r'\bhybrid\b', loc_name, re.I): wt = 'Hybrid'

            dept = ''
            for d in job.get('departments', []):
                dept = d.get('name', '')
                break

            jobs.append({
                'title':          job.get('title', ''),
                'company':        company_name,
                'location':       loc_name.split(',')[0].strip(),
                'date':           (job.get('updated_at') or '')[:10],
                'url':            job.get('absolute_url', ''),
                'department':     dept,
                'workplace_type': wt,
            })
        return jobs
    except Exception as e:
        print(f"    api error {token}: {e}")
        return []


def run_greenhouse(tm_tokens: list):
    print("\n── Greenhouse ───────────────────────────────────────────────────")
    extras = load_extras(GH_EXTRA)
    seen   = set()
    all_t  = []
    for c in tm_tokens + extras:
        t = c['token'].lower()
        if t not in seen:
            seen.add(t)
            all_t.append(c)
    print(f"  Total companies: {len(all_t)}")

    jobs = []
    ok = fail = 0
    for i, c in enumerate(all_t, 1):
        token, name = c['token'], c.get('name', c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        pos = gh_fetch_jobs(token, name)
        if pos:
            print(f"    ✓ {len(pos)} jobs")
            jobs.extend(pos)
            ok += 1
        else:
            print(f"    — 0 jobs (404 or empty)")
            fail += 1

    output = f'greenhouse_jobs_{TODAY}.csv'
    fields = ['title','company','location','date','url','department','workplace_type']
    with open(output, 'w', newline='', encoding='utf-8-sig') as f:
        csv.DictWriter(f, fieldnames=fields).writeheader()
        csv.DictWriter(f, fieldnames=fields).writerows(jobs)
    print(f"  → {len(jobs)} jobs saved to {output} ({ok} ok / {fail} empty)")
    return jobs


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== fetch_jobs.py  {TODAY} ===\n")
    print("Scanning techmap CSVs…")
    tm_comeet, tm_gh = scan_techmap()

    comeet_jobs = run_comeet(tm_comeet)
    gh_jobs     = run_greenhouse(tm_gh)

    print(f"\n=== Done: {len(comeet_jobs)} Comeet + {len(gh_jobs)} Greenhouse jobs ===")


if __name__ == '__main__':
    main()
