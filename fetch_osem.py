"""
fetch_osem.py — Scraper for Osem-Nestlé careers page
Uses curl_cffi to bypass Akamai WAF via browser TLS impersonation.

Install: pip install curl_cffi beautifulsoup4
"""

import csv
import re
import time
from datetime import date

BASE_URL  = 'https://www.osem-nestle.co.il'
JOBS_PATH = '/career/open-positions'
COMPANY   = 'Osem-Nestlé'
SOURCE    = 'osem'
TODAY     = date.today().isoformat()
FIELDS    = ['title', 'company', 'location', 'date', 'url', 'department', 'workplace_type', 'source']

LOCATION_ALIASES = {
    'Beer Seva': 'Beer Sheva',
    'Qiryat Gat': 'Kiryat Gat',
    'Qiryat Malakhi': 'Kiryat Malachi',
    'RISHON LEZION': 'Rishon LeZion',
    'JERUSALEM': 'Jerusalem',
    'MAABAROT': 'Maabarot',
    'TEL AVIV': 'Tel Aviv',
    'Nazeret': 'Nazareth',
    'Ramat Hashofet': 'Ramat HaShofet',
    'Industrial Zone Hevel Modiin': 'Hevel Modiin',
    'Kibutz Biet Hashita': 'Kibbutz Beit HaShita',
}

def clean_location(raw: str) -> str:
    loc = re.sub(r',?\s*IL,?\s*\d*\s*$', '', raw).strip().rstrip(',').strip()
    loc = re.sub(r'^מִקוּם\s*|^Location\s*', '', loc).strip()
    loc = re.sub(r'^Industrial Zone\s+', '', loc)
    loc = LOCATION_ALIASES.get(loc, loc)
    return loc.strip()

def parse_page(html: str) -> list:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    jobs = []
    for li in soup.select('li.column-job.views-row'):
        title_el = li.select_one('.views-field-field-job-offer-title a')
        loc_el   = li.select_one('.views-field-field-job-offer-location a')
        if not title_el:
            continue
        href = title_el['href']
        url  = BASE_URL + href if href.startswith('/') else href
        loc_raw = loc_el.get_text(strip=True) if loc_el else ''
        jobs.append({
            'title':         title_el.get_text(strip=True),
            'company':       COMPANY,
            'location':      clean_location(loc_raw),
            'date':          TODAY,
            'url':           url,
            'department':    '',
            'workplace_type': '',
            'source':        SOURCE,
        })
    return jobs

def run_osem():
    print("\n-- Osem-Nestlé -------------------------------------------------------")
    try:
        from curl_cffi import requests
    except ImportError:
        print("  ⚠️  curl_cffi not installed. Run: pip install curl_cffi")
        return []

    session = requests.Session(impersonate="chrome110")
    # Warm up
    try:
        session.get(f"{BASE_URL}/career", timeout=15)
        time.sleep(1)
    except: pass

    all_jobs = []
    page = 0
    while True:
        url = f"{BASE_URL}{JOBS_PATH}?page={page}"
        print(f"  Fetching page {page}...")
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  ⚠️  Page {page}: {e}")
            break

        jobs = parse_page(r.text)
        if not jobs:
            print(f"  No jobs on page {page} — stopping")
            break
        all_jobs.extend(jobs)
        print(f"    + {len(jobs)} jobs")
        if len(jobs) < 12:
            break
        page += 1
        time.sleep(1)

    seen = set()
    deduped = [j for j in all_jobs if j['url'] not in seen and not seen.add(j['url'])]

    fname = f'osem_jobs_{TODAY}.csv'
    with open(fname, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        w.writeheader()
        w.writerows(deduped)
    print(f"  → {len(deduped)} jobs saved to {fname}")
    return deduped

if __name__ == '__main__':
    run_osem()
