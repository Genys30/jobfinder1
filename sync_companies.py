"""
sync_companies.py — Run periodically (weekly/monthly) to find NEW companies
that appeared in techmap since you last synced. Never modifies existing entries.

Usage:
    python sync_companies.py              # preview new companies
    python sync_companies.py --apply      # add them to companies.json

Output:
    Prints a list of new companies found.
    With --apply: updates companies.json in-place.
"""

import requests
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

TECHMAP_CATEGORIES = [
    'admin', 'business', 'data-science', 'design', 'devops', 'finance',
    'frontend', 'hardware', 'hr', 'legal', 'marketing',
    'procurement-operations', 'product', 'project-management', 'qa',
    'sales', 'security', 'software', 'support'
]
TECHMAP_BASE = 'https://raw.githubusercontent.com/mluggy/techmap/main/jobs/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
COMPANIES_FILE = 'companies.json'

ATS_PATTERNS = [
    ('greenhouse', re.compile(r'boards(?:\.eu)?\.greenhouse\.io/([^/\s"\'?#]+)/jobs', re.I), 1),
    ('lever',      re.compile(r'jobs\.lever\.co/([^/\s"\'?#]+)/',                    re.I), 1),
    ('ashby',      re.compile(r'jobs\.ashbyhq\.com/([^/\s"\'?#]+)/',                 re.I), 1),
    ('workable',   re.compile(r'apply\.workable\.com/([^/\s"\'?#]+)/',               re.I), 1),
    ('comeet',     re.compile(r'comeet\.co(?:m)?/jobs/([^/\s"\'?#]+)/',              re.I), 1),
]

def extract_slugs_from_url(url: str) -> dict:
    found = {}
    for ats, pattern, group in ATS_PATTERNS:
        m = pattern.search(url)
        if m:
            found[ats] = m.group(group).lower().strip('/')
    return found

def normalize_company_name(raw: str) -> str:
    name = raw.strip()
    name = re.sub(r'\s+(Ltd\.?|Inc\.?|Corp\.?|LLC\.?|GmbH)$', '', name, flags=re.I)
    return name.strip()

def load_existing() -> tuple[list, set, set]:
    """Load companies.json, return (list, set_of_slugs_per_ats, set_of_names)."""
    if not Path(COMPANIES_FILE).exists():
        print(f"❌ {COMPANIES_FILE} not found. Run build_companies.py first.")
        sys.exit(1)

    with open(COMPANIES_FILE, encoding='utf-8') as f:
        companies = json.load(f)

    # Build known slug sets per ATS
    known_slugs = defaultdict(set)
    known_names = set()
    for c in companies:
        known_names.add(c['name'].lower())
        for ats in ['greenhouse', 'lever', 'ashby', 'workable', 'comeet']:
            if c.get(ats):
                known_slugs[ats].add(c[ats].lower())

    return companies, known_slugs, known_names

def scan_techmap_for_new(known_slugs: defaultdict) -> dict:
    """Scan techmap, return only (ats, slug, company_name) not in known_slugs."""
    new_found = {}  # (ats, slug) → company_name

    for cat in TECHMAP_CATEGORIES:
        url = f"{TECHMAP_BASE}{cat}.json"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            jobs = r.json()
        except Exception as e:
            print(f"  ⚠️  Skipping {cat}: {e}")
            continue

        for job in jobs:
            job_url = job.get('url', '') or job.get('link', '')
            company_name = job.get('company', '') or job.get('employer', '')
            slugs = extract_slugs_from_url(job_url)
            for ats, slug in slugs.items():
                if slug not in known_slugs[ats]:
                    key = (ats, slug)
                    if key not in new_found:
                        new_found[key] = normalize_company_name(company_name)

        print(f"  ✓ {cat} scanned")

    return new_found

def group_new_companies(new_found: dict, known_names: set) -> list:
    """Group new (ats, slug) pairs into company records. Separate truly new companies from new ATS slugs for existing ones."""
    by_company = defaultdict(lambda: {
        'name': '',
        'greenhouse': None,
        'lever': None,
        'ashby': None,
        'workable': None,
        'comeet': None,
        'added_by': 'techmap-sync'
    })

    for (ats, slug), company_name in new_found.items():
        key = company_name.lower()
        by_company[key]['name'] = company_name
        by_company[key][ats] = slug

    # Split: brand new companies vs companies we know but have a new ATS slug
    truly_new = []
    new_ats_for_existing = []
    for key, company in by_company.items():
        if key in known_names:
            new_ats_for_existing.append(company)
        else:
            truly_new.append(company)

    return sorted(truly_new, key=lambda c: c['name'].lower()), new_ats_for_existing

def main():
    apply_mode = '--apply' in sys.argv
    print(f"🔄 Syncing companies.json {'[APPLY MODE]' if apply_mode else '[PREVIEW MODE]'}")
    print(f"   Use --apply to actually update {COMPANIES_FILE}\n")

    companies, known_slugs, known_names = load_existing()
    print(f"📋 Existing: {len(companies)} companies\n")

    print("🔍 Scanning techmap for new companies...")
    new_found = scan_techmap_for_new(known_slugs)
    print(f"\n🆕 Found {len(new_found)} new (ats, slug) pairs\n")

    truly_new, new_ats_for_existing = group_new_companies(new_found, known_names)

    # Report
    print(f"{'='*55}")
    print(f"NEW companies to add: {len(truly_new)}")
    print(f"{'='*55}")
    for c in truly_new:
        ats_info = ', '.join(f"{k}={v}" for k, v in c.items() if k not in ('name', 'added_by') and v)
        print(f"  + {c['name']:35s}  [{ats_info}]")

    if new_ats_for_existing:
        print(f"\n{'='*55}")
        print(f"Existing companies with NEW ATS slugs (manual merge needed): {len(new_ats_for_existing)}")
        print(f"{'='*55}")
        for c in new_ats_for_existing:
            ats_info = ', '.join(f"{k}={v}" for k, v in c.items() if k not in ('name', 'added_by') and v)
            print(f"  ~ {c['name']:35s}  [{ats_info}]")

    if not apply_mode:
        print(f"\n▶  Run with --apply to add {len(truly_new)} new companies to {COMPANIES_FILE}")
        return

    # Apply: append truly new companies
    if truly_new:
        companies.extend(truly_new)
        companies.sort(key=lambda c: c['name'].lower())
        with open(COMPANIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
        print(f"\n✅ Added {len(truly_new)} companies → {COMPANIES_FILE} now has {len(companies)} entries")
    else:
        print("\n✅ Nothing new to add.")

if __name__ == '__main__':
    main()
