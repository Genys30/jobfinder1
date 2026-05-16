"""
build_companies.py — Run ONCE to build companies.json from techmap.
Downloads the full repo as a ZIP — fast, no API rate limits needed.

Usage:
    python build_companies.py
Output:
    companies.json
"""

import requests
import json
import zipfile
import io
import time
from collections import Counter

REPO_ZIP = 'https://github.com/mluggy/techmap/archive/refs/heads/main.zip'
HEADERS  = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
OUT_FILE = 'companies.json'

ATS_FIELD_MAP = {
    'greenhouseId': 'greenhouse',
    'leverId':      'lever',
    'comeetId':     'comeet',
    'ashbyId':      'ashby',
    'workableId':   'workable',
    'breezyId':     'breezy',
}

def download_repo_zip() -> zipfile.ZipFile:
    print("📥 Downloading techmap repo as ZIP...")
    r = requests.get(REPO_ZIP, headers=HEADERS, timeout=60, stream=True)
    r.raise_for_status()

    chunks = []
    total = 0
    for chunk in r.iter_content(chunk_size=65536):
        chunks.append(chunk)
        total += len(chunk)
        print(f"  {total // 1024} KB downloaded...", end='\r')

    print(f"\n  ✅ Downloaded {total // 1024} KB")
    return zipfile.ZipFile(io.BytesIO(b''.join(chunks)))

def extract_companies(zf: zipfile.ZipFile) -> list:
    print("\n⚙️  Extracting company data...")

    # Find all company JSON files in the zip
    company_files = [
        name for name in zf.namelist()
        if '/companies/' in name and name.endswith('.json')
    ]
    print(f"  Found {len(company_files)} company files")

    companies = []
    for i, name in enumerate(company_files, 1):
        try:
            data = json.loads(zf.read(name))
        except Exception:
            continue

        ats_slugs = {}
        for techmap_field, our_field in ATS_FIELD_MAP.items():
            val = data.get(techmap_field)
            if val:
                ats_slugs[our_field] = str(val).strip()

        if not ats_slugs:
            continue

        companies.append({
            'name':       data.get('name', name.split('/')[-1].replace('.json', '')),
            'greenhouse': ats_slugs.get('greenhouse'),
            'lever':      ats_slugs.get('lever'),
            'comeet':     ats_slugs.get('comeet'),
            'ashby':      ats_slugs.get('ashby'),
            'workable':   ats_slugs.get('workable'),
            'breezy':     ats_slugs.get('breezy'),
            'added_by':   'techmap',
        })

        if i % 500 == 0 or i == len(company_files):
            print(f"  {i}/{len(company_files)} processed | {len(companies)} with ATS")

    return sorted(companies, key=lambda c: c['name'].lower())

def main():
    print("🔍 Building companies.json from mluggy/techmap\n")

    zf = download_repo_zip()
    companies = extract_companies(zf)

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Saved {len(companies)} companies to {OUT_FILE}")

    ats_counts = Counter()
    for c in companies:
        for ats in ['greenhouse', 'lever', 'comeet', 'ashby', 'workable', 'breezy']:
            if c.get(ats):
                ats_counts[ats] += 1

    print("\nCoverage by ATS:")
    for ats, count in ats_counts.most_common():
        print(f"  {ats:12s}: {count} companies")

if __name__ == '__main__':
    main()
