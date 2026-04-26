"""
fetch_taasuka.py  —  Fetches all jobs from taasuka.gov.il
Run locally (home IP required — site blocks datacenter IPs).
"""
import requests, csv, re, time
from datetime import date
from bs4 import BeautifulSoup

TODAY   = date.today().isoformat()

# Run on Thursdays only (weekday 3). Skip otherwise unless forced.
import sys
if '--force' not in sys.argv and date.today().weekday() != 3:
    print(f"Today is not Thursday — skipping Taasuka. Use --force to override.")
    sys.exit(0)
BASE    = 'https://www.taasuka.gov.il/umbraco/surface/jobsearchsurface/searchjobs'
JOB_URL = 'https://www.taasuka.gov.il/he/Applicants/jobdetails?jobid={}'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer':    'https://www.taasuka.gov.il/he/Applicants/jobs',
    'Accept':     'text/html,application/xhtml+xml',
    'Accept-Language': 'he-IL,he;q=0.9,en;q=0.8',
}
FIELDNAMES = ['title', 'company', 'location', 'date', 'url',
              'department', 'workplace_type', 'source']

def ddmmyyyy_to_iso(s):
    parts = s.split('.')
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return s

def fetch_page(page=1):
    params = {
        'ProfessionCategoryCode': '',
        'FreeText':               '',
        'IsEmployersJobSearch':   'false',
        'page':                   page,
    }
    try:
        r = requests.get(BASE, params=params, headers=HEADERS, timeout=30)
        if not r.ok:
            print(f"    HTTP {r.status_code} on page {page}")
            return None
        return r.text
    except Exception as e:
        print(f"    x page {page}: {e}")
        return None

def get_total_pages(soup, jobs_per_page):
    """Extract total results from 'נמצאו X תוצאות' text."""
    text = soup.get_text()
    m = re.search(r'נמצאו\s*([\d,]+)\s*תוצאות', text)
    if m:
        total = int(m.group(1).replace(',', ''))
        pages = (total + jobs_per_page - 1) // jobs_per_page
        print(f"  Total results: {total:,} → {pages} pages")
        return pages
    return 10  # fallback

def parse_jobs(html):
    soup  = BeautifulSoup(html, 'html.parser')
    jobs  = []
    for item in soup.select('div.jobItem'):
        job_id = item.get('jobid', '')
        title  = (item.get('jobtitle') or '').strip()
        if not title:
            a = item.select_one('.jobTitle a')
            title = a.get_text(strip=True) if a else ''
        location     = ''
        updated_date = ''
        for d in item.select('.jobDetails div'):
            strong = d.find('strong')
            span   = d.find('span')
            if not strong or not span:
                continue
            label = strong.get_text(strip=True)
            value = span.get_text(strip=True)
            if 'מקום' in label:
                location = value
            elif 'תאריך' in label:
                updated_date = ddmmyyyy_to_iso(value)
        jobs.append({
            'title':          title,
            'company':        '',
            'location':       location,
            'date':           updated_date,
            'url':            JOB_URL.format(job_id) if job_id else '',
            'department':     '',
            'workplace_type': '',
            'source':         'taasuka',
        })
    return soup, jobs

def write_csv(jobs, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(jobs)
    print(f"  Wrote {len(jobs)} rows → {filename}")

def run_taasuka():
    print("\n-- Taasuka (שירות התעסוקה) ------------------------------------------")
    all_jobs  = []
    seen_urls = set()

    html = fetch_page(1)
    if not html:
        print("  x Could not fetch page 1")
        return []

    soup, jobs = parse_jobs(html)
    jobs_per_page = len(jobs) if jobs else 10
    total_pages = get_total_pages(soup, jobs_per_page)

    for j in jobs:
        if j['url'] and j['url'] not in seen_urls:
            seen_urls.add(j['url'])
            all_jobs.append(j)
    print(f"  Page 1/{total_pages}: {len(jobs)} jobs")

    for page in range(2, total_pages + 1):
        time.sleep(0.5)
        html = fetch_page(page)
        if not html:
            break
        _, jobs = parse_jobs(html)
        if not jobs:
            print(f"  Page {page}: no jobs — stopping")
            break
        new = 0
        for j in jobs:
            if j['url'] and j['url'] not in seen_urls:
                seen_urls.add(j['url'])
                all_jobs.append(j)
                new += 1
        if page % 50 == 0 or page == total_pages:
            print(f"  Page {page}/{total_pages}: {len(all_jobs):,} total so far")
        if new == 0:
            print(f"  Page {page}: no new jobs — stopping")
            break

    print(f"  Total unique: {len(all_jobs):,}")
    return all_jobs

if __name__ == '__main__':
    jobs = run_taasuka()
    if jobs:
        write_csv(jobs, f'taasuka_jobs_{TODAY}.csv')
    else:
        print("  No jobs fetched — no file written.")
