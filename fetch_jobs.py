"""
fetch_jobs.py  —  Nightly GitHub Action
Sources: Comeet · Greenhouse · Lever · SmartRecruiters · Recruitee
"""
import requests, csv, json, re
from datetime import date

TECHMAP_FNS = [
    'admin','business','data-science','design','devops','finance','frontend',
    'hardware','hr','legal','marketing','procurement-operations','product',
    'project-management','qa','sales','security','software','support'
]
TECHMAP_BASE = 'https://raw.githubusercontent.com/mluggy/techmap/main/jobs/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
TODAY = date.today().isoformat()

ISRAEL_COUNTRIES = {'IL','ISR','ISRAEL'}
ISRAEL_CITIES = {
    'tel aviv','tel-aviv','herzliya','haifa','jerusalem','beer sheva',
    "be'er sheva",'petah tikva','raanana','netanya','rehovot',
    'rishon lezion','holon','bnei brak','kfar saba','modiin','ashkelon',
    'ashdod','bat yam','givatayim','rosh haayin','lod','ramla','nazareth',
    'hadera','caesarea','yokneam','matam','airport city','kiryat gat',
    'hod hasharon','ramat gan'
}

def is_israel(text='', country='', remote=False):
    if remote: return True
    if country.upper() in ISRAEL_COUNTRIES: return True
    t = text.lower()
    if 'israel' in t: return True
    return any(c in t for c in ISRAEL_CITIES)

def load_extras(fname):
    try:
        data = json.load(open(fname))
        print(f"  extras: {len(data)} from {fname}")
        return data
    except FileNotFoundError:
        return []

def write_csv(rows, fields, fname):
    with open(fname, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {len(rows)} jobs saved to {fname}")

# ── Techmap scanner ──────────────────────────────────────────────────────────
def scan_techmap():
    pat_comeet = re.compile(r'comeet\.co[m]?/jobs/([^/\s"\']+)/([0-9A-Fa-f]{2,}\.[0-9A-Fa-f]{3,})', re.I)
    pat_gh     = re.compile(r'boards(?:\.eu)?\.greenhouse\.io/([^/\s"\'?#]+)/jobs', re.I)
    pat_lever  = re.compile(r'jobs\.lever\.co/([^/\s"\'?#]+)/', re.I)

    comeet = {}
    gh = {}
    lever = {}

    for fn in TECHMAP_FNS:
        try:
            r = requests.get(TECHMAP_BASE + fn + '.csv', timeout=30, headers=HEADERS)
            if not r.ok: continue
            for row in csv.DictReader(r.text.splitlines()):
                url  = row.get('url','')
                comp = row.get('company','')

                m = pat_comeet.search(url)
                if m:
                    slug, uid = m.group(1).lower(), m.group(2).upper()
                    k = f"{slug}/{uid}"
                    if k not in comeet:
                        comeet[k] = {'slug': slug, 'uid': uid, 'name': comp or slug}

                for pat, d in [(pat_gh, gh), (pat_lever, lever)]:
                    m = pat.search(url)
                    if m:
                        t = m.group(1).lower()
                        if t not in d:
                            d[t] = {'token': t, 'name': comp or t}

        except Exception as e:
            print(f"  warn: techmap/{fn} - {e}")

    print(f"  techmap: comeet={len(comeet)} gh={len(gh)} lever={len(lever)}")
    return comeet, gh, lever


# ══ COMEET ═══════════════════════════════════════════════════════════════════
def comeet_token(slug, uid):
    try:
        r = requests.get(f'https://www.comeet.com/jobs/{slug}/{uid}', timeout=30, headers=HEADERS)
        if not r.ok: return None
        for p in [r'"token"\s*:\s*"([0-9A-F]{16,})"', r"'token'\s*:\s*'([0-9A-F]{16,})'",
                  r'[?&]token=([0-9A-F]{16,})', r'"companyToken"\s*:\s*"([0-9A-F]{16,})"']:
            m = re.search(p, r.text, re.I)
            if m: return m.group(1).upper()
    except: pass
    return None

def run_comeet(tm):
    print("\n-- Comeet -----------------------------------------------------------")
    seen = set(); all_c = []
    for c in list(tm.values()) + load_extras('comeet_extra_companies.json'):
        k = f"{c['slug'].lower()}/{c['uid'].upper()}"
        if k not in seen: seen.add(k); all_c.append(c)
    print(f"  Companies: {len(all_c)}")
    jobs = []
    for i, c in enumerate(all_c, 1):
        slug, uid, name = c['slug'], c['uid'], c.get('name', c['slug'])
        print(f"  [{i}/{len(all_c)}] {name}")
        tok = comeet_token(slug, uid)
        if not tok: print("    x token not found"); continue
        try:
            r = requests.get(f'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={tok}&details=false', timeout=60, headers=HEADERS)
            if not r.ok: print(f"    - {r.status_code}"); continue
            pos = []
            for p in r.json():
                loc = p.get('location') or {}
                wt = p.get('workplace_type', '')
                city = loc.get('city') or loc.get('name', '')
                if not is_israel(city + ' ' + loc.get('name',''), loc.get('country',''), 'remote' in wt.lower()):
                    continue
                pos.append({'title': p.get('name',''), 'company': p.get('company_name') or name,
                    'location': city, 'date': (p.get('time_updated') or '')[:10],
                    'url': p.get('url_active_page') or p.get('url_comeet_hosted_page',''),
                    'department': p.get('department',''), 'workplace_type': wt})
            print(f"    + {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    x {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'comeet_jobs_{TODAY}.csv')


# ══ GREENHOUSE ════════════════════════════════════════════════════════════════
def run_greenhouse(tm):
    print("\n-- Greenhouse -------------------------------------------------------")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('greenhouse_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i, c in enumerate(all_t, 1):
        token, name = c['token'], c.get('name', c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://boards-api.greenhouse.io/v1/boards/{token}/jobs', timeout=30, headers=HEADERS)
            if not r.ok: print(f"    - {r.status_code}"); continue
            pos = []
            for job in r.json().get('jobs', []):
                loc_name = job.get('location', {}).get('name', '')
                offices  = job.get('offices', [])
                country  = next((o.get('country_code','') for o in offices if o.get('country_code')), '')
                office_names = ' '.join(o.get('name','') for o in offices)
                if not is_israel(loc_name + ' ' + office_names, country): continue
                wt = 'Remote' if re.search(r'\bremote\b', loc_name, re.I) else ('Hybrid' if re.search(r'\bhybrid\b', loc_name, re.I) else '')
                dept = next((d.get('name','') for d in job.get('departments',[])), '')
                pos.append({'title': job.get('title',''), 'company': name,
                    'location': loc_name.split(',')[0].strip(),
                    'date': (job.get('updated_at') or '')[:10],
                    'url': job.get('absolute_url',''), 'department': dept, 'workplace_type': wt})
            print(f"    + {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    x {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'greenhouse_jobs_{TODAY}.csv')


# ══ LEVER ════════════════════════════════════════════════════════════════════
def run_lever(tm):
    print("\n-- Lever ------------------------------------------------------------")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('lever_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i, c in enumerate(all_t, 1):
        token, name = c['token'], c.get('name', c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://api.lever.co/v0/postings/{token}?mode=json', timeout=30, headers=HEADERS)
            if not r.ok: print(f"    - {r.status_code}"); continue
            pos = []
            for job in r.json():
                cats  = job.get('categories', {})
                loc   = cats.get('location', '')
                wtype = job.get('workplaceType', '')
                if not is_israel(loc, remote=(wtype == 'remote')): continue
                ts = job.get('createdAt', 0)
                dt = date.fromtimestamp(ts/1000).isoformat() if ts else ''
                pos.append({'title': job.get('text',''), 'company': name,
                    'location': loc, 'date': dt, 'url': job.get('hostedUrl',''),
                    'department': cats.get('team',''), 'workplace_type': wtype})
            print(f"    + {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    x {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'lever_jobs_{TODAY}.csv')


# ══ ASHBY ════════════════════════════════════════════════════════════════════
def run_ashby(tm):
    print("\n-- Ashby -----------------------------------------------------------")
    seen = set(); all_t = []
    extras = load_extras('ashby_extra_companies.json')
    for c in extras:
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i, c in enumerate(all_t, 1):
        token, name = c['token'], c.get('name', c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://api.ashbyhq.com/posting-api/job-board/{token}',
                timeout=30, headers=HEADERS)
            if not r.ok: print(f"    - {r.status_code}"); continue
            pos = []
            for job in r.json().get('jobPostings', []):
                if not job.get('isListed', True): continue
                loc    = job.get('locationName', '')
                remote = job.get('locationIsRemote', False)
                if not is_israel(loc, remote=remote): continue
                wt = 'Remote' if remote else ('Hybrid' if 'hybrid' in loc.lower() else '')
                pos.append({'title': job.get('title',''), 'company': name,
                    'location': loc, 'date': (job.get('publishedDate') or '')[:10],
                    'url': job.get('externalLink') or job.get('jobUrl',''),
                    'department': job.get('departmentName',''), 'workplace_type': wt})
            print(f"    + {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    x {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'ashby_jobs_{TODAY}.csv')


# ══ WORKABLE ═════════════════════════════════════════════════════════════════
def run_workable(tm):
    print("\n-- Workable --------------------------------------------------------")
    seen = set(); all_t = []
    extras = load_extras('workable_extra_companies.json')
    for c in extras:
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i, c in enumerate(all_t, 1):
        token, name = c['token'], c.get('name', c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://apply.workable.com/api/v1/widget/accounts/{token}',
                timeout=30, headers=HEADERS)
            if not r.ok: print(f"    - {r.status_code}"); continue
            pos = []
            for job in r.json().get('jobs', []):
                loc    = job.get('location', {})
                city   = loc.get('city','')
                country= loc.get('country_code','')
                remote = loc.get('telecommuting', False)
                loc_str= loc.get('location_str','') or f"{city}, {loc.get('country','')}"
                if not is_israel(loc_str + ' ' + city, country, remote): continue
                wt = 'Remote' if remote else ''
                pos.append({'title': job.get('title',''), 'company': name,
                    'location': city or loc_str.split(',')[0].strip(),
                    'date': (job.get('created_at') or '')[:10],
                    'url': job.get('url',''), 'department': job.get('department',''),
                    'workplace_type': wt})
            print(f"    + {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    x {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'workable_jobs_{TODAY}.csv')


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"=== fetch_jobs.py  {TODAY} ===\n")
    print("Scanning techmap...")
    comeet, gh, lever = scan_techmap()
    run_comeet(comeet)
    run_greenhouse(gh)
    run_lever(lever)
    run_ashby({})
    run_workable({})
    print("\n=== All done ===")

if __name__ == '__main__':
    main()
