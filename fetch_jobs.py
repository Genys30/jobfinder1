"""
fetch_jobs.py  —  Nightly GitHub Action
Sources: Comeet · Greenhouse · Lever · Ashby · Workable
Outputs one dated CSV per source.
"""

import requests, csv, json, re
from datetime import date

# ── Config ─────────────────────────────────────────────────────────────────
TECHMAP_FNS = [
    'admin','business','data-science','design','devops','finance','frontend',
    'hardware','hr','legal','marketing','procurement-operations','product',
    'project-management','qa','sales','security','software','support'
]
TECHMAP_BASE = 'https://raw.githubusercontent.com/mluggy/techmap/main/jobs/'
HEADERS      = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}
TODAY        = date.today().isoformat()

ISRAEL_COUNTRIES = {'IL','ISR','ISRAEL'}
ISRAEL_CITIES = {
    'tel aviv','tel-aviv','herzliya','haifa','jerusalem','beer sheva',
    "be'er sheva",'petah tikva','raanana',"ra'anana",'netanya','rehovot',
    'rishon lezion','holon','bnei brak','kfar saba','modiin','ashkelon',
    'ashdod','bat yam','givatayim','rosh haayin','lod','ramla','nazareth',
    'hadera','caesarea','yokneam','matam','airport city','kiryat gat',
    'hod hasharon','even yehuda','ramat gan','petah tiqwa','rishon le zion'
}

# ── Shared helpers ──────────────────────────────────────────────────────────
def is_israel(text: str, country: str = '', remote: bool = False) -> bool:
    t = text.lower()
    if remote: return True
    if country.upper() in ISRAEL_COUNTRIES: return True
    if 'israel' in t: return True
    return any(c in t for c in ISRAEL_CITIES)

def load_extras(fname: str) -> list:
    try:
        data = json.load(open(fname))
        print(f"  extras: {len(data)} from {fname}")
        return data
    except FileNotFoundError:
        return []

def write_csv(rows: list, fields: list, fname: str):
    with open(fname, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)
    print(f"  → {len(rows)} jobs saved to {fname}")

# ── Techmap scanner ─────────────────────────────────────────────────────────
def scan_techmap():
    patterns = {
        'comeet':    re.compile(r'comeet\.co[m]?/jobs/([^/\s"\']+)/([0-9A-Fa-f]{2,}\.[0-9A-Fa-f]{3,})', re.I),
        'greenhouse':re.compile(r'boards(?:\.eu)?\.greenhouse\.io/([^/\s"\'?#]+)/jobs', re.I),
        'lever':     re.compile(r'jobs\.lever\.co/([^/\s"\'?#]+)/', re.I),
        'ashby':     re.compile(r'jobs\.ashbyhq\.com/([^/\s"\'?#]+)/', re.I),
        'workable':  re.compile(r'(?:apply\.workable\.com|([^.\s"\']+)\.workable\.com)/([^/\s"\'?#]+)/', re.I),
    }
    found = {k: {} for k in patterns}

    for fn in TECHMAP_FNS:
        try:
            r = requests.get(TECHMAP_BASE + fn + '.csv', timeout=30, headers=HEADERS)
            if not r.ok: continue
            for row in csv.DictReader(r.text.splitlines()):
                url  = row.get('url','')
                comp = row.get('company','')
                # Comeet
                m = patterns['comeet'].search(url)
                if m:
                    slug,uid = m.group(1).lower(), m.group(2).upper()
                    k = f"{slug}/{uid}"
                    if k not in found['comeet']:
                        found['comeet'][k] = {'slug':slug,'uid':uid,'name':comp or slug}
                # Others (single token)
                for src in ('greenhouse','lever','smartr'):
                    m = patterns[src].search(url)
                    if m:
                        t = m.group(1).lower()
                        if t not in found[src]:
                            found[src][t] = {'token':t,'name':comp or t}
                # Recruitee (subdomain or careers.recruitee.com)
                m = patterns['recruitee'].search(url)
                if m:
                    t = (m.group(1) or m.group(2) or '').lower().strip('/')
                    if t and t not in found['recruitee']:
                        found['recruitee'][t] = {'token':t,'name':comp or t}
        except Exception as e:
            print(f"  warn: techmap/{fn} — {e}")

    for src,d in found.items():
        print(f"  techmap {src}: {len(d)}")
    return found

# ══════════════════════════════════════════════════════════════════════════
# COMEET
# ══════════════════════════════════════════════════════════════════════════
def comeet_token(slug, uid):
    url = f'https://www.comeet.com/jobs/{slug}/{uid}'
    try:
        r = requests.get(url, timeout=30, headers=HEADERS)
        if not r.ok: return None
        for p in [r'"token"\s*:\s*"([0-9A-F]{16,})"',r"'token'\s*:\s*'([0-9A-F]{16,})'",
                  r'[?&]token=([0-9A-F]{16,})',r'"companyToken"\s*:\s*"([0-9A-F]{16,})"']:
            m = re.search(p, r.text, re.I)
            if m: return m.group(1).upper()
    except: pass
    return None

def comeet_jobs(uid, token, name):
    url = f'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={token}&details=false'
    try:
        r = requests.get(url, timeout=60, headers=HEADERS)
        if not r.ok: return []
        jobs = []
        for pos in r.json():
            loc   = pos.get('location') or {}
            wtype = pos.get('workplace_type','')
            city  = loc.get('city') or loc.get('name','')
            if not is_israel(city+' '+loc.get('name',''), loc.get('country',''), 'remote' in wtype.lower()):
                continue
            jobs.append({'title':pos.get('name',''),'company':pos.get('company_name') or name,
                'location':city,'date':(pos.get('time_updated') or '')[:10],
                'url':pos.get('url_active_page') or pos.get('url_comeet_hosted_page',''),
                'department':pos.get('department',''),'workplace_type':wtype})
        return jobs
    except: return []

def run_comeet(tm):
    print("\n── Comeet ───────────────────────────────────────────────────────")
    seen = set(); all_c = []
    for c in list(tm.values()) + load_extras('comeet_extra_companies.json'):
        k = f"{c['slug'].lower()}/{c['uid'].upper()}"
        if k not in seen: seen.add(k); all_c.append(c)
    print(f"  Companies: {len(all_c)}")
    jobs = []
    for i,c in enumerate(all_c,1):
        print(f"  [{i}/{len(all_c)}] {c.get('name',c['slug'])}")
        tok = comeet_token(c['slug'], c['uid'])
        if not tok: print("    ✗ token"); continue
        pos = comeet_jobs(c['uid'], tok, c.get('name',''))
        print(f"    ✓ {len(pos)}"); jobs.extend(pos)
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'comeet_jobs_{TODAY}.csv')

# ══════════════════════════════════════════════════════════════════════════
# GREENHOUSE
# ══════════════════════════════════════════════════════════════════════════
def run_greenhouse(tm):
    print("\n── Greenhouse ───────────────────────────────────────────────────")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('greenhouse_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i,c in enumerate(all_t,1):
        token,name = c['token'],c.get('name',c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://boards-api.greenhouse.io/v1/boards/{token}/jobs', timeout=30, headers=HEADERS)
            if not r.ok: print(f"    — {r.status_code}"); continue
            pos = []
            for job in r.json().get('jobs',[]):
                loc_name = job.get('location',{}).get('name','')
                offices  = job.get('offices',[])
                country  = next((o.get('country_code','') for o in offices if o.get('country_code')),'')
                office_names = ' '.join(o.get('name','') for o in offices)
                if not is_israel(loc_name+' '+office_names, country):
                    continue
                wt = 'Remote' if re.search(r'\bremote\b',loc_name,re.I) else ('Hybrid' if re.search(r'\bhybrid\b',loc_name,re.I) else '')
                dept = next((d.get('name','') for d in job.get('departments',[])), '')
                pos.append({'title':job.get('title',''),'company':name,
                    'location':loc_name.split(',')[0].strip(),
                    'date':(job.get('updated_at') or '')[:10],
                    'url':job.get('absolute_url',''),'department':dept,'workplace_type':wt})
            print(f"    ✓ {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    ✗ {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'greenhouse_jobs_{TODAY}.csv')

# ══════════════════════════════════════════════════════════════════════════
# LEVER
# ══════════════════════════════════════════════════════════════════════════
def run_lever(tm):
    print("\n── Lever ────────────────────────────────────────────────────────")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('lever_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i,c in enumerate(all_t,1):
        token,name = c['token'],c.get('name',c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://api.lever.co/v0/postings/{token}?mode=json', timeout=30, headers=HEADERS)
            if not r.ok: print(f"    — {r.status_code}"); continue
            pos = []
            for job in r.json():
                cats     = job.get('categories',{})
                loc      = cats.get('location','')
                wtype    = job.get('workplaceType','')
                remote   = wtype == 'remote'
                if not is_israel(loc, remote=remote):
                    continue
                # createdAt is milliseconds
                ts = job.get('createdAt',0)
                dt = date.fromtimestamp(ts/1000).isoformat() if ts else ''
                pos.append({'title':job.get('text',''),'company':name,
                    'location':loc,'date':dt,
                    'url':job.get('hostedUrl',''),
                    'department':cats.get('team',''),'workplace_type':wtype})
            print(f"    ✓ {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    ✗ {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'lever_jobs_{TODAY}.csv')

# ══════════════════════════════════════════════════════════════════════════
# ASHBY
# ══════════════════════════════════════════════════════════════════════════
def run_smartr(tm):
    print("\n── SmartRecruiters ──────────────────────────────────────────────")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('smartr_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i,c in enumerate(all_t,1):
        token,name = c['token'],c.get('name',c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            # Fetch IL jobs + remote jobs separately
            all_pos = []
            for url in [
                f'https://api.smartrecruiters.com/v1/companies/{token}/postings?country=il&limit=100',
                f'https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=100',
            ]:
                r = requests.get(url, timeout=30, headers=HEADERS)
                if not r.ok: break
                for job in r.json().get('content',[]):
                    loc  = job.get('location',{})
                    city = loc.get('city','')
                    country = loc.get('country','')
                    remote  = loc.get('remote',False)
                    if not is_israel(city, country, remote): continue
                    wt = 'Remote' if remote else ''
                    dept = (job.get('department') or {}).get('label','')
                    released = (job.get('releasedDate') or '')[:10]
                    apply_url = job.get('ref') or f"https://jobs.smartrecruiters.com/{token}/{job.get('id','')}"
                    all_pos.append({'title':job.get('name',''),'company':name,
                        'location':city,'date':released,
                        'url':apply_url,'department':dept,'workplace_type':wt})
            # Dedupe within this company
            seen_urls = set()
            pos = []
            for j in all_pos:
                k = j['url'] or (j['title']+'|'+j['company'])
                if k not in seen_urls: seen_urls.add(k); pos.append(j)
            print(f"    ✓ {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    ✗ {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'smartr_jobs_{TODAY}.csv')

# ══════════════════════════════════════════════════════════════════════════
# WORKABLE
# ══════════════════════════════════════════════════════════════════════════
def run_recruitee(tm):
    print("\n── Recruitee ────────────────────────────────────────────────────")
    seen = set(); all_t = []
    for c in list(tm.values()) + load_extras('recruitee_extra_companies.json'):
        t = c['token'].lower()
        if t not in seen: seen.add(t); all_t.append(c)
    print(f"  Companies: {len(all_t)}")
    jobs = []
    for i,c in enumerate(all_t,1):
        token,name = c['token'],c.get('name',c['token'])
        print(f"  [{i}/{len(all_t)}] {name}")
        try:
            r = requests.get(f'https://careers.recruitee.com/api/c/{token}/offers',
                timeout=30, headers=HEADERS)
            if not r.ok: print(f"    — {r.status_code}"); continue
            pos = []
            for job in r.json().get('offers',[]):
                city    = job.get('city','')
                country = job.get('country','')
                remote  = job.get('remote',False)
                loc_str = job.get('location','')
                if not is_israel(loc_str+' '+city+' '+country, remote=remote):
                    continue
                wt = 'Remote' if remote else ('Hybrid' if 'hybrid' in loc_str.lower() else '')
                dept = ''
                for tag in job.get('tags',[]):
                    if tag.get('type') == 'department': dept = tag.get('name',''); break
                pos.append({'title':job.get('title',''),'company':name,
                    'location':city or loc_str.split(',')[0].strip(),
                    'date':(job.get('created_at') or '')[:10],
                    'url':job.get('url','') or f"https://careers.recruitee.com/c/{token}",
                    'department':dept,'workplace_type':wt})
            print(f"    ✓ {len(pos)}"); jobs.extend(pos)
        except Exception as e: print(f"    ✗ {e}")
    write_csv(jobs, ['title','company','location','date','url','department','workplace_type'], f'recruitee_jobs_{TODAY}.csv')

# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f"=== fetch_jobs.py  {TODAY} ===\n")
    print("Scanning techmap…")
    tm = scan_techmap()
    run_comeet(tm['comeet'])
    run_greenhouse(tm['greenhouse'])
    run_lever(tm['lever'])
    run_smartr(tm['smartr'])
    run_recruitee(tm['recruitee'])
    print("\n=== All done ===")

if __name__ == '__main__':
    main()
