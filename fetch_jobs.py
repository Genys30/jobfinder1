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


def dedup_jobs(jobs):
    seen = set()
    result = []
    for j in jobs:
        key = j.get('url') or (str(j.get('title','')).lower().strip() + '|' + str(j.get('company','')).lower().strip())
        if key and key not in seen:
            seen.add(key)
            result.append(j)
    return result

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
    write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'comeet_jobs_{TODAY}.csv')


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
    write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'greenhouse_jobs_{TODAY}.csv')


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
    write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'lever_jobs_{TODAY}.csv')


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
    write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'ashby_jobs_{TODAY}.csv')


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
    write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'workable_jobs_{TODAY}.csv')


# ══ BGU (אוניברסיטת בן-גוריון) ═══════════════════════════════════════════════
def run_bgu():
    print("\n-- BGU External Positions (בן-גוריון) -------------------------------")
    import re as _re
    from bs4 import BeautifulSoup as _BS
    URL = "https://bguhr.my.salesforce-sites.com/Gius?mode=external"

    def parse_date(s):
        m = _re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', s)
        if not m: return TODAY
        d, mo, y = m.group(1), m.group(2), m.group(3)
        if len(y) == 2: y = "20" + y
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    try:
        r = requests.get(URL, headers={**HEADERS,
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://bguhr.my.salesforce-sites.com/"}, timeout=30)
        r.raise_for_status()
        soup = _BS(r.text, "html.parser")
        jobs = []
        seen = set()
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 3: continue
            texts = [c.get_text(strip=True) for c in cells]
            if any(h in texts for h in ["שם המשרה", "מס' משרה", "Name"]): continue
            job_id   = texts[0].strip() if texts else ""
            title    = texts[1].strip() if len(texts) > 1 else ""
            date_str = texts[3].strip() if len(texts) > 3 else (texts[2].strip() if len(texts) > 2 else "")
            if not title: continue
            # Use job_id as dedup key since all URLs are the same
            if job_id in seen: continue
            seen.add(job_id or title)
            # Append job ID to title for identification
            display_title = f"{title} (מס' {job_id})" if job_id else title
            jobs.append({"title": display_title,
                "company": "אוניברסיטת בן-גוריון בנגב",
                "location": "באר שבע",
                "date": TODAY,
                "deadline": parse_date(date_str) if date_str else "",
                "url": URL,
                "department": "", "workplace_type": "onsite"})
        print(f"  + {len(jobs)}")
        write_csv(jobs, ["title","company","location","date","deadline","url","department","workplace_type"],
            f"bgu_jobs_{TODAY}.csv")
    except Exception as e:
        print(f"  x {e}")


# ══ WEIZMANN (Academic) ═══════════════════════════════════════════════════════
def run_weizmann():
    print("\n-- Weizmann Institute (אקדמי) ---------------------------------------")
    try:
        r = requests.get(
            "https://www.weizmann.ac.il/career/jobs",
            headers={**HEADERS, "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"},
            timeout=30)
        if not r.ok:
            print(f"  - {r.status_code}")
            return
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        jobs = []
        seen = set()
        for link in soup.select("a[href*='/career/jobs/']"):
            href = link.get("href", "")
            last = href.rstrip("/").split("?")[0].split("/")[-1]
            if not last.isdigit() and not (last and last != "jobs"):
                continue
            if not last or last == "jobs":
                continue
            url = "https://www.weizmann.ac.il" + href if href.startswith("/") else href
            if url in seen:
                continue
            seen.add(url)
            title_el = link.select_one("h2") or link.select_one("h3")
            title = (title_el.get_text(strip=True) if title_el else link.get_text(strip=True)).strip()
            if not title:
                continue
            department = ""
            workplace_type = ""
            for dt in link.select("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd: continue
                label = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                if "קטגוריה" in label: department = value
                elif "היקף" in label: workplace_type = value
            jobs.append({
                "title": title, "company": "מכון ויצמן למדע",
                "location": "רחובות", "date": TODAY, "url": url,
                "department": department, "workplace_type": workplace_type,
            })
        print(f"  + {len(jobs)}")
        write_csv(jobs, ["title","company","location","date","url","department","workplace_type"],
            f"weizmann_jobs_{TODAY}.csv")
    except Exception as e:
        print(f"  x {e}")


# ══ MITAM (NGO / third sector) ═══════════════════════════════════════════════
def run_mitam():
    print("\n-- Mitam (מגזר שלישי) -----------------------------------------------")
    SUPABASE_URL = "https://cbqnuxmnmimbdhmgfkwl.supabase.co/rest/v1/jobs"
    API_KEY      = "sb_publishable_v6lGDz5AuEgjmXsbJot1ig_TRxyFTeh"
    try:
        r = requests.get(SUPABASE_URL,
            params={
                "select": "id,title,location,job_type,field,created_at,slug,organizations(name)",
                "is_active": "eq.true",
                "order": "created_at.desc",
            },
            headers={
                "apikey": API_KEY,
                "Authorization": f"Bearer {API_KEY}",
                "Origin": "https://www.mitam-hr.org",
                "Referer": "https://www.mitam-hr.org/",
                "Accept": "application/json",
            },
            timeout=30)
        if not r.ok:
            print(f"  - {r.status_code} {r.text[:100]}")
            return
        raw = r.json()
        jobs = []
        for j in raw:
            if not j.get("title"): continue
            org  = j.get("organizations") or {}
            name = org.get("name", "") if isinstance(org, dict) else ""
            slug = j.get("slug") or str(j.get("id", ""))
            url  = f"https://www.mitam-hr.org/jobs/{slug}" if slug else "https://www.mitam-hr.org/Jobs"
            jobs.append({
                "title":          (j.get("title") or "").strip(),
                "company":        name or "עמותה",
                "location":       (j.get("location") or "").strip(),
                "date":           (j.get("created_at") or "")[:10] or TODAY,
                "url":            url,
                "department":     (j.get("field") or "").strip(),
                "workplace_type": (j.get("job_type") or "").strip(),
            })
        print(f"  + {len(jobs)}")
        write_csv(dedup_jobs(jobs),
            ['title','company','location','date','url','department','workplace_type'],
            f'mitam_jobs_{TODAY}.csv')
    except Exception as e:
        print(f"  x {e}")


# ══ HUJI ALUMNI CAREER (student/junior jobs) ══════════════════════════════════
def run_huji():
    print("\n-- HUJI Alumni Career (מרכז קריירה) ----------------------------------")
    import re as _re
    from bs4 import BeautifulSoup as _BS
    BASE_H   = "https://hujicareer.co.il"
    JOBS_URL = BASE_H + "/jobs/"
    REMOTE_KW = _re.compile(r'מהבית|remote', _re.I)
    HYBRID_KW = _re.compile(r'היברידי|hybrid', _re.I)
    LOC_KW = ["ירושלים","תל אביב","רמת גן","הרצליה","מהבית","חיפה",
              "באר שבע","רחובות","פתח תקווה","ראשון לציון","נתניה"]

    def fetch(url):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 404: return None
            r.raise_for_status()
            return _BS(r.text, "html.parser")
        except Exception as e:
            print(f"  [warn] {e}"); return None

    def parse(soup):
        jobs = []
        cards = soup.select("article") or soup.select(".jet-listing-grid__item")
        for card in cards:
            title_el = card.select_one("h4.jet-listing-dynamic-field__content") or \
                       card.select_one("h4") or card.select_one("h3")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title: continue
            link_el = card.select_one("a.elementor-button") or card.select_one("a[href*='/jobs/']")
            url = ""
            if link_el:
                href = link_el.get("href","")
                if href and href.rstrip("/") != BASE_H + "/jobs":
                    url = href if href.startswith("http") else BASE_H + href
            if not url: continue
            text = card.get_text(" ", strip=True)
            img = card.select_one("img[alt]")
            company = img.get("alt","").strip() if img else ""
            if company and len(company) > 40: company = ""
            pub_date = ""
            m = _re.search(r'(\d{2}/\d{2}/\d{4})', text)
            if m:
                p = m.group(1).split("/")
                pub_date = f"{p[2]}-{p[1]}-{p[0]}"
            location = next((kw for kw in LOC_KW if kw in text), "")
            wt = ("hybrid" if HYBRID_KW.search(title+" "+text[:100])
                  else "remote" if REMOTE_KW.search(title+" "+location)
                  else "onsite")
            jobs.append({"title": title, "company": company or "HUJI Alumni Career",
                "location": location, "date": pub_date or TODAY, "url": url,
                "department": "", "workplace_type": wt,
                "level": "junior", "source": "huji-alumni"})
        return jobs

    all_jobs = []; seen = set()
    for page in range(1, 20):
        url = JOBS_URL if page == 1 else BASE_H + f"/jobs/page/{page}/"
        soup = fetch(url)
        if not soup: break
        jobs = parse(soup)
        if not jobs: break
        new = 0
        for j in jobs:
            if j["url"] not in seen:
                seen.add(j["url"]); all_jobs.append(j); new += 1
        print(f"  Page {page}: +{new} (total {len(all_jobs)})")
        if not soup.select_one("a.next.page-numbers, .nav-next a"): break
        import time; time.sleep(0.5)

    print(f"  + {len(all_jobs)}")
    write_csv(all_jobs,
        ["title","company","location","date","url","department","workplace_type","level","source"],
        f"huji_alumni_jobs_{TODAY}.csv")


# ══ HUJI POSITIONS (האוניברסיטה העברית - משרות פנימיות) ══════════════════════
def run_huji_positions():
    print("\n-- HUJI Positions (האוניברסיטה העברית בירושלים) ----------------------")
    from bs4 import BeautifulSoup as _BS
    import time, re as _re

    SEARCH_URL = "https://huji.hunterhrms.com/search-results/"
    BASE = "https://huji.hunterhrms.com"

    def fetch_page(url):
        try:
            r = requests.get(url, headers={**HEADERS,
                "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
                "Referer": "https://huji.hunterhrms.com/"}, timeout=30)
            r.raise_for_status()
            return _BS(r.text, "html.parser")
        except Exception as e:
            print(f"  [warn] {e}")
            return None

    def parse_deadline(s):
        m = _re.search(r'(\d{1,2})[/.](\d{1,2})[/.](\d{4})', s)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        return ""

    def fetch_description(jobcode):
        url = f"{BASE}/job-details/?jobcode={jobcode}"
        soup = fetch_page(url)
        if not soup:
            return "", "", url
        desc, reqs = "", ""
        full_text = soup.get_text("\n", strip=True)
        for marker in ["תיאור המשרה", "תיאור התפקיד:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                for stop in ["דרישות המשרה", "דרישות התפקיד:", "הערות", "כפיפות:"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                desc = after.strip()
                break
        for marker in ["דרישות המשרה", "דרישות התפקיד:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                for stop in ["הערות", "כפיפות:", "היקף משרה:", "במסגרת מדיניות"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                reqs = after.strip()
                break
        return desc, reqs, url

    # ── Scrape listing page ───────────────────────────────────────────────────
    soup = fetch_page(SEARCH_URL)
    if not soup:
        print("  x could not fetch HUJI positions page")
        return

    jobs = []
    seen = set()

    for wrap in soup.select(".job-wrap"):
        label = wrap.select_one("label.job-title")
        if not label:
            continue
        jobcode = label.get("for", "").strip()
        if not jobcode or jobcode in seen:
            continue
        seen.add(jobcode)

        title = label.get_text(strip=True)
        # Remove checkbox text noise
        title = _re.sub(r'\s+', ' ', title).strip()

        campus_el = wrap.select_one("p.kampus")
        campus = campus_el.get_text(strip=True) if campus_el else "ירושלים"

        # Deadline
        deadline_raw = ""
        for el in wrap.select(".last-date, .date"):
            t = el.get_text(strip=True)
            if _re.search(r'\d{2}/\d{2}/\d{4}', t):
                deadline_raw = t
                break
        deadline = parse_deadline(deadline_raw)

        jobs.append({
            "title": title,
            "company": "האוניברסיטה העברית בירושלים",
            "location": "ירושלים",
            "date": TODAY,
            "deadline": deadline,
            "jobcode": jobcode,
            "campus": campus,
            "department": "",
            "workplace_type": "onsite",
            "description": "",
            "requirements": "",
            "url": "",
        })

    print(f"  Found {len(jobs)} listings — fetching descriptions...")

    for i, job in enumerate(jobs, 1):
        desc, reqs, url = fetch_description(job["jobcode"])
        job["description"] = desc
        job["requirements"] = reqs
        job["url"] = url
        print(f"  [{i}/{len(jobs)}] {job['title'][:60]}")
        time.sleep(0.4)

    print(f"  + {len(jobs)}")
    write_csv(
        jobs,
        ["title", "company", "location", "date", "deadline", "url",
         "jobcode", "campus", "department", "workplace_type",
         "description", "requirements"],
        f"huji_positions_{TODAY}.csv"
    )


# ══ TECHNION HR (טכניון - מכון טכנולוגי לישראל) ══════════════════════════════
def run_technion():
    print("\n-- Technion HR (טכניון) ---------------------------------------------")
    from bs4 import BeautifulSoup as _BS
    URL = "https://hr.technion.ac.il/positions/"
    try:
        r = requests.get(URL, headers={**HEADERS,
            "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
            "Referer": "https://hr.technion.ac.il/"}, timeout=30)
        r.raise_for_status()
        soup = _BS(r.text, "html.parser")
        jobs = []
        seen = set()
        for card in soup.select("div.wrapper-job"):
            title_el = card.select_one("span.col-3")
            dept_el  = card.select_one("span.col-2")
            num_el   = card.select_one("span.col-1")
            link_el  = card.select_one("a.wrap-btn") or card.select_one("div.wrap-btn a")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title: continue
            dept  = dept_el.get_text(strip=True)  if dept_el  else ""
            jobid = ""
            url   = URL
            if link_el:
                href = link_el.get("href", "")
                if "jobid=" in href:
                    jobid = href.split("jobid=")[-1]
                    url = f"https://hr.technion.ac.il/positions/?jobid={jobid}"
                elif href.startswith("http"):
                    url = href
            key = jobid or title
            if key in seen: continue
            seen.add(key)
            jobs.append({"title": title, "company": "הטכניון - מכון טכנולוגי לישראל",
                "location": "חיפה", "date": TODAY, "url": url,
                "department": dept, "workplace_type": "onsite"})
        print(f"  + {len(jobs)}")
        write_csv(jobs, ["title","company","location","date","url","department","workplace_type"],
            f"technion_jobs_{TODAY}.csv")
    except Exception as e:
        print(f"  x {e}")


# ── History snapshot ─────────────────────────────────────────────────────────
def update_history():
    import os, csv as _csv
    HIST = 'history.csv'
    FIELDS = ['date','total','cyber','fintech','health','media','hardware',
              'defence','automotive','gaming','public','industrial','retail',
              'agritech','foodtech','rd','product','data','design','sales',
              'marketing','operations','support','management','intern',
              'remote','hybrid','onsite',
              'linkedin','comeet','greenhouse','lever','ashby','workable']

    # Load all today's CSVs
    rows = []
    for src in ['comeet','greenhouse','lever','ashby','workable']:
        fname = f'{src}_jobs_{TODAY}.csv'
        if not os.path.exists(fname): continue
        with open(fname, encoding='utf-8-sig') as f:
            rows.extend(list(_csv.DictReader(f)))
    # LinkedIn: pick latest file
    import glob
    li_files = sorted(glob.glob('linkedin_jobs_*.csv'))
    if li_files:
        with open(li_files[-1], encoding='utf-8-sig') as f:
            li_rows = list(_csv.DictReader(f))
    else:
        li_rows = []

    all_rows = rows + li_rows

    def g(row, *keys):
        for k in keys:
            v = (row.get(k) or row.get(k.title()) or '').strip()
            if v: return v
        return ''

    # Sector keywords (mirrors JS classifier, simplified)
    SECTOR_KW = {
        'cyber':      re.compile(r'cyber|security|firewall|malware|threat|siem|edr|xdr|zero.?trust|vulnerab|appsec', re.I),
        'fintech':    re.compile(r'fintech|payment|bank|financial|credit|insur|trading|crypto|blockchain|lending|payroll|neobank', re.I),
        'health':     re.compile(r'health|medical|medtech|biotech|pharma|genomic|clinical|patient|therap|diagnos|imaging|dental|cardio', re.I),
        'media':      re.compile(r'media|adtech|advertis|broadcast|streaming|entertainment|influencer|social media', re.I),
        'hardware':   re.compile(r'semiconductor|chip|silicon|processor|fpga|embedded|firmware|sensor|radar|lidar|photonic|robotic|circuit|wafer', re.I),
        'defence':    re.compile(r'defense|defence|military|weapon|missile|uav|unmanned|aerospace|tactical|drone', re.I),
        'automotive': re.compile(r'automotive|vehicle|fleet|electric vehicle|autonomous|adas|self.?driving|telematics|mobility|charging', re.I),
        'gaming':     re.compile(r'gaming|casino|esport|lottery|mobile game|game studio|slot|poker|bingo|fantasy sport', re.I),
        'public':     re.compile(r'government|municipal|govtech|ministry|smart city|public sector|civic tech|e.?gov|federal', re.I),
        'industrial': re.compile(r'industrial|manufacturing|factory|production|supply chain|logistics|warehouse|scada|automation|industry 4|iot', re.I),
        'retail':     re.compile(r'retail|e.?commerce|marketplace|fashion|apparel|consumer goods|grocery|dtc|point.?of.?sale', re.I),
        'agritech':   re.compile(r'agro|agriculture|agritech|farm|crop|irrigation|precision farm|soil|fertilizer|livestock|harvest', re.I),
        'foodtech':   re.compile(r'food|foodtech|nutrition|protein|plant.?based|cultivated meat|fermentation|beverage|culinary|alternative protein', re.I),
    }
    ROLE_KW = {
        'intern':     re.compile(r'intern|internship|student|trainee|apprentice|co.?op|graduate program', re.I),
        'management': re.compile(r'vp|vice president|cto|coo|cpo|ciso|cmo|cfo|ceo|head of|director|general manager|chief ', re.I),
        'rd':         re.compile(r'engineer|developer|devops|sre|architect|backend|frontend|full.?stack|mobile|firmware|embedded|platform|security research|qa|tester|sdet|software', re.I),
        'product':    re.compile(r'product manager|product owner|pm|product lead|program manager|project manager|scrum', re.I),
        'data':       re.compile(r'data scientist|data engineer|data analyst|analytics engineer|ml engineer|machine learning|deep learning|ai engineer|bi engineer|business intelligence|llm|computer vision', re.I),
        'design':     re.compile(r'designer|ux|ui|user experience|figma|product design|visual design|graphic', re.I),
        'sales':      re.compile(r'account executive|account manager|sales engineer|business development|bd|sdr|bdr|pre.?sales|revenue|partnership', re.I),
        'marketing':  re.compile(r'marketing|growth|content|seo|sem|brand|demand generation|campaign|copywriter|social media|pr|field marketing', re.I),
        'support':    re.compile(r'customer success|customer support|technical support|implementation|solutions engineer|integration engineer|onboarding|professional services', re.I),
        'operations': re.compile(r'hr|human resources|recruiter|talent acquisition|finance|accounting|legal|procurement|office manager|operations|ops|admin|it manager|supply chain', re.I),
    }

    def classify_sector(company):
        c = company.lower()
        for sec, rx in SECTOR_KW.items():
            if rx.search(c): return sec
        return ''

    def classify_role(title):
        for role, rx in ROLE_KW.items():
            if rx.search(title): return role
        return ''

    counts = {f: 0 for f in FIELDS}
    counts['date'] = TODAY

    src_counts = {'linkedin': len(li_rows), 'comeet': 0, 'greenhouse': 0, 'lever': 0, 'ashby': 0, 'workable': 0}
    for src in ['comeet','greenhouse','lever','ashby','workable']:
        fname = f'{src}_jobs_{TODAY}.csv'
        if os.path.exists(fname):
            with open(fname, encoding='utf-8-sig') as f:
                src_counts[src] = sum(1 for _ in _csv.DictReader(f))

    counts['total']    = len(all_rows)
    counts['linkedin'] = src_counts['linkedin']
    counts['comeet']   = src_counts['comeet']
    counts['greenhouse']= src_counts['greenhouse']
    counts['lever']    = src_counts['lever']
    counts['ashby']    = src_counts['ashby']
    counts['workable'] = src_counts['workable']

    for row in all_rows:
        title   = g(row,'title','job_title','position')
        company = g(row,'company','company_name','employer')
        wt      = g(row,'workplace_type','work_type','location').lower()
        sec = classify_sector(company)
        if sec: counts[sec] = counts.get(sec, 0) + 1
        role = classify_role(title)
        if role: counts[role] = counts.get(role, 0) + 1
        if 'remote'  in wt: counts['remote']  += 1
        elif 'hybrid' in wt: counts['hybrid']  += 1
        elif wt: counts['onsite'] += 1

    # Read existing history, skip today if already present
    existing = []
    if os.path.exists(HIST):
        with open(HIST, encoding='utf-8-sig') as f:
            existing = list(_csv.DictReader(f))
        existing = [r for r in existing if r.get('date') != TODAY]

    existing.append(counts)

    with open(HIST, 'w', newline='', encoding='utf-8-sig') as f:
        w = _csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        w.writeheader()
        w.writerows(existing)

    print(f"  -> history.csv updated ({len(existing)} days)")

# ══ TAU (אוניברסיטת תל אביב) ══════════════════════════════════════════════════
def run_tau():
    print("\n-- TAU (אוניברסיטת תל אביב) -----------------------------------------")
    from bs4 import BeautifulSoup as _BS
    import time, re as _re

    LISTING_URL = "https://www.tau.ac.il/positions?qt-jobs_tabs=0"
    BASE = "https://www.tau.ac.il"

    def fetch_page(url):
        try:
            r = requests.get(url, headers={**HEADERS,
                "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
                "Referer": "https://www.tau.ac.il/"}, timeout=30)
            r.raise_for_status()
            return _BS(r.text, "html.parser")
        except Exception as e:
            print(f"  [warn] {e}")
            return None

    def parse_deadline(s):
        m = _re.search(r'(\d{1,2})[/.](\d{1,2})[/.](\d{4})', s)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        return ""

    def fetch_description(url):
        soup = fetch_page(url)
        if not soup:
            return "", ""
        desc, reqs = "", ""
        full_text = soup.get_text("\n", strip=True)
        for marker in ["תיאור התפקיד:", "תיאור המשרה:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                for stop in ["דרישות התפקיד:", "דרישות המשרה:", "כפיפות:", "היקף משרה:", "מעמד משרה:"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                desc = after.strip()
                break
        for marker in ["דרישות התפקיד:", "דרישות המשרה:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                for stop in ["כפיפות:", "היקף משרה:", "מעמד משרה:", "המשרה מיועדת", "הגשת מועמדות"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                reqs = after.strip()
                break
        return desc, reqs

    soup = fetch_page(LISTING_URL)
    if not soup:
        print("  x could not fetch TAU positions page")
        return

    jobs = []
    seen = set()
    tables = soup.select("table")
    tab_labels = ["administrative", "academic_staff"]

    for tab_idx, table in enumerate(tables[:2]):
        staff_type = tab_labels[tab_idx] if tab_idx < len(tab_labels) else "administrative"
        for row in table.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            link_el = cells[0].find("a")
            if not link_el:
                continue
            title = link_el.get_text(strip=True)
            if not title:
                continue
            href = link_el.get("href", "")
            url = BASE + href if href.startswith("/") else href
            if not url or url in seen:
                continue
            seen.add(url)
            department = cells[1].get_text(strip=True)
            internal_external = cells[2].get_text(strip=True)
            deadline_raw = cells[3].get_text(strip=True)
            deadline = parse_deadline(deadline_raw)
            jobs.append({
                "title": title,
                "company": "אוניברסיטת תל אביב",
                "location": "תל אביב",
                "date": TODAY,
                "deadline": deadline,
                "url": url,
                "department": department,
                "workplace_type": "onsite",
                "staff_type": staff_type,
                "internal_external": internal_external,
                "description": "",
                "requirements": "",
            })

    print(f"  Found {len(jobs)} listings — fetching descriptions...")
    for i, job in enumerate(jobs, 1):
        desc, reqs = fetch_description(job["url"])
        job["description"] = desc
        job["requirements"] = reqs
        print(f"  [{i}/{len(jobs)}] {job['title'][:60]}")
        time.sleep(0.4)

    print(f"  + {len(jobs)}")
    write_csv(
        jobs,
        ["title", "company", "location", "date", "deadline", "url",
         "department", "workplace_type", "staff_type", "internal_external",
         "description", "requirements"],
        f"tau_jobs_{TODAY}.csv"
    )

# ══ HAIFA UNIVERSITY (אוניברסיטת חיפה) ══════════════════════════════════════
def run_haifa():
    print("\n-- Haifa University (אוניברסיטת חיפה) --------------------------------")
    from bs4 import BeautifulSoup as _BS
    import re as _re

    URL = "https://hr.haifa.ac.il/%D7%93%D7%A8%D7%95%D7%A9%D7%99%D7%9D/"

    try:
        r = requests.get(URL, headers={**HEADERS,
            "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
            "Referer": "https://hr.haifa.ac.il/"}, timeout=30)
        r.raise_for_status()
        soup = _BS(r.text, "html.parser")
    except Exception as e:
        print(f"  [warn] {e}")
        return

    jobs = []
    seen = set()

    # Each job is an <a> tag with job number + title as text, linking to detail page
    # Pattern: links whose href contains a job number slug under hr.haifa.ac.il
    BASE = "https://hr.haifa.ac.il"

    # Jobs are in the main content — find all <a> links that look like job listings
    # Each job link text starts with a number (job code) e.g. "3326 כותב/ת..."
    content = soup.select_one("#main, .site-main, article, .entry-content") or soup

    for a in content.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        # Job links: text starts with digits (job code)
        if not _re.match(r'^\d+\s+\S', text):
            continue
        if not href.startswith("http"):
            href = BASE + href
        if "hr.haifa.ac.il" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)

        # Split job code from title
        m = _re.match(r'^(\d+)\s+(.+)$', text)
        if not m:
            continue
        job_code = m.group(1).strip()
        title = m.group(2).strip()

        # Find the sibling content block after this link for description/requirements
        # The page embeds full text in expandable divs after each link
        parent = a.find_parent()
        desc, reqs, dept = "", "", ""

        # Walk up to find the containing block and extract text sections
        block = a.find_parent(["div", "section", "article", "li"])
        if block:
            full_text = block.get_text("\n", strip=True)
            # Extract description
            for marker in ["תיאור התפקיד:", "תיאור התפקיד"]:
                if marker in full_text:
                    after = full_text.split(marker, 1)[1]
                    for stop in ["דרישות תפקיד:", "דרישות התפקיד:", "דרישות התפקיד", "שפות:", "היקף משרה:"]:
                        if stop in after:
                            after = after.split(stop, 1)[0]
                    desc = after.strip()
                    break
            # Extract requirements
            for marker in ["דרישות תפקיד:", "דרישות התפקיד:", "דרישות התפקיד"]:
                if marker in full_text:
                    after = full_text.split(marker, 1)[1]
                    for stop in ["שפות:", "היקף משרה:", "קורות חיים", "אוניברסיטת חיפה מקדמת"]:
                        if stop in after:
                            after = after.split(stop, 1)[0]
                    reqs = after.strip()
                    break
            # Extract department (ביחידה:)
            dm = _re.search(r'ביחידה[:\s]+([^\n]+)', full_text)
            if dm:
                dept = dm.group(1).strip()

        jobs.append({
            "title": title,
            "company": "אוניברסיטת חיפה",
            "location": "חיפה",
            "date": TODAY,
            "deadline": "",
            "url": href,
            "job_code": job_code,
            "department": dept,
            "workplace_type": "onsite",
            "description": desc,
            "requirements": reqs,
        })
        print(f"  {job_code}: {title[:60]}")

    print(f"  + {len(jobs)}")
    write_csv(
        jobs,
        ["title", "company", "location", "date", "deadline", "url",
         "job_code", "department", "workplace_type", "description", "requirements"],
        f"haifa_jobs_{TODAY}.csv"
    )


# ══ PLAYWRIGHT HELPER ════════════════════════════════════════════════════════
def _pw_get(url, wait_selector=None, wait_ms=2000):
    """Fetch a page using Playwright headless Chromium. Returns HTML string or None."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"))
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if wait_selector:
                try: page.wait_for_selector(wait_selector, timeout=8000)
                except: pass
            else:
                page.wait_for_timeout(wait_ms)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"  [pw error] {e}")
        return None


# ══ KPMG ISRAEL ═══════════════════════════════════════════════════════════════
def run_kpmg():
    print("\n-- KPMG Israel -------------------------------------------------------")
    from bs4 import BeautifulSoup as _BS
    import time, re as _re

    LIST_URL = "https://kpmg.co.il/technologyconsulting/he/vacancies/technology-consulting"
    BASE = "https://kpmg.co.il"

    html = _pw_get(LIST_URL, wait_selector="a[href*='/vacancies/technology-consulting/']")
    if not html:
        print("  x could not fetch KPMG listing page"); return

    soup = _BS(html, "html.parser")
    job_links = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/vacancies/technology-consulting/" not in href: continue
        if href.rstrip("/") == LIST_URL.rstrip("/"): continue
        url = href if href.startswith("http") else BASE + href
        if url in seen: continue
        seen.add(url)
        job_links.append(url)

    print(f"  Found {len(job_links)} listings — fetching descriptions...")
    jobs = []

    for i, url in enumerate(job_links, 1):
        detail_html = _pw_get(url)
        if not detail_html:
            print(f"  [{i}] skip — could not fetch"); continue
        d = _BS(detail_html, "html.parser")
        h1 = d.select_one("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title: continue

        full = d.get_text("\n", strip=True)
        city = "תל אביב"
        if "חיפה" in full or "Haifa" in full: city = "חיפה"

        # Department from breadcrumb or meta
        dept = "Technology Consulting"
        dept_el = d.select_one(".breadcrumb li:last-child, .category, .department")
        if dept_el: dept = dept_el.get_text(strip=True)

        desc, reqs = "", ""
        for marker in ["About the job", "about the job", "We are seeking", "we are seeking"]:
            if marker in full:
                after = full.split(marker, 1)[1]
                for stop in ["Requirements", "requirement", "The role"]:
                    if stop in after: after = after.split(stop, 1)[0]
                desc = after.strip(); break
        for marker in ["Requirements", "requirement"]:
            if marker in full:
                after = full.split(marker, 1)[1]
                for stop in ["The position is open", "position is open", "למדהים", "Share"]:
                    if stop in after: after = after.split(stop, 1)[0]
                reqs = after.strip(); break

        jobs.append({
            "title": title, "company": "KPMG Israel",
            "city": city, "date": TODAY, "url": url,
            "department": dept, "workplace_type": "onsite",
            "description": desc, "requirements": reqs,
        })
        print(f"  [{i}/{len(job_links)}] {title[:60]}")

    print(f"  + {len(jobs)}")
    write_csv(jobs,
        ["title","company","city","date","url","department","workplace_type","description","requirements"],
        f"kpmg_jobs_{TODAY}.csv")


# ══ DELOITTE ISRAEL ════════════════════════════════════════════════════════════
def run_deloitte():
    print("\n-- Deloitte Israel ---------------------------------------------------")
    from bs4 import BeautifulSoup as _BS
    import re as _re

    LIST_URL = "https://careers.deloitte.co.il/positions/"
    BASE = "https://careers.deloitte.co.il"

    html = _pw_get(LIST_URL, wait_selector=".positions-list, .job-item, article", wait_ms=3000)
    if not html:
        print("  x could not fetch Deloitte listing page"); return

    soup = _BS(html, "html.parser")
    jobs = []
    seen = set()

    # Try to find job items — Deloitte uses a custom WordPress plugin
    # Each job: title text, location, department in repeated blocks
    lines = [l.strip() for l in soup.get_text("\n").split("\n") if l.strip()]

    i = 0
    while i < len(lines):
        line = lines[i]
        # Location line pattern: "City, Israel"
        if i + 1 < len(lines) and _re.match(r'^[\w\s\-\']+,\s*Israel$', lines[i+1]):
            if (4 < len(line) < 80
                    and not _re.search(r'positions|Deloitte|Why|Search|About|Interns|\d+ positions|Services|Industries|Privacy|Cookie|Facebook|Instagram', line, _re.I)):
                city_m = _re.match(r'^([\w\s\-\']+),\s*Israel$', lines[i+1])
                city = city_m.group(1).strip() if city_m else "תל אביב"
                # Map English city names
                city_map = {"Tel Aviv": "תל אביב", "Haifa": "חיפה", "Jerusalem": "ירושלים",
                            "Yokneam Illit": "יוקנעם עילית", "Yokne'am Illit": "יוקנעם עילית"}
                city = city_map.get(city, city)
                dept = lines[i+2].strip() if i+2 < len(lines) else ""
                key = line.lower() + "|" + city
                if key not in seen:
                    seen.add(key)
                    jobs.append({
                        "title": line, "company": "Deloitte Israel",
                        "city": city, "date": TODAY,
                        "url": BASE + "/positions/",
                        "department": dept, "workplace_type": "onsite",
                        "description": "", "requirements": "",
                    })
                i += 3; continue
        i += 1

    print(f"  + {len(jobs)}")
    write_csv(jobs,
        ["title","company","city","date","url","department","workplace_type","description","requirements"],
        f"deloitte_jobs_{TODAY}.csv")


# ══ EY ISRAEL ═════════════════════════════════════════════════════════════════
def run_ey():
    print("\n-- EY Israel ---------------------------------------------------------")
    from bs4 import BeautifulSoup as _BS
    import time, re as _re

    CAREER_URL = "https://ey.co.il/career/"
    BASE = "https://ey.co.il"

    html = _pw_get(CAREER_URL, wait_selector="a[href*='/open-jobs/']")
    if not html:
        print("  x could not fetch EY career page"); return

    soup = _BS(html, "html.parser")
    job_urls = []
    seen_urls = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/open-jobs/" not in href: continue
        url = href if href.startswith("http") else BASE + href
        if url in seen_urls: continue
        seen_urls.add(url)
        job_urls.append(url)

    print(f"  Found {len(job_urls)} job links — fetching details...")
    jobs = []
    seen_titles = set()

    for i, url in enumerate(job_urls, 1):
        detail_html = _pw_get(url)
        if not detail_html:
            print(f"  [{i}] skip"); continue

        d = _BS(detail_html, "html.parser")
        h1 = d.select_one("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title: continue

        t_key = title.lower().strip()
        if t_key in seen_titles: continue
        seen_titles.add(t_key)

        full = d.get_text("\n", strip=True)

        city = "תל אביב"
        if "חיפה" in full: city = "חיפה"

        # Department — look for list items under h1
        dept = ""
        for el in d.select("ul li, .job-meta span, .job-tags span"):
            t = el.get_text(strip=True)
            if t and t not in ["משרה מלאה","תל-אביב","חיפה","משרה חלקית","Full time"] and len(t) < 50:
                dept = t; break

        worktype = "parttime" if "משרה חלקית" in full else "fulltime"

        desc, reqs = "", ""
        if "תיאור התפקיד" in full:
            after = full.split("תיאור התפקיד", 1)[1]
            for stop in ["מה נדרש", "דרישות", "להגשת מועמדות"]:
                if stop in after: after = after.split(stop, 1)[0]
            desc = after.strip()
        if "מה נדרש" in full:
            after = full.split("מה נדרש", 1)[1]
            for stop in ["להגשת מועמדות", "הכירו את הצוות", "חזרה לעמוד"]:
                if stop in after: after = after.split(stop, 1)[0]
            reqs = after.strip()

        jobs.append({
            "title": title, "company": "EY Israel",
            "city": city, "date": TODAY, "url": url,
            "department": dept, "workplace_type": worktype,
            "description": desc, "requirements": reqs,
        })
        print(f"  [{i}/{len(job_urls)}] {title[:60]}")

    print(f"  + {len(jobs)}")
    write_csv(jobs,
        ["title","company","city","date","url","department","workplace_type","description","requirements"],
        f"ey_jobs_{TODAY}.csv")


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
    run_mitam()
    run_weizmann()
    run_bgu()
    run_huji()
    run_huji_positions()
    run_technion()
    run_tau()
    run_haifa()
    run_kpmg()
    run_deloitte()
    run_ey()
    print("\nUpdating history...")
    update_history()
    print("\n=== All done ===")

if __name__ == '__main__':
    main()
