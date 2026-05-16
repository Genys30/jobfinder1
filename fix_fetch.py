t = open('fetch_jobs.py', encoding='utf-8').read()

# Fix 1: details=true -> details=false in comeet
old1 = "r = requests.get(f'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={tok}&details=true', timeout=60, headers=HEADERS)"
new1 = "r = requests.get(f'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={tok}&details=false', timeout=60, headers=HEADERS)"
if old1 in t:
    t = t.replace(old1, new1)
    print('Fix 1 applied: details=true -> details=false')
else:
    print('Fix 1 NOT FOUND')

# Fix 2: add isinstance check + remove description extraction in comeet loop
old2 = """            pos = []
            for p in r.json():
                loc = p.get('location') or {}
                wt = p.get('workplace_type', '')
                city = loc.get('city') or loc.get('name', '')
                if not is_israel(city + ' ' + loc.get('name',''), loc.get('country',''), 'remote' in wt.lower()):
                    continue
                details = p.get('details') or {}
                raw_desc = (details.get('requirements_and_responsibilities') or
                            details.get('description') or
                            p.get('description') or '')
                # strip basic HTML tags if present
                description = re.sub(r'<[^>]+>', ' ', raw_desc).strip()[:1500]
                pos.append({'title': p.get('name',''), 'company': p.get('company_name') or name,
                    'location': city, 'date': (p.get('time_updated') or '')[:10],
                    'url': p.get('url_active_page') or p.get('url_comeet_hosted_page',''),
                    'department': p.get('department',''), 'workplace_type': wt,
                    'description': description})"""
new2 = """            pos = []
            for p in r.json():
                if not isinstance(p, dict): continue
                loc = p.get('location') or {}
                wt = p.get('workplace_type', '')
                city = loc.get('city') or loc.get('name', '')
                if not is_israel(city + ' ' + loc.get('name',''), loc.get('country',''), 'remote' in wt.lower()):
                    continue
                pos.append({'title': p.get('name',''), 'company': p.get('company_name') or name,
                    'location': city, 'date': (p.get('time_updated') or '')[:10],
                    'url': p.get('url_active_page') or p.get('url_comeet_hosted_page',''),
                    'department': p.get('department',''), 'workplace_type': wt})"""
if old2 in t:
    t = t.replace(old2, new2)
    print('Fix 2 applied: isinstance check + removed description from comeet')
else:
    print('Fix 2 NOT FOUND')

# Fix 3: update write_csv fields for comeet (remove description)
old3 = "write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type','description'], f'comeet_jobs_{TODAY}.csv')"
new3 = "write_csv(dedup_jobs(jobs), ['title','company','location','date','url','department','workplace_type'], f'comeet_jobs_{TODAY}.csv')"
if old3 in t:
    t = t.replace(old3, new3)
    print('Fix 3 applied: removed description from comeet write_csv')
else:
    print('Fix 3 NOT FOUND')

# Fix 4: Lever isinstance check
old4 = "                    item.get('text', '')\n                    for lst in (job.get('lists') or [])\n                    for item in (lst.get('content') or [])"
new4 = "                    item.get('text', '') if isinstance(item, dict) else str(item)\n                    for lst in (job.get('lists') or [])\n                    for item in (lst.get('content') or [])"
if old4 in t:
    t = t.replace(old4, new4)
    print('Fix 4 applied: Lever isinstance check')
elif new4 in t:
    print('Fix 4 already present')
else:
    print('Fix 4 NOT FOUND')

open('fetch_jobs.py', 'w', encoding='utf-8').write(t)
print('\nDone. Run: python fetch_jobs.py')
