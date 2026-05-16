import requests, re, json

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; jobfinder-bot/1.0)'}

data = json.load(open('companies.json'))
comeet_companies = [c for c in data if c.get('comeet')]
print(f'Comeet companies in companies.json: {len(comeet_companies)}')

c = comeet_companies[0]
slug_uid = c['comeet']
slug, uid = slug_uid.split('/', 1)
name = c['name']
print(f'\nTesting: {name} ({slug}/{uid})')

def comeet_token(slug, uid):
    try:
        r = requests.get(f'https://www.comeet.com/jobs/{slug}/{uid}', timeout=30, headers=HEADERS)
        for p in [r'"token"\s*:\s*"([0-9A-F]{16,})"', r"'token'\s*:\s*'([0-9A-F]{16,})'",
                  r'[?&]token=([0-9A-F]{16,})', r'"companyToken"\s*:\s*"([0-9A-F]{16,})"']:
            m = re.search(p, r.text, re.I)
            if m: return m.group(1).upper()
    except Exception as e:
        print(f'  token fetch error: {e}')
    return None

tok = comeet_token(slug, uid)
print(f'Token: {tok}')

if tok:
    url = f'https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={tok}&details=false'
    print(f'URL: {url}')
    r = requests.get(url, timeout=30, headers=HEADERS)
    print(f'Status: {r.status_code}')
    print(f'Response (first 500 chars):\n{r.text[:500]}')
    resp = r.json()
    print(f'\nType: {type(resp)}')
    if isinstance(resp, list):
        print(f'Length: {len(resp)}')
        if resp:
            print(f'First item type: {type(resp[0])}')
            if isinstance(resp[0], dict):
                print(f'First item keys: {list(resp[0].keys())[:8]}')
            elif isinstance(resp[0], list):
                print(f'First item is LIST: {resp[0][:3]}')
            else:
                print(f'First item: {resp[0]}')
    elif isinstance(resp, dict):
        print(f'Dict keys: {list(resp.keys())}')
