import requests, csv, re
from datetime import date

TODAY = date.today().isoformat()

def _strip_html(html):
    text = re.sub(r'<[^>]+>', ' ', html or '')
    return re.sub(r'\s+', ' ', text).strip()

r = requests.post(
    "https://jobs.clalitapps.co.il/CandidateAPI/api//position/Search/9E6C0368-A39E-4D83-803E-CF2AF0BA28DD",
    json={"KeyWords": "", "CategoryId": ["0"], "countryId": 2, "cityId": []},
    headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
    timeout=60
)
positions = r.json().get("positions", [])
print(f"Fetched {len(positions)} positions")

jobs = []
for p in positions:
    title   = (p.get("jobTitleText") or "").strip()
    company = (p.get("affiliateDisplayName") or "כללית").strip()
    if not title: continue
    pid = p.get("compPositionID")
    jobs.append({
        "title": title, "company": company,
        "location": (p.get("displayLocation") or "").strip(),
        "date": (p.get("activationDate") or "")[:10],
        "url": f"https://jobs.clalitapps.co.il/clalit/redmatch-apply/redmatch.apply.html?compPositionID={pid}",
        "department": (p.get("fieldDesc") or "").strip(),
        "workplace_type": "",
        "description": _strip_html(p.get("description") or p.get("shortDescription") or ""),
    })

fname = f"clalit_jobs_{TODAY}.csv"
with open(fname, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["title","company","location","date","url","department","workplace_type","description"])
    w.writeheader(); w.writerows(jobs)
print(f"Saved {len(jobs)} jobs to {fname}")