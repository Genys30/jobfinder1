"""
fetch_gotfriends.py  —  GotFriends scraper for jobfinder
Watermark-based: only fetches jobs newer than last run.
"""

import csv, re, sys, time, json
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL       = "https://www.gotfriends.co.il"
WATERMARK_FILE = Path(__file__).parent / "gotfriends_watermark.txt"
OUTPUT_DIR     = Path(__file__).parent
TODAY          = date.today().isoformat()
OUTFILE        = OUTPUT_DIR / f"gotfriends_jobs_{TODAY}.csv"
DELAY          = 1.5
MAX_PAGES      = 300

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL + "/",
}

def load_watermark():
    if WATERMARK_FILE.exists():
        try:
            return int(WATERMARK_FILE.read_text().strip())
        except ValueError:
            pass
    return 0

def save_watermark(max_id):
    WATERMARK_FILE.write_text(str(max_id))

def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()

def page_url(page):
    return f"{BASE_URL}/jobs/?page={page}"

def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # Each job has a div.career_num with the job ID.
    # Walk up to find the job container, then extract title/desc/req.
    for id_div in soup.find_all("div", class_="career_num"):
        # Extract job ID
        m = re.search(r"\d{4,}", id_div.get_text())
        if not m:
            continue
        job_id = int(m.group())

        # The job container is a parent of id_div.
        # Walk up until we find a tag that also contains an <a href="/jobslobby/...">
        container = id_div
        job_link = None
        for _ in range(8):  # max 8 levels up
            container = container.parent
            if container is None:
                break
            job_link = container.find("a", href=lambda h: h and "/jobslobby/" in h)
            if job_link:
                break

        if not job_link:
            continue

        title   = clean(job_link.get_text())
        href    = job_link["href"]
        job_url = href if href.startswith("http") else BASE_URL + href

        # Location: look for element containing "מיקום"
        location = ""
        loc_el = container.find(string=re.compile(r"מיקום"))
        if loc_el:
            loc_text = clean(loc_el.parent.get_text())
            location = re.sub(r"מיקום\s*:?\s*", "", loc_text).strip()

        # Description and requirements: div.desc blocks inside container
        description  = ""
        requirements = ""
        for desc_div in container.find_all("div", class_="desc"):
            title_c = desc_div.find("div", class_="title_c")
            if not title_c:
                continue
            header = clean(title_c.get_text())
            # Remove the title_c from the text so we only get the body
            title_c.decompose()
            body = clean(desc_div.get_text(separator=" "))
            if "תיאור" in header:
                description = body
            elif "דרישות" in header:
                requirements = body

        jobs.append({
            "title":        title,
            "company":      "אנונימי / סודי",
            "location":     location or "Israel",
            "date":         TODAY,
            "url":          job_url,
            "source":       "gotfriends",
            "job_id":       job_id,
            "description":  description,
            "requirements": requirements,
        })

    return jobs

def scrape(watermark):
    all_jobs = []
    new_max  = watermark
    session  = requests.Session()
    session.headers.update(HEADERS)

    for page in range(1, MAX_PAGES + 1):
        url = page_url(page)
        print(f"  page {page}…", end=" ", flush=True)

        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            r.encoding = "utf-8"
        except requests.RequestException as e:
            print(f"ERROR ({e}) — stopping")
            break

        jobs = parse_page(r.text)

        if not jobs:
            print("no jobs found — end of results")
            break

        new_on_page  = 0
        hit_watermark = False
        for j in jobs:
            if j["job_id"] <= watermark:
                hit_watermark = True
            else:
                all_jobs.append(j)
                new_on_page += 1
                if j["job_id"] > new_max:
                    new_max = j["job_id"]

        print(f"{len(jobs)} total, {new_on_page} new")

        if hit_watermark:
            print(f"  ↳ reached watermark ({watermark}) — done")
            break

        time.sleep(DELAY)

    return all_jobs, new_max

def write_csv(jobs):
    fieldnames = ["title","company","location","date","url","source",
                  "job_id","description","requirements"]
    with open(OUTFILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(jobs)
    # Update file index for index.html
    files = sorted(p.name for p in OUTPUT_DIR.glob("gotfriends_jobs_*.csv"))
    (OUTPUT_DIR / "gotfriends_file_index.json").write_text(
        json.dumps({"files": files})
    )
    print(f"Wrote {len(jobs)} jobs → {OUTFILE}")

if __name__ == "__main__":
    watermark = load_watermark()
    print(f"GotFriends scraper — watermark: {watermark or '(first run)'}")

    jobs, new_max = scrape(watermark)

    if not jobs:
        print("No new jobs.")
        sys.exit(0)

    write_csv(jobs)
    save_watermark(new_max)
    print(f"Watermark updated → {new_max}")
