#!/usr/bin/env python3
"""
fetch_weizmann.py — Scrapes jobs from Weizmann Institute (weizmann.ac.il/career/jobs)
Drupal site, server-rendered HTML, all jobs on one page (~54 total).
Outputs weizmann_jobs_YYYY-MM-DD.csv
"""

import csv
import sys
from datetime import date

import requests
from bs4 import BeautifulSoup

TODAY       = date.today().isoformat()
OUTPUT_FILE = f"weizmann_jobs_{TODAY}.csv"
BASE        = "https://www.weizmann.ac.il"
JOBS_URL    = f"{BASE}/career/jobs"
FIELDNAMES  = ["title", "company", "location", "date", "url", "department", "workplace_type"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}


def scrape():
    print(f"[weizmann] Fetching {JOBS_URL}")
    try:
        r = requests.get(JOBS_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[weizmann] ERROR: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []

    # Each job is an <a> tag linking to /career/jobs/NNNNN
    for link in soup.select("a[href*='/career/jobs/']"):
        href = link.get("href", "")
        # Skip non-job links (e.g. /career/jobs?categories=7)
        if not href.split("?")[0].rstrip("/").split("/")[-1].isdigit():
            # also try slug-based URLs
            last = href.rstrip("/").split("/")[-1]
            if not last or last == "jobs":
                continue

        url = BASE + href if href.startswith("/") else href

        # Title — direct text or nested h2/h3
        title_el = link.select_one("h2") or link.select_one("h3")
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        title = title.strip()
        if not title:
            continue

        # Category and job type from sibling spans inside the link
        spans = link.select("span")
        department = ""
        workplace_type = ""
        for span in spans:
            text = span.get_text(strip=True)
            prev = span.find_previous_sibling()
            label_el = span.find_previous("span", class_=lambda c: c and "label" in c) if prev else None
            # Try to find label text nearby
            parent_text = span.parent.get_text(" ", strip=True) if span.parent else ""
            if "קטגוריה" in parent_text or "category" in parent_text.lower():
                department = text
            elif "היקף" in parent_text or "משרה" in text:
                workplace_type = text

        # Fallback: grab all dt/dd pairs inside the link
        for dt in link.select("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if "קטגוריה" in label:
                department = value
            elif "היקף" in label:
                workplace_type = value

        jobs.append({
            "title":          title,
            "company":        "מכון ויצמן למדע",
            "location":       "רחובות",
            "date":           TODAY,
            "url":            url,
            "department":     department,
            "workplace_type": workplace_type,
        })

    # Deduplicate by URL
    seen = set()
    deduped = []
    for j in jobs:
        if j["url"] not in seen:
            seen.add(j["url"])
            deduped.append(j)

    return deduped


def main():
    jobs = scrape()
    if not jobs:
        print("[weizmann] No jobs found — no file written.")
        return

    print(f"[weizmann] {len(jobs)} jobs → {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(jobs)
    print("[weizmann] Done.")


if __name__ == "__main__":
    main()
