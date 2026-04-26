#!/usr/bin/env python3
"""
fetch_huji.py — Scrapes jobs from HUJI Career Center (hujicareer.co.il/jobs)
WordPress site, paginated. Jobs tagged as junior level, source=huji.
Outputs huji_jobs_YYYY-MM-DD.csv
"""

import csv
import re
import sys
from datetime import date
from time import sleep

import requests
from bs4 import BeautifulSoup

TODAY       = date.today().isoformat()
OUTPUT_FILE = f"huji_jobs_{TODAY}.csv"
BASE        = "https://hujicareer.co.il"
PAGE_URL    = BASE + "/jobs/page/{}/"
JOBS_URL    = BASE + "/jobs/"
FIELDNAMES  = ["title", "company", "location", "date", "url",
               "department", "workplace_type", "level", "source"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}

# Workplace type detection from title/location
REMOTE_KW  = re.compile(r'מהבית|remote|היברידי|hybrid', re.I)
HYBRID_KW  = re.compile(r'היברידי|hybrid', re.I)


def fetch_page(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  [warn] {url}: {e}", file=sys.stderr)
        return None


def parse_jobs(soup: BeautifulSoup) -> list[dict]:
    jobs = []

    # Each job card: article or div with a link + h4 title
    # From HTML: img, company name, h4 title, date, location, "לכל הפרטים" link
    cards = (
        soup.select("article.type-post")
        or soup.select("article")
        or soup.select(".job-listing, .job_listing")
        or soup.select(".elementor-post")
    )

    # Fallback: find all "לכל הפרטים" links and work backwards
    if not cards:
        detail_links = soup.select("a[href*='/jobs/']")
        detail_links = [a for a in detail_links
                        if a.get_text(strip=True) in ("לכל הפרטים", "לפרטים נוספים", "קרא עוד")
                        or re.search(r'/jobs/[^/]+/$', a.get("href", ""))]
        for link in detail_links:
            href = link.get("href", "")
            if not href or href.rstrip("/") == BASE + "/jobs":
                continue
            # Walk up to find the card container
            container = link.parent
            for _ in range(5):
                if container is None:
                    break
                container = container.parent

            if container:
                cards.append(container)

    for card in cards:
        # Title
        title_el = (card.select_one("h4") or card.select_one("h3") or
                    card.select_one("h2") or card.select_one(".job-title"))
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        # URL
        link_el = card.select_one("a[href*='/jobs/']")
        url = ""
        if link_el:
            href = link_el.get("href", "")
            if href and href.rstrip("/") != BASE + "/jobs":
                url = href if href.startswith("http") else BASE + href

        # Company
        company = ""
        img = card.select_one("img[alt]")
        if img:
            company = img.get("alt", "").strip()
        if not company:
            # Try to find text before the h4
            paras = card.select("p, span, div")
            for p in paras:
                t = p.get_text(strip=True)
                if t and t != title and len(t) < 60 and not re.search(r'\d{2}/\d{2}', t):
                    company = t
                    break

        # Date — dd/mm/yyyy pattern
        pub_date = ""
        text = card.get_text(" ", strip=True)
        m = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if m:
            parts = m.group(1).split("/")
            pub_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

        # Location
        location = ""
        loc_keywords = ["ירושלים", "תל אביב", "רמת גן", "הרצליה", "מהבית", "חיפה",
                        "באר שבע", "רחובות", "פתח תקווה", "ראשון לציון", "נתניה"]
        for kw in loc_keywords:
            if kw in text:
                location = kw
                break

        # Workplace type
        if HYBRID_KW.search(title + " " + location + " " + text[:200]):
            workplace_type = "hybrid"
        elif REMOTE_KW.search(title + " " + location + " " + text[:200]):
            workplace_type = "remote"
        else:
            workplace_type = "onsite"

        if title and url:
            jobs.append({
                "title":          title,
                "company":        company or "HUJI Career",
                "location":       location,
                "date":           pub_date or TODAY,
                "url":            url,
                "department":     "",
                "workplace_type": workplace_type,
                "level":          "junior",
                "source":         "huji",
            })

    return jobs


def scrape_all() -> list[dict]:
    all_jobs: list[dict] = []
    seen_urls: set[str] = set()

    for page_num in range(1, 20):
        url = JOBS_URL if page_num == 1 else PAGE_URL.format(page_num)
        print(f"[huji] Page {page_num}: {url}")
        soup = fetch_page(url)

        if soup is None:
            print(f"[huji] Page {page_num} not found — stopping.")
            break

        jobs = parse_jobs(soup)
        if not jobs:
            print(f"[huji] No jobs on page {page_num} — stopping.")
            break

        new = 0
        for j in jobs:
            if j["url"] and j["url"] not in seen_urls:
                seen_urls.add(j["url"])
                all_jobs.append(j)
                new += 1

        print(f"[huji]   → {new} new (total: {len(all_jobs)})")

        # Check for next page
        next_link = soup.select_one("a.next.page-numbers, .nav-next a")
        if not next_link:
            print(f"[huji] No next page — done.")
            break

        sleep(0.5)

    return all_jobs


def main():
    jobs = scrape_all()
    if not jobs:
        print("[huji] No jobs found — no file written.")
        return

    print(f"[huji] {len(jobs)} jobs → {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(jobs)
    print("[huji] Done.")


if __name__ == "__main__":
    main()
