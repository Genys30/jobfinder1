#!/usr/bin/env python3
"""
fetch_mitam.py — Fetches NGO jobs from mitam-hr.org via Supabase REST API.
Outputs mitam_jobs_YYYY-MM-DD.csv to repo root.
Run nightly via GitHub Actions.
"""

import csv
import sys
from datetime import date

import requests

TODAY       = date.today().isoformat()
OUTPUT_FILE = f"mitam_jobs_{TODAY}.csv"

SUPABASE_URL = "https://cbqnuxmnmimbdhmgfkwl.supabase.co/rest/v1/jobs"
API_KEY      = "sb_publishable_v6lGDz5AuEgjmXsbJot1ig_TRxyFTeh"

PARAMS = {
    "select": "id,title,description,location,job_type,field,created_at,logo_url,is_hot,slug,organizations(name,logo_url)",
    "is_active": "eq.true",
    "order": "created_at.desc",
}

HEADERS = {
    "apikey": API_KEY,
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Origin": "https://www.mitam-hr.org",
    "Referer": "https://www.mitam-hr.org/",
}

FIELDNAMES = ["title", "company", "location", "date", "url", "department", "workplace_type", "source"]


def fetch_jobs() -> list:
    print("[mitam] Fetching from Supabase API...")
    try:
        r = requests.get(SUPABASE_URL, params=PARAMS, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[mitam] ERROR: {e}", file=sys.stderr)
        return []


def norm(job: dict) -> dict:
    org  = job.get("organizations") or {}
    name = org.get("name", "") if isinstance(org, dict) else ""
    slug = job.get("slug") or str(job.get("id", ""))
    url  = f"https://www.mitam-hr.org/jobs/{slug}" if slug else "https://www.mitam-hr.org/Jobs"
    created = (job.get("created_at") or "")[:10]
    return {
        "title":          (job.get("title") or "").strip(),
        "company":        name or "עמותה",
        "location":       (job.get("location") or "").strip(),
        "date":           created or TODAY,
        "url":            url,
        "department":     (job.get("field") or "").strip(),
        "workplace_type": (job.get("job_type") or "").strip(),
        "source":         "mitam",
    }


def main():
    raw = fetch_jobs()
    if not raw:
        print("[mitam] No jobs returned — no file written.")
        return

    jobs = [norm(j) for j in raw if j.get("title")]
    print(f"[mitam] {len(jobs)} jobs → {OUTPUT_FILE}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(jobs)

    print("[mitam] Done.")


if __name__ == "__main__":
    main()
