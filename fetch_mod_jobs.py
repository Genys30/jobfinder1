#!/usr/bin/env python3
"""
fetch_mod_jobs.py — משרד הביטחון job scraper
Source: https://jobs.mod.gov.il/api/TenderPublish/GetAllPublished
Output: mod_jobs_YYYY-MM-DD.csv  (jobfinder-compatible format)

NOTE: The API requires browser-like headers. Run this locally (not in CI),
or pass a session cookie with --cookie if the server starts blocking:
    python fetch_mod_jobs.py --cookie ".AspNetCore.Session=xxxx"

Run:
    python fetch_mod_jobs.py
"""

import requests
import csv
import sys
import argparse
from datetime import date, datetime

API_URL  = "https://jobs.mod.gov.il/api/TenderPublish/GetAllPublished"
SOURCE   = "mod"
OUT_FILE = f"mod_jobs_{date.today().isoformat()}.csv"

FIELDNAMES = [
    "id", "title", "company", "location", "date_posted",
    "deadline", "url", "source", "education", "experience", "summary"
]


def fmt_date(s):
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s.rstrip("Z")).strftime("%Y-%m-%d")
    except Exception:
        return s[:10]


def clean(s):
    """Strip pipes (used as line-breaks in source) and normalize whitespace."""
    if not s:
        return ""
    return " ".join(s.replace("|", " ").split())


def fetch_jobs(cookie=None):
    print(f"Fetching from {API_URL} ...")
    headers = {
        "Content-Type": "application/json",
        "Accept":        "application/json, text/plain, */*",
        "Referer":       "https://jobs.mod.gov.il/",
        "Origin":        "https://jobs.mod.gov.il",
        "User-Agent":    (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    if cookie:
        headers["Cookie"] = cookie

    try:
        resp = requests.post(API_URL, headers=headers, json={}, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    if data.get("HasError"):
        print(f"API error: {data.get('Messages')}", file=sys.stderr)
        sys.exit(1)

    tenders = data.get("Data", [])
    print(f"  Got {len(tenders)} tenders")
    return tenders


def parse_tender(t):
    bank_job    = t.get("BankJob") or {}
    hr_job      = bank_job.get("HrJob") or {}
    publish     = t.get("TenderPublish") or {}
    tender_id   = t.get("Id")

    return {
        "id":          t.get("TenderObjectID") or str(tender_id),
        "title":       clean(hr_job.get("JobName") or bank_job.get("JobName") or ""),
        "company":     clean(bank_job.get("DepartmentName") or "משרד הביטחון"),
        "location":    clean(hr_job.get("JobAreaDescription") or ""),
        "date_posted": fmt_date(publish.get("StartDate") or t.get("CreatedAt")),
        "deadline":    fmt_date(t.get("NomineesApplyingDate")),
        "url":         f"https://jobs.mod.gov.il/#/Tenders?TenderId={tender_id}",
        "source":      SOURCE,
        "education":   clean(hr_job.get("Education") or ""),
        "experience":  clean(hr_job.get("Experience") or ""),
        "summary":     clean(hr_job.get("GeneralSummary") or ""),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookie", default=None,
                        help="Optional session cookie string if plain request is blocked")
    args = parser.parse_args()

    tenders = fetch_jobs(cookie=args.cookie)
    rows    = [parse_tender(t) for t in tenders if t.get("BankJob")]

    # Deduplicate by TenderObjectID
    seen, unique = set(), []
    for r in rows:
        if r["id"] not in seen:
            seen.add(r["id"])
            unique.append(r)

    print(f"  Writing {len(unique)} jobs → {OUT_FILE}")
    with open(OUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(unique)

    print("Done.")


if __name__ == "__main__":
    main()
