#!/usr/bin/env python3
"""
update_history.py
─────────────────
Runs nightly (via GitHub Actions) to append one row to history.csv.
Each row = today's snapshot of ALL job counts broken down by:
  • Role type  (rd, product, data, sales, marketing, support, operations, management, design, intern, administrative)
  • Level      (entry, mid, senior, advanced, director)
  • Work type  (remote, hybrid, onsite)
  • Source     (linkedin, comeet, greenhouse, lv, ab, wk, direct, mt,
                weizmann, bgu, technion, huji, tau, haifa, kpmg, deloitte,
                ey, joint, bar, bar-alumni, bis)

Usage:
    python update_history.py
    python update_history.py --date 2026-05-01   # backfill a specific date
    python update_history.py --dry-run            # print row, don't write
"""

import csv
import glob
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

# ── CLI ───────────────────────────────────────────────────────────────────────
DRY_RUN   = "--dry-run" in sys.argv
FORCE_DATE = next((a for a in sys.argv[1:] if re.match(r"\d{4}-\d{2}-\d{2}", a)), None)
TODAY     = FORCE_DATE or date.today().isoformat()
HISTORY   = "history.csv"

# ── CSV columns (order matters — must match existing history.csv if it exists) ─
COLUMNS = [
    "date", "total",
    # Role types
    "rd", "product", "data", "sales", "marketing", "support",
    "operations", "management", "design", "intern", "administrative",
    # Levels
    "entry", "mid", "senior", "advanced", "director",
    # Work types
    "remote", "hybrid", "onsite",
    # Sources
    "linkedin", "comeet", "greenhouse", "lv", "ab", "wk", "direct",
    "mt", "weizmann", "bgu", "technion", "huji", "tau", "haifa",
    "kpmg", "deloitte", "ey", "joint", "bar", "bar-alumni", "bis",
]

# ── Classifiers (mirror the JS functions exactly) ─────────────────────────────

SEGMENTS = {
    "intern":      re.compile(r"\b(intern|internship|student|trainee|apprentice|co-op|coop|graduate program|סטודנט|מתמחה|מתנדב)\b", re.I),
    "management":  re.compile(r"\b(vp|vice president|cto|coo|cpo|ciso|cmo|cfo|ceo|head of|director|general manager|gm\b|chief |ראש אגף|ראש תחום|מנהל.?ת אגף|מנהל.?ת מערך|מנכ\"ל|סמנכ\"ל|ראש צוות|מנהל.?ת מחלקה|מנהל.?ת המרכז|מנהל.?ת מערך)\b", re.I),
    "rd":          re.compile(r"\b(engineer|developer|dev\b|devops|sre|architect|backend|front.?end|full.?stack|mobile|ios|android|firmware|embedded|infrastructure|platform|security research|reverse engineer|qa\b|quality assurance|tester|testing|sdet|software|r&d|research and develop)\b|מהנדס|מפתח|איש.אשת סיסטם|תומך.ת מחשוב|תומך.ת IT|מנהל.ת מעבדה|מהנדס.ת מעבדה|ביואינפורמטיק|לבורנט|טכנאי|מומחה.ית מולטימדיה|אחראי.ת ניהול זהויות|מיישמ", re.I),
    "product":     re.compile(r"\b(product manager|product owner|pm\b|product lead|product director|vp product|program manager|project manager|scrum|agile coach)\b|מנהל.ת פרויקט|מנהל.ת תוכנית|מנהל.ת תכנון", re.I),
    "data":        re.compile(r"\b(data scientist|data engineer|data analyst|analytics engineer|ml engineer|machine learning|deep learning|ai engineer|artificial intelligence|bi engineer|business intelligence|data architect|data platform|nlp|computer vision|llm|model|datawarehouse|data warehouse)\b|מפתח.ת AI|מפתח.ת DATAWAREHOUSE", re.I),
    "design":      re.compile(r"\b(designer|ux|ui\b|user experience|user interface|figma|product design|visual design|graphic|illustrat)\b|אדריכל|מעצב", re.I),
    "sales":       re.compile(r"\b(account executive|account manager|sales engineer|sales manager|business development|bd\b|sdr\b|bdr\b|pre.?sales|presales|revenue|partnership|channel|alliances|enterprise sales|deal)\b|רכז.ת קשרי תעשיה|פיתוח עסקי", re.I),
    "marketing":   re.compile(r"\b(marketing|growth|content|seo|sem|brand|demand generation|campaign|copywriter|social media|pr\b|public relations|communications|field marketing|product marketing)\b|שיווק|קמפיין", re.I),
    "support":     re.compile(r"\b(customer success|customer support|technical support|implementation|solutions engineer|solution consultant|integration engineer|onboarding|professional services|client success|support engineer)\b|יועץ.ת חינוכי", re.I),
    "operations":  re.compile(r"\b(hr\b|human resources|recruiter|talent acquisition|people ops|finance|accounting|legal|counsel|procurement|office manager|operations|ops\b|admin|executive assistant|ea\b|it manager|information technology|supply chain|logistics|biz ops|revenue ops|sales ops)\b|ראש לשכה|מנהל.ת לשכה|משאבי אנוש|רכז.ת|ביקורת פנים|בטיחות|ספרי|ספרנ|אחזקה|כלכלנ|חשבות|מנהח\"ש", re.I),
    "administrative": re.compile(r"רכז.ת מינהל|מנהל.ת מינהל|מנהל.ת משרד|מינהל.ת|סגל מנהלי|אדמיניסטרציה|administrative|coordinator|administrator", re.I),
}

def classify_segment(title: str) -> str:
    if not title:
        return ""
    if SEGMENTS["intern"].search(title):
        return "intern"
    for seg, pattern in SEGMENTS.items():
        if seg == "intern":
            continue
        if pattern.search(title):
            return seg
    return ""

LEVEL_DIRECTOR = re.compile(
    r"\b(director|vp\b|vice president|head of|chief|cto|ceo|coo|cpo|ciso|cmo|cfo|general manager|gm\b|president)\b"
    r"|ראש אגף|סמנכ\"ל|מנכ\"ל|ראש תחום|מנהל.ת אגף|מנהל.ת מערך|מנהל.ת מחלקה", re.I)
LEVEL_ADVANCED = re.compile(
    r"\b(staff|principal|distinguished|fellow|lead\b|architect|l[45]\b|level [45]|iv\b)\b", re.I)
LEVEL_SENIOR   = re.compile(
    r"\b(senior|sr\.?\b|experienced|l3\b|level 3|iii\b)\b|בכיר|בכירה|ראשי|ראשית|אחראי.ת", re.I)
LEVEL_MID      = re.compile(
    r"\b(mid[- ]?level|mid[- ]?senior|intermediate|l2\b|level 2|ii\b)\b|מומחה|מומחית|רכז.ת|מנהל.ת מעבדה|מיישמ|מהנדס.ת ראשי", re.I)
LEVEL_ENTRY    = re.compile(
    r"\b(junior|jr\.?\b|entry[- ]?level|associate|new grad|graduate|fresher|trainee|l1\b|level 1|i\b)\b|סטודנט|מתמחה|לבורנט", re.I)

def classify_level(title: str) -> str:
    if not title:
        return ""
    if LEVEL_DIRECTOR.search(title):  return "director"
    if LEVEL_ADVANCED.search(title):  return "advanced"
    if LEVEL_SENIOR.search(title):    return "senior"
    if LEVEL_MID.search(title):       return "mid"
    if LEVEL_ENTRY.search(title):     return "entry"
    return ""

def classify_worktype(wt: str) -> str:
    wt = (wt or "").lower()
    if "remote"  in wt or "מהבית" in wt: return "remote"
    if "hybrid"  in wt:                   return "hybrid"
    return "onsite"

# ── File discovery ────────────────────────────────────────────────────────────
EXCLUDE = {"history.csv", "leumit_jobs"}  # skip these

def find_job_csvs() -> list[str]:
    """Return all *_jobs*.csv files in the current directory, newest-first per source."""
    all_files = glob.glob("*_jobs*.csv") + glob.glob("*_jobs.csv")
    result = []
    for f in sorted(set(all_files), reverse=True):
        if any(ex in f for ex in EXCLUDE):
            continue
        result.append(f)
    return result

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    csv_files = find_job_csvs()
    if not csv_files:
        print("No job CSV files found. Exiting.")
        return

    print(f"Reading {len(csv_files)} CSV file(s)…")

    # Dedup across all files by URL (fallback: company|title)
    seen:   set[str]  = set()
    counts: dict      = defaultdict(int)
    total             = 0

    for fpath in csv_files:
        try:
            with open(fpath, encoding="utf-8-sig", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Dedup key
                    url  = (row.get("url") or "").strip()
                    key  = url or f"{row.get('company','')}|{row.get('title','')}"
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    total += 1

                    title    = row.get("title",        "")
                    wt_raw   = row.get("workplace_type") or row.get("workType") or row.get("work_type") or ""
                    source   = (row.get("source") or "").strip().lower()

                    seg  = classify_segment(title)
                    lvl  = classify_level(title)
                    wt   = classify_worktype(wt_raw)

                    if seg:  counts[seg]    += 1
                    if lvl:  counts[lvl]    += 1
                    counts[wt]              += 1
                    if source: counts[source] += 1

        except Exception as e:
            print(f"  [warn] could not read {fpath}: {e}")

    print(f"Total unique jobs: {total}")

    # Build the new row
    row_data = {"date": TODAY, "total": total}
    for col in COLUMNS:
        if col in ("date", "total"):
            continue
        row_data[col] = counts.get(col, 0)

    if DRY_RUN:
        print("\n── DRY RUN — row that would be appended ──")
        for k, v in row_data.items():
            print(f"  {k}: {v}")
        return

    # Check if history.csv exists and already has today's date
    file_exists = Path(HISTORY).exists()
    if file_exists:
        with open(HISTORY, encoding="utf-8-sig") as f:
            existing = list(csv.DictReader(f))
        if any(r.get("date") == TODAY for r in existing):
            print(f"history.csv already has a row for {TODAY}. Overwriting it.")
            existing = [r for r in existing if r.get("date") != TODAY]
        else:
            existing = existing

        # Make sure all new columns exist in old rows
        all_cols = set(COLUMNS)
        for r in existing:
            for col in all_cols:
                r.setdefault(col, 0)
    else:
        existing = []

    existing.append(row_data)
    # Sort chronologically
    existing.sort(key=lambda r: r.get("date", ""))

    with open(HISTORY, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing)

    print(f"✓ history.csv updated — {len(existing)} rows total, latest: {TODAY}")
    print(f"  Breakdown: rd={row_data['rd']} | senior={row_data['senior']} | junior={row_data['entry']} | remote={row_data['remote']} | hybrid={row_data['hybrid']}")

if __name__ == "__main__":
    main()
