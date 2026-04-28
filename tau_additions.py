# ══ TAU (אוניברסיטת תל אביב) ══════════════════════════════════════════════════
# Add this function to fetch_jobs.py alongside the other academic scrapers.
# Then call run_tau() inside main(), and add tau_jobs_*.csv to the git commit step.

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
        """Parse DD/MM/YYYY → YYYY-MM-DD"""
        m = _re.search(r'(\d{1,2})[/.](\d{1,2})[/.](\d{4})', s)
        if m:
            return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        return ""

    def fetch_description(url):
        """Fetch job detail page and extract description + requirements."""
        soup = fetch_page(url)
        if not soup:
            return "", ""
        desc, reqs = "", ""
        # TAU detail pages use labeled paragraphs: "תיאור התפקיד:" and "דרישות התפקיד:"
        full_text = soup.get_text("\n", strip=True)
        # Extract description block
        for marker in ["תיאור התפקיד:", "תיאור המשרה:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                # Stop at next section
                for stop in ["דרישות התפקיד:", "דרישות המשרה:", "כפיפות:", "היקף משרה:", "מעמד משרה:"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                desc = after.strip()
                break
        # Extract requirements block
        for marker in ["דרישות התפקיד:", "דרישות המשרה:"]:
            if marker in full_text:
                after = full_text.split(marker, 1)[1]
                for stop in ["כפיפות:", "היקף משרה:", "מעמד משרה:", "המשרה מיועדת", "הגשת מועמדות"]:
                    if stop in after:
                        after = after.split(stop, 1)[0]
                reqs = after.strip()
                break
        return desc, reqs

    # ── 1. Scrape listing page ────────────────────────────────────────────────
    soup = fetch_page(LISTING_URL)
    if not soup:
        print("  x could not fetch TAU positions page")
        return

    jobs = []
    seen = set()

    # Both tabs (admin + academic) are rendered in the same HTML as two <table> elements
    tables = soup.select("table")
    tab_labels = ["administrative", "academic_staff"]  # first table = admin, second = academic

    for tab_idx, table in enumerate(tables[:2]):
        staff_type = tab_labels[tab_idx] if tab_idx < len(tab_labels) else "administrative"
        for row in table.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            # Col 0: job title + link, Col 1: unit, Col 2: internal/external, Col 3: deadline
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
            internal_external = cells[2].get_text(strip=True)  # פנימי / חיצוני
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
                "staff_type": staff_type,          # administrative / academic_staff
                "internal_external": internal_external,
                "description": "",
                "requirements": "",
            })

    print(f"  Found {len(jobs)} listings — fetching descriptions...")

    # ── 2. Enrich with descriptions from detail pages ─────────────────────────
    for i, job in enumerate(jobs, 1):
        desc, reqs = fetch_description(job["url"])
        job["description"] = desc
        job["requirements"] = reqs
        print(f"  [{i}/{len(jobs)}] {job['title'][:60]}")
        time.sleep(0.4)   # be polite — TAU is a Drupal site

    print(f"  + {len(jobs)}")
    write_csv(
        jobs,
        ["title", "company", "location", "date", "deadline", "url",
         "department", "workplace_type", "staff_type", "internal_external",
         "description", "requirements"],
        f"tau_jobs_{TODAY}.csv"
    )


# ── Changes needed in main() ─────────────────────────────────────────────────
# Add this line inside main() after run_technion():
#   run_tau()
#
# ── Changes needed in fetch_jobs.yml ─────────────────────────────────────────
# In the "git add" line, append:   tau_jobs_*.csv
# Full updated line:
#   git add comeet_jobs_*.csv greenhouse_jobs_*.csv bgu_jobs_*.csv weizmann_jobs_*.csv technion_jobs_*.csv huji_jobs_*.csv mitam_jobs_*.csv tau_jobs_*.csv
