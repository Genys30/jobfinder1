// ═══════════════════════════════════════════════════════════════════════
// GOTFRIENDS — index.html integration patch
// Add the loadGotFriends() call alongside your other load* functions.
// The popup reuses the SAME modal already used by KPMG/Deloitte/etc.
// ═══════════════════════════════════════════════════════════════════════

// ── 1. LOAD FUNCTION ──────────────────────────────────────────────────
// Add this function near your other loadKPMG / loadDeloitte functions:

async function loadGotFriends() {
  // Find all gotfriends_jobs_*.csv files committed to the repo
  // (same pattern used for bgu_extra_jobs_*.csv)
  const index = await fetch('gotfriends_file_index.json').then(r => r.json()).catch(() => null);
  if (!index || !index.files) return;

  for (const file of index.files) {
    await loadCSV(file, r => ({
      title:        r.title,
      company:      r.company,    // "אנונימי / סודי"
      location:     r.location,
      date:         r.date,
      url:          r.url,
      source:       'gotfriends',
      job_id:       r.job_id,
      description:  r.description  || '',
      requirements: r.requirements || '',
    }));
  }
}

// ── 2. BADGE & ROW STYLING ────────────────────────────────────────────
// In your renderRow() / badge logic, add gotfriends alongside kpmg/deloitte:

// const gotfriendsBadge = r.source === 'gotfriends'
//   ? '<span class="gf-badge">GotFriends</span>' : '';

// In rowCls map add:   gotfriends: ' gf-row'

// In your CSS (or <style> block):
/*
.gf-badge {
  background: #00b377;
  color: #fff;
  font-size: 10px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 4px;
  vertical-align: middle;
  margin-left: 4px;
}
.gf-row { border-left: 3px solid #00b377; }
*/

// ── 3. POPUP (description on row click) ──────────────────────────────
// Your KPMG popup handler already checks for r.description. 
// GotFriends jobs also have r.requirements — show both in the modal:

// Inside your existing showJobModal() (or equivalent), add requirements
// support if not already there. Replace or extend the modal body render:

function buildModalBody(job) {
  // job = the row object from your allJobs array
  const desc = (job.description || '').trim();
  const req  = (job.requirements || '').trim();

  if (!desc && !req) {
    // No local description — just open the URL
    window.open(job.url, '_blank');
    return null;   // signal: don't open modal
  }

  let html = '';

  if (desc) {
    html += `<div class="modal-section">
      <h4>תיאור המשרה</h4>
      <p>${desc.replace(/\n/g, '<br>')}</p>
    </div>`;
  }

  if (req) {
    html += `<div class="modal-section">
      <h4>דרישות המשרה</h4>
      <p>${req.replace(/\n/g, '<br>')}</p>
    </div>`;
  }

  html += `<div class="modal-footer">
    <a href="${job.url}" target="_blank" rel="noopener" class="modal-apply-btn">
      למשרה המלאה ←
    </a>
  </div>`;

  return html;
}

// ── 4. FILE INDEX APPROACH (alternative: glob pattern) ───────────────
// Option A — generate gotfriends_file_index.json in fetch_gotfriends.py:
//   (add at end of write_csv):
//     import json, glob
//     files = sorted(glob.glob("gotfriends_jobs_*.csv"))
//     Path("gotfriends_file_index.json").write_text(json.dumps({"files": files}))
//
// Option B — use the same glob trick you use for bgu_extra_jobs_*.csv
//   (if your fetch_jobs.py already writes a master manifest, add gotfriends there)

// ── 5. SOURCE FILTER DROPDOWN ─────────────────────────────────────────
// Add to your <select id="source-filter"> options:
//   <option value="gotfriends">GotFriends</option>

// ── 6. DATABAR COUNT ──────────────────────────────────────────────────
// If you display per-source counts in your databar:
//   const gfCount = allJobs.filter(j => j.source === 'gotfriends').length;
//   document.getElementById('gf-count').textContent = gfCount;
