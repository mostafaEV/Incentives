# CAPEX Incentive Finder

Live dashboard for US energy efficiency, tax, grant, financing, and demand response programs.

---

## Deploy in 5 minutes (free, no account needed)

### Option A — Netlify drag-and-drop (fastest)

1. Go to **https://app.netlify.com/drop**
2. Drag the `docs/` folder into the browser window
3. Done — you get a live URL like `https://cool-name-123.netlify.app`

To update data: run `python scraper/scraper.py`, then drag the `docs/` folder again.

---

### Option B — GitHub Pages + auto-refresh (recommended)

**First deploy:**

1. Create a new repo on github.com (click + → New repository)
2. Push this whole folder to it:
   ```bash
   git init
   git add .
   git commit -m "initial commit"
   git branch -M main
   git remote add origin https://github.com/YOURNAME/YOURREPO.git
   git push -u origin main
   ```
3. In GitHub: go to **Settings → Pages → Source → Deploy from branch → main → /docs**
4. Your dashboard is live at `https://YOURNAME.github.io/YOURREPO`

**Enable weekly auto-refresh:**

The `.github/workflows/weekly-scrape.yml` file is already included.
GitHub will automatically run the scraper every Monday at 6am UTC and push
updated data. Nothing else needed.

To trigger a manual run: go to **Actions → Weekly CAPEX incentive scrape → Run workflow**.

---

## Folder structure

```
capex_live/
├── docs/
│   ├── index.html              ← The dashboard (reads capex_incentives.json)
│   └── capex_incentives.json   ← Data file (replaced by scraper each week)
├── scraper/
│   ├── scraper.py              ← Main scraper (DSIRE, DOE, utilities, PACE, etc.)
│   ├── alert.py                ← Weekly email digest of new programs
│   └── requirements.txt        ← Python dependencies
└── .github/
    └── workflows/
        └── weekly-scrape.yml   ← GitHub Actions automation
```

---

## Run the scraper manually

```bash
cd scraper
pip install -r requirements.txt
playwright install chromium

# All sources
python scraper.py

# Specific sources only
python scraper.py --sources dsire federal
python scraper.py --sources states --states NY CA TX IL MA

# Then export to docs folder
cp capex_incentives.json ../docs/
```

---

## QA workflow

Programs scraped from the web are marked `qa_status: "unreviewed"` in the dashboard.
After reviewing a program, update its status in the database:

```python
import sqlite3
conn = sqlite3.connect("scraper/incentives.db")
conn.execute("UPDATE programs SET qa_status='reviewed' WHERE name LIKE '%NYSERDA%'")
conn.commit()
```

Only `reviewed` programs show the green "verified" badge in the dashboard.

---

## Email alerts for new programs

```bash
# Set your email credentials
export SMTP_USER=you@gmail.com
export SMTP_PASS=your-app-password

# Send digest of programs added/changed in last 7 days
python scraper/alert.py --email you@company.com

# Or print without sending
python scraper/alert.py --print-only
```
