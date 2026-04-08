"""
CAPEX Incentive Scraper — USA
Covers: Energy Efficiency, Tax Incentives, Grants, Low-Interest Financing,
        Demand Response, Water/Wastewater, CHP, Storage, EV Charging,
        USDA REAP, PACE, SBA, State Green Banks, Manufacturing/MEP

Sources: DSIRE API, DOE, EPA, USDA, SBA, utility sites, state energy offices,
         state green banks, PACE registries

Usage:
    pip install requests beautifulsoup4 playwright pandas
    playwright install chromium
    python scraper.py [--sources all|energy|tax|grants|financing|demand|water|capex]
                      [--states NY CA TX ...]
"""

import requests
import json
import sqlite3
import hashlib
import datetime
import time
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("[warn] playwright not installed — JS-rendered pages will be skipped")

DB_PATH = Path("incentives.db")

# ─── Equipment / Technology Keywords ─────────────────────────────────────────

EQUIPMENT_KEYWORDS = {
    # Energy efficiency
    "VFD":          ["variable frequency drive","variable speed drive","vfd","vsd","adjustable speed drive","motor drive","asd","variable speed pump"],
    "Chiller":      ["chiller","water-cooled chiller","air-cooled chiller","centrifugal chiller","magnetic bearing chiller","screw chiller"],
    "Pump":         ["pump","high efficiency pump","pump rebate","pump motor","circulator pump","booster pump"],
    "Boiler":       ["boiler","condensing boiler","high efficiency boiler","steam boiler","hot water boiler"],
    "HVAC":         ["hvac","heat pump","rooftop unit","rtu","air handler","cooling tower","packaged unit","split system","chiller plant"],
    "Lighting":     ["led","lighting","lamp","luminaire","lighting retrofit","lighting controls","daylight sensor"],
    "Compressed Air":["compressed air","air compressor","compressor efficiency","pneumatic"],
    "Motors":       ["electric motor","premium efficiency motor","nema premium","motor efficiency","ie3","ie4"],
    # Generation & storage
    "CHP":          ["combined heat and power","chp","cogeneration","micro-chp","prime mover","waste heat recovery"],
    "Battery Storage":["battery storage","bess","energy storage","lithium ion storage","behind-the-meter storage"],
    "Thermal Storage":["thermal energy storage","ice storage","chilled water storage","thermal storage tank"],
    "Solar":        ["solar","photovoltaic","pv system","solar pv","rooftop solar"],
    # Transport & infrastructure
    "EV Charging":  ["ev charging","electric vehicle charging","evse","level 2 charger","dcfc","fleet electrification"],
    # Industrial process
    "Refrigeration":["refrigeration","commercial refrigeration","industrial refrigeration","cold storage","refrigerant"],
    "Heat Exchanger":["heat exchanger","heat recovery","economizer","plate heat exchanger"],
    "Steam System": ["steam trap","steam system","steam leak","flash steam","condensate return"],
    # Water
    "Water Efficiency":["water efficiency","water conservation","low-flow","waterwise","water reuse","grey water"],
    "Wastewater":   ["wastewater","sewage treatment","water treatment","biogas","anaerobic digestion"],
    # Envelope & controls
    "Building Controls":["building automation","bas","bms","energy management system","ems","smart controls","demand control"],
    "Insulation":   ["insulation","building envelope","air sealing","weatherization","cool roof"],
}

CAPEX_CATEGORIES = [
    "Energy Efficiency", "Tax Incentive", "Grant", "Loan / Financing",
    "Demand Response", "Water & Wastewater", "Manufacturing / Industrial",
    "Renewable Energy", "EV Infrastructure", "Technical Assistance"
]

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS programs (
            id TEXT PRIMARY KEY,
            name TEXT,
            org TEXT,
            program_type TEXT,
            capex_category TEXT,
            state TEXT,
            equipment TEXT,
            incentive_amount TEXT,
            incentive_type TEXT,
            max_incentive TEXT,
            deadline TEXT,
            eligible_sectors TEXT,
            stacking_allowed TEXT,
            source_url TEXT,
            notes TEXT,
            raw_text TEXT,
            source TEXT,
            qa_status TEXT DEFAULT 'unreviewed',
            first_seen TEXT,
            last_updated TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            scraped_at TEXT,
            programs_found INTEGER,
            status TEXT,
            error TEXT
        )
    """)
    conn.commit()
    return conn


def make_id(name: str, org: str) -> str:
    return hashlib.md5((name + org).encode()).hexdigest()


def upsert_program(conn, program: dict):
    key = make_id(program.get("name",""), program.get("org",""))
    now = datetime.datetime.utcnow().isoformat()
    existing = conn.execute("SELECT id FROM programs WHERE id=?", (key,)).fetchone()
    program["id"] = key
    program["last_updated"] = now
    if not existing:
        program["first_seen"] = now
        cols = ", ".join(program.keys())
        ph   = ", ".join("?" for _ in program)
        conn.execute(f"INSERT INTO programs ({cols}) VALUES ({ph})", list(program.values()))
    else:
        sets = ", ".join(f"{k}=?" for k in program if k != "id")
        vals = [v for k,v in program.items() if k != "id"] + [key]
        conn.execute(f"UPDATE programs SET {sets} WHERE id=?", vals)
    conn.commit()


# ─── Text Utilities ───────────────────────────────────────────────────────────

def tag_equipment(text: str) -> list:
    tl = text.lower()
    return [eq for eq, kws in EQUIPMENT_KEYWORDS.items() if any(kw in tl for kw in kws)]


def extract_amount(text: str) -> str:
    patterns = [
        r'up\s+to\s+\$[\d,]+(?:\.\d+)?(?:\s*/\s*(?:HP|ton|kWh|sq\s*ft|unit|lamp|fixture|MMBtu|acre))?',
        r'\$[\d,]+(?:\.\d+)?\s*(?:to|-)\s*\$[\d,]+(?:\.\d+)?(?:\s*/\s*\w+)?',
        r'\$[\d,]+(?:\.\d+)?(?:\s*/\s*(?:HP|ton|kWh|sq\s*ft|unit|lamp|fixture|MMBtu|acre))?',
        r'[\d]+(?:\.\d+)?%\s+(?:rebate|incentive|of\s+(?:project\s+)?cost|grant|tax\s+credit)',
        r'[\d]+(?:\.\d+)?\s*cents?\s*/\s*kWh',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()[:80]
    return ""


def scrape_static(url: str) -> str:
    resp = requests.get(url, timeout=30, headers={
        "User-Agent": "Mozilla/5.0 (compatible; CAPEXIncentiveScraper/2.0)"
    })
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script","style","nav","footer","header","aside"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def scrape_js(url: str) -> str:
    if not PLAYWRIGHT_AVAILABLE:
        return ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=45000, wait_until="networkidle")
        content = page.inner_text("body")
        browser.close()
        return content


def chunk_text(text: str, window: int = 500, step: int = 300) -> list:
    words = text.split()
    return [" ".join(words[i:i+window]) for i in range(0, len(words), step) if len(" ".join(words[i:i+window])) > 60]


def scrape_source(src: dict) -> str:
    try:
        if src.get("js_required"):
            return scrape_js(src["url"])
        return scrape_static(src["url"])
    except Exception as e:
        print(f"    [fetch error] {e}")
        return ""


def save_source_programs(conn, src: dict, text: str, capex_category: str, default_equip=None):
    chunks = chunk_text(text)
    saved = 0
    seen_amts = set()
    for chunk in chunks:
        equip = tag_equipment(chunk) or (default_equip or [])
        if not equip:
            continue
        amt = extract_amount(chunk)
        if not amt or amt in seen_amts:
            continue
        seen_amts.add(amt)
        program = {
            "name": f"{src['name']} — {', '.join(equip[:3])}",
            "org": src["org"],
            "program_type": src.get("type","State"),
            "capex_category": capex_category,
            "state": src.get("state","All"),
            "equipment": json.dumps(equip),
            "incentive_amount": amt,
            "incentive_type": src.get("incentive_type","Rebate"),
            "max_incentive": src.get("max",""),
            "deadline": src.get("deadline","Ongoing"),
            "eligible_sectors": src.get("sectors","Commercial, Industrial"),
            "stacking_allowed": "Yes",
            "source_url": src["url"],
            "notes": chunk[:400],
            "raw_text": chunk[:800],
            "source": src.get("source_tag", src["name"]),
        }
        upsert_program(conn, program)
        saved += 1
    return saved


# ─── 1. DSIRE API ─────────────────────────────────────────────────────────────

def fetch_dsire(conn, equipment_filter=None):
    print("\n[DSIRE] Fetching programs from dsireusa.org API...")
    all_items, page = [], 1
    while True:
        try:
            resp = requests.get(
                "https://programs.dsireusa.org/api/v1/programs",
                params={"page": page, "limit": 100, "active": "true"},
                timeout=30, headers={"Accept": "application/json"}
            )
            resp.raise_for_status()
            data  = resp.json()
            items = data.get("data", [])
            if not items: break
            all_items.extend(items)
            total = data.get("meta", {}).get("total", 0)
            print(f"  Page {page}: {len(items)} items (total {total})")
            if len(all_items) >= total: break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  [DSIRE error] {e}")
            break

    saved = 0
    for p in all_items:
        name = p.get("name","")
        desc = (p.get("summary","") or "") + " " + (p.get("websiteUrl","") or "")
        equip = tag_equipment(name + " " + desc)
        if equipment_filter and not any(e in equip for e in equipment_filter):
            continue
        ptype = p.get("programType",{})
        ptype_name = ptype.get("name","") if isinstance(ptype,dict) else ""
        capex_cat = "Tax Incentive" if "tax" in ptype_name.lower() else \
                    "Loan / Financing" if "loan" in ptype_name.lower() else \
                    "Grant" if "grant" in ptype_name.lower() else "Energy Efficiency"
        program = {
            "name": name,
            "org": (p.get("administrator",{}) or {}).get("name","") if isinstance(p.get("administrator"),dict) else "",
            "program_type": ptype_name or "Federal",
            "capex_category": capex_cat,
            "state": (p.get("state",{}) or {}).get("abbreviation","All") if isinstance(p.get("state"),dict) else "All",
            "equipment": json.dumps(equip),
            "incentive_amount": extract_amount(desc),
            "incentive_type": ptype_name,
            "max_incentive": "",
            "deadline": p.get("endDate","Ongoing") or "Ongoing",
            "eligible_sectors": "",
            "stacking_allowed": "",
            "source_url": p.get("websiteUrl","https://programs.dsireusa.org"),
            "notes": (p.get("summary","") or "")[:500],
            "raw_text": json.dumps(p)[:1500],
            "source": "DSIRE",
        }
        upsert_program(conn, program)
        saved += 1

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("DSIRE", datetime.datetime.utcnow().isoformat(), saved, "ok", None))
    conn.commit()
    print(f"  Saved {saved} DSIRE programs.")
    return saved


# ─── 2. Federal DOE / EPA / USDA Sources ─────────────────────────────────────

FEDERAL_SOURCES = [
    # Energy efficiency
    {"name":"ENERGY STAR Rebate Finder","org":"EPA/DOE","type":"Federal","capex_category":"Energy Efficiency",
     "url":"https://www.energystar.gov/rebate-finder","js_required":True,"incentive_type":"Rebate",
     "sectors":"Commercial","deadline":"Ongoing","max":"Varies"},
    {"name":"DOE EERE Industrial Efficiency","org":"US DOE","type":"Federal","capex_category":"Technical Assistance",
     "url":"https://www.energy.gov/eere/industry/industrial-efficiency-and-decarbonization","js_required":False,
     "incentive_type":"Technical Assistance","sectors":"Industrial","deadline":"Ongoing","max":"Free"},
    {"name":"DOE Better Buildings Financing Navigator","org":"US DOE","type":"Federal","capex_category":"Loan / Financing",
     "url":"https://betterbuildingssolutioncenter.energy.gov/financing-navigator","js_required":True,
     "incentive_type":"Financing Navigator","sectors":"Commercial, Industrial","deadline":"Ongoing","max":"Varies"},
    {"name":"DOE Advanced Manufacturing Office Assessments","org":"US DOE","type":"Federal","capex_category":"Technical Assistance",
     "url":"https://www.energy.gov/eere/amo/save-energy-now-assessments","js_required":False,
     "incentive_type":"Free Assessment","sectors":"Manufacturing","deadline":"Ongoing","max":"Free"},
    # Tax
    {"name":"IRA Section 48 Investment Tax Credit","org":"IRS/DOE","type":"Federal","capex_category":"Tax Incentive",
     "url":"https://www.energy.gov/policy/inflation-reduction-act","js_required":False,
     "incentive_type":"Tax Credit","sectors":"Commercial, Industrial","deadline":"2032","max":"No cap"},
    {"name":"Section 179D Commercial Buildings Deduction","org":"IRS","type":"Federal","capex_category":"Tax Incentive",
     "url":"https://www.irs.gov/businesses/small-businesses-self-employed/section-179d-commercial-buildings-energy-efficiency-tax-deduction",
     "js_required":False,"incentive_type":"Tax Deduction","sectors":"Commercial","deadline":"Ongoing","max":"Varies by sq ft"},
    {"name":"Bonus Depreciation (MACRS) for Energy Equipment","org":"IRS","type":"Federal","capex_category":"Tax Incentive",
     "url":"https://www.irs.gov/businesses/small-businesses-self-employed/a-brief-overview-of-depreciation",
     "js_required":False,"incentive_type":"Accelerated Depreciation","sectors":"Commercial, Industrial","deadline":"2027","max":"No cap"},
    {"name":"48C Advanced Energy Project Tax Credit","org":"IRS/DOE","type":"Federal","capex_category":"Tax Incentive",
     "url":"https://www.energy.gov/lpo/48c","js_required":False,
     "incentive_type":"Tax Credit","sectors":"Industrial, Manufacturing","deadline":"2032","max":"No cap"},
    # Grants
    {"name":"USDA Rural Energy for America (REAP)","org":"USDA","type":"Federal","capex_category":"Grant",
     "url":"https://www.rd.usda.gov/programs-services/energy-programs/rural-energy-america-program-renewable-energy-systems-energy-efficiency-improvement-guaranteed-loans-grants",
     "js_required":False,"incentive_type":"Grant + Loan","sectors":"Rural Commercial, Agricultural","deadline":"Rolling","max":"$1,000,000"},
    {"name":"USDA Business & Industry Loan Guarantee","org":"USDA Rural Dev","type":"Federal","capex_category":"Loan / Financing",
     "url":"https://www.rd.usda.gov/programs-services/business-programs/business-industry-loan-guarantees",
     "js_required":False,"incentive_type":"Loan Guarantee","sectors":"Rural Commercial, Industrial","deadline":"Ongoing","max":"$25,000,000"},
    {"name":"EDA Manufacturing Modernization Grants","org":"US EDA","type":"Federal","capex_category":"Grant",
     "url":"https://www.eda.gov/funding/programs","js_required":False,
     "incentive_type":"Grant","sectors":"Manufacturing","deadline":"Rolling RFPs","max":"Varies"},
    {"name":"EPA Clean Air Act Section 138 Grants","org":"US EPA","type":"Federal","capex_category":"Grant",
     "url":"https://www.epa.gov/inflation-reduction-act/inflation-reduction-act-resources-states-tribes-and-communities",
     "js_required":False,"incentive_type":"Grant","sectors":"Industrial, Combustion","deadline":"Rolling","max":"Varies"},
    # Financing
    {"name":"SBA 504 Loan Program (Green)","org":"SBA","type":"Federal","capex_category":"Loan / Financing",
     "url":"https://www.sba.gov/funding-programs/loans/504-loans","js_required":False,
     "incentive_type":"Below-market loan","sectors":"Small Business","deadline":"Ongoing","max":"$5,500,000"},
    {"name":"SBA 7(a) Equipment Financing","org":"SBA","type":"Federal","capex_category":"Loan / Financing",
     "url":"https://www.sba.gov/funding-programs/loans/7a-loans","js_required":False,
     "incentive_type":"Loan Guarantee","sectors":"Small Business","deadline":"Ongoing","max":"$5,000,000"},
    # Water
    {"name":"EPA Clean Water State Revolving Fund","org":"US EPA","type":"Federal","capex_category":"Water & Wastewater",
     "url":"https://www.epa.gov/cwsrf","js_required":False,
     "incentive_type":"Low-interest loan","sectors":"Municipal, Industrial","deadline":"Ongoing","max":"No cap"},
    {"name":"EPA WaterSense Commercial Rebates","org":"US EPA","type":"Federal","capex_category":"Water & Wastewater",
     "url":"https://www.epa.gov/watersense/watersense-products","js_required":False,
     "incentive_type":"Rebate","sectors":"Commercial","deadline":"Ongoing","max":"Varies"},
    {"name":"USDA Water & Environmental Programs","org":"USDA Rural Dev","type":"Federal","capex_category":"Water & Wastewater",
     "url":"https://www.rd.usda.gov/programs-services/water-environmental-programs","js_required":False,
     "incentive_type":"Grant + Loan","sectors":"Rural, Municipal","deadline":"Rolling","max":"Varies"},
    # CHP / Storage
    {"name":"DOE CHP Technical Assistance Partnerships","org":"US DOE","type":"Federal","capex_category":"Technical Assistance",
     "url":"https://www.energy.gov/eere/amo/combined-heat-and-power-technical-assistance-partnerships-chp-taps",
     "js_required":False,"incentive_type":"Technical Assistance","sectors":"Industrial, Commercial","deadline":"Ongoing","max":"Free"},
    {"name":"IRA Battery Storage Tax Credit (48)","org":"IRS/DOE","type":"Federal","capex_category":"Tax Incentive",
     "url":"https://www.energy.gov/eere/solar/homeowners-guide-federal-tax-credit-solar-photovoltaics",
     "js_required":False,"incentive_type":"Tax Credit","sectors":"Commercial, Industrial","deadline":"2032","max":"No cap"},
    # MEP / Manufacturing
    {"name":"NIST MEP Manufacturing Extension Partnership","org":"NIST","type":"Federal","capex_category":"Manufacturing / Industrial",
     "url":"https://www.nist.gov/mep","js_required":False,
     "incentive_type":"Technical Assistance + Cost Share","sectors":"Manufacturing","deadline":"Ongoing","max":"50% cost share"},
]


def scrape_federal_sources(conn):
    print("\n[Federal] Scraping DOE / EPA / USDA / SBA sources...")
    for src in FEDERAL_SOURCES:
        print(f"  [{src['name']}]")
        text = scrape_source(src)
        equip = tag_equipment(text) if text else list(EQUIPMENT_KEYWORDS.keys())[:8]
        amt = extract_amount(text) if text else "See program"
        program = {
            "name": src["name"],
            "org": src["org"],
            "program_type": src["type"],
            "capex_category": src["capex_category"],
            "state": "All",
            "equipment": json.dumps(equip or ["VFD","Chiller","Pump","Boiler","HVAC"]),
            "incentive_amount": amt or "See program details",
            "incentive_type": src.get("incentive_type",""),
            "max_incentive": src.get("max",""),
            "deadline": src.get("deadline","Ongoing"),
            "eligible_sectors": src.get("sectors",""),
            "stacking_allowed": "Yes",
            "source_url": src["url"],
            "notes": f"{src.get('incentive_type','')} · {src.get('sectors','')} · max: {src.get('max','')}",
            "raw_text": text[:800] if text else "",
            "source": "Federal",
        }
        upsert_program(conn, program)
        time.sleep(0.5)

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("Federal", datetime.datetime.utcnow().isoformat(), len(FEDERAL_SOURCES), "ok", None))
    conn.commit()
    print(f"  Saved {len(FEDERAL_SOURCES)} federal programs.")


# ─── 3. PACE Financing Registries ────────────────────────────────────────────

PACE_SOURCES = [
    {"name":"Ygrene C-PACE Financing","org":"Ygrene Energy Fund","type":"Financing","state":"CA",
     "url":"https://ygrene.com/commercial","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"Nuveen C-PACE","org":"Nuveen Green Capital","type":"Financing","state":"All",
     "url":"https://www.nuveengc.com","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"Petros PACE Finance","org":"Petros PACE","type":"Financing","state":"All",
     "url":"https://www.petrospaceUSA.com","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"PACE Nation State Programs Directory","org":"PACE Nation","type":"Financing","state":"All",
     "url":"https://www.pacenation.org/pace-programs/","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"NY C-PACE (NYCEEC)","org":"NY Green Bank / NYCEEC","type":"Financing","state":"NY",
     "url":"https://nyceec.com/c-pace/","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"CT C-PACE Program","org":"CT Green Bank","type":"Financing","state":"CT",
     "url":"https://ctgreenbank.com/c-pace/","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"CA PACE Lending (CAEATFA)","org":"CA CAEATFA","type":"Financing","state":"CA",
     "url":"https://treasurer.ca.gov/caeatfa/cpace/","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"TX C-PACE Program","org":"TX PACE Authority","type":"Financing","state":"TX",
     "url":"https://txpace.com","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"FL C-PACE Program","org":"Florida PACE Funding Agency","type":"Financing","state":"FL",
     "url":"https://www.flpace.com","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
    {"name":"CO C-PACE Program","org":"Colorado PACE","type":"Financing","state":"CO",
     "url":"https://copace.com","capex_category":"Loan / Financing","incentive_type":"PACE Financing"},
]


def scrape_pace_sources(conn):
    print("\n[PACE] Scraping C-PACE financing programs...")
    saved = 0
    for src in PACE_SOURCES:
        print(f"  [{src['name']}]")
        text = scrape_source(src)
        program = {
            "name": src["name"],
            "org": src["org"],
            "program_type": "Financing",
            "capex_category": "Loan / Financing",
            "state": src.get("state","All"),
            "equipment": json.dumps(["VFD","Chiller","Pump","Boiler","HVAC","Lighting","CHP","Solar","Battery Storage"]),
            "incentive_amount": "100% project financing",
            "incentive_type": "C-PACE (Property Assessed Clean Energy)",
            "max_incentive": "No cap (typically up to 25% of property value)",
            "deadline": "Ongoing",
            "eligible_sectors": "Commercial, Industrial, Multifamily",
            "stacking_allowed": "Yes — stacks with grants and tax credits",
            "source_url": src["url"],
            "notes": "C-PACE finances energy efficiency, renewable energy, and water upgrades through property tax assessment. No upfront cost. 5–30 year terms. Transfers with property sale.",
            "raw_text": text[:600] if text else "",
            "source": "PACE",
        }
        upsert_program(conn, program)
        saved += 1
        time.sleep(0.5)

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("PACE", datetime.datetime.utcnow().isoformat(), saved, "ok", None))
    conn.commit()
    print(f"  Saved {saved} PACE programs.")


# ─── 4. State Green Banks ─────────────────────────────────────────────────────

GREEN_BANKS = [
    {"name":"NY Green Bank Commercial Programs","org":"NY Green Bank","state":"NY",
     "url":"https://greenbank.ny.gov/Products/Commercial-and-Industrial","incentive_type":"Low-interest loan / credit enhancement"},
    {"name":"CT Green Bank C&I Programs","org":"CT Green Bank","state":"CT",
     "url":"https://ctgreenbank.com/for-business/","incentive_type":"Loan / PACE"},
    {"name":"CA Infrastructure Bank (IBank) CLEEN","org":"CA IBank","state":"CA",
     "url":"https://ibank.ca.gov/programs/cleen/","incentive_type":"Low-interest loan"},
    {"name":"MD Clean Energy Capital Program","org":"MD Clean Energy Center","state":"MD",
     "url":"https://www.mdcleanenergy.org/business","incentive_type":"Loan"},
    {"name":"HI Green Infrastructure Authority","org":"HI GIA","state":"HI",
     "url":"https://hawaiicleanenergyinitiative.org","incentive_type":"Loan"},
    {"name":"NJ Green Bank (NJEDA)","org":"NJEDA","state":"NJ",
     "url":"https://www.njeda.com/clean-energy/","incentive_type":"Loan / Grant"},
    {"name":"DC Green Finance Authority","org":"DC Green Finance","state":"DC",
     "url":"https://dcgreenfinance.org","incentive_type":"PACE + Loan"},
    {"name":"RI Infrastructure Bank PACE","org":"RI Infrastructure Bank","state":"RI",
     "url":"https://www.riib.org/programs/commercial-pace","incentive_type":"PACE Financing"},
    {"name":"MA Clean Energy Center Financing","org":"MassCEC","state":"MA",
     "url":"https://www.masscec.com/programs/clean-energy-results-program","incentive_type":"Loan + Grant"},
    {"name":"CO Clean Energy Fund","org":"CO OEDIT","state":"CO",
     "url":"https://oedit.colorado.gov/colorado-clean-energy-fund","incentive_type":"Loan"},
]


def scrape_green_banks(conn):
    print("\n[Green Banks] Scraping state green bank programs...")
    saved = 0
    for src in GREEN_BANKS:
        print(f"  [{src['state']}] {src['name']}")
        text = scrape_source(src)
        program = {
            "name": src["name"],
            "org": src["org"],
            "program_type": "State",
            "capex_category": "Loan / Financing",
            "state": src["state"],
            "equipment": json.dumps(["VFD","Chiller","Pump","Boiler","HVAC","Lighting","CHP","Solar","Battery Storage","EV Charging"]),
            "incentive_amount": "Below-market financing",
            "incentive_type": src["incentive_type"],
            "max_incentive": "Varies by project",
            "deadline": "Ongoing",
            "eligible_sectors": "Commercial, Industrial, Multifamily",
            "stacking_allowed": "Yes",
            "source_url": src["url"],
            "notes": f"{src['incentive_type']} for clean energy and efficiency projects. Green banks offer favorable terms not available through conventional lenders.",
            "raw_text": text[:600] if text else "",
            "source": "GreenBank",
        }
        upsert_program(conn, program)
        saved += 1
        time.sleep(0.5)

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("GreenBank", datetime.datetime.utcnow().isoformat(), saved, "ok", None))
    conn.commit()
    print(f"  Saved {saved} green bank programs.")


# ─── 5. Utility Sources (expanded) ────────────────────────────────────────────

UTILITY_SOURCES = [
    {"name":"MassSave C&I","org":"Eversource/National Grid","state":"MA","url":"https://www.masssave.com/business/rebates-and-incentives","js_required":False},
    {"name":"ComEd Energy Efficiency","org":"ComEd","state":"IL","url":"https://www.comed.com/WaysToSave/ForYourBusiness/Pages/RebatesIncentives.aspx","js_required":True},
    {"name":"PG&E Business Efficiency","org":"PG&E","state":"CA","url":"https://www.pge.com/en_US/business/save-energy-money/energy-efficiency-rebates-and-incentives/energy-efficiency-rebates-and-incentives.page","js_required":True},
    {"name":"SCE Business Rebates","org":"Southern California Edison","state":"CA","url":"https://www.sce.com/business/rebates","js_required":True},
    {"name":"Focus on Energy Business","org":"Focus on Energy","state":"WI","url":"https://focusonenergy.com/business/equipment-rebates","js_required":False},
    {"name":"Duke Energy Efficiency","org":"Duke Energy","state":"NC","url":"https://www.duke-energy.com/business/products/energy-efficiency","js_required":False},
    {"name":"PECO Smart Ideas","org":"PECO Energy","state":"PA","url":"https://www.peco.com/WaysToSave/ForYourBusiness","js_required":False},
    {"name":"NV Energy EfficiencyPlus","org":"NV Energy","state":"NV","url":"https://www.nvenergy.com/business/energyefficiency","js_required":False},
    {"name":"Xcel Energy Business Rebates","org":"Xcel Energy","state":"CO","url":"https://www.xcelenergy.com/programs_and_rebates/business_programs_and_rebates","js_required":False},
    {"name":"DTE Energy Business Programs","org":"DTE Energy","state":"MI","url":"https://newlook.dteenergy.com/wps/wcm/connect/dte-web/home/save-energy/business","js_required":True},
    {"name":"Consumers Energy Business Efficiency","org":"Consumers Energy","state":"MI","url":"https://www.consumersenergy.com/business/save-energy-and-money","js_required":False},
    {"name":"Eversource CT Business Programs","org":"Eversource CT","state":"CT","url":"https://www.eversource.com/content/ct-c/business/save-money-energy/rebates-incentives","js_required":False},
    {"name":"National Grid Business Solutions","org":"National Grid","state":"NY","url":"https://www.nationalgridus.com/ny-business/energy-saving-programs","js_required":False},
    {"name":"PSE&G Business Efficiency","org":"PSE&G","state":"NJ","url":"https://pseg.com/home/business/saveenergy/index.jsp","js_required":True},
    {"name":"Puget Sound Energy Business Efficiency","org":"Puget Sound Energy","state":"WA","url":"https://pse.com/en/business-rebates","js_required":False},
    {"name":"Portland General Electric Business","org":"PGE Oregon","state":"OR","url":"https://portlandgeneral.com/business/save-energy-money/rebates-and-programs","js_required":False},
    {"name":"Ameren Illinois Efficiency Program","org":"Ameren Illinois","state":"IL","url":"https://www.amerenillinois.com/home/save-energy/business","js_required":False},
    {"name":"APS Business Energy Efficiency","org":"Arizona Public Service","state":"AZ","url":"https://www.aps.com/en/Residential/Save-Money-and-Energy/Rebates-and-Incentives/Business-Solutions","js_required":True},
    {"name":"CPS Energy Business Programs","org":"CPS Energy","state":"TX","url":"https://www.cpsenergy.com/en/my-home/my-account/save-money-on-my-bill/business-programs.html","js_required":False},
    {"name":"Georgia Power Business Programs","org":"Georgia Power","state":"GA","url":"https://www.georgiapower.com/business/save-energy-money/rebates-and-incentives.html","js_required":True},
]


def scrape_utilities(conn):
    print("\n[Utilities] Scraping utility rebate pages...")
    for src in UTILITY_SOURCES:
        print(f"  [{src['state']}] {src['name']}")
        try:
            text = scrape_source(src)
            if not text:
                continue
            saved = save_source_programs(conn, src, text, "Energy Efficiency")
            conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
                (src["name"], datetime.datetime.utcnow().isoformat(), saved, "ok", None))
            conn.commit()
            print(f"    {saved} snippets saved.")
        except Exception as e:
            print(f"    [error] {e}")
            conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
                (src["name"], datetime.datetime.utcnow().isoformat(), 0, "error", str(e)))
            conn.commit()
        time.sleep(1.0)


# ─── 6. State Energy Offices (expanded) ───────────────────────────────────────

STATE_ENERGY_OFFICES = [
    {"name":"NYSERDA Programs","org":"NYSERDA","state":"NY","url":"https://www.nyserda.ny.gov/All-Programs","js_required":False},
    {"name":"MassCEC Clean Energy","org":"MassCEC","state":"MA","url":"https://www.masscec.com/programs","js_required":False},
    {"name":"CT DEEP Energy Programs","org":"CT DEEP","state":"CT","url":"https://portal.ct.gov/DEEP/Energy","js_required":False},
    {"name":"NJ BPU Clean Energy","org":"NJ BPU","state":"NJ","url":"https://njcleanenergy.com/commercial-industrial","js_required":False},
    {"name":"PA DEP Energy Programs","org":"PA DEP","state":"PA","url":"https://www.dep.pa.gov/Business/Energy","js_required":False},
    {"name":"FL DEO Energy","org":"Florida DEO","state":"FL","url":"https://floridajobs.org/community-planning-and-development/energy","js_required":False},
    {"name":"NC Energy Office","org":"NC DEQ","state":"NC","url":"https://www.ncdeq.gov/energy-efficiency","js_required":False},
    {"name":"VA DEQ Energy","org":"Virginia DEQ","state":"VA","url":"https://www.deq.virginia.gov/programs/energy","js_required":False},
    {"name":"GA Environmental Finance","org":"GEFA","state":"GA","url":"https://gefa.georgia.gov/energy-efficiency","js_required":False},
    {"name":"IL DCEO Energy","org":"Illinois DCEO","state":"IL","url":"https://dceo.illinois.gov/energy","js_required":False},
    {"name":"OH Development Energy","org":"Ohio Development","state":"OH","url":"https://development.ohio.gov/business/energy-resources","js_required":False},
    {"name":"MI EGLE Energy","org":"Michigan EGLE","state":"MI","url":"https://www.michigan.gov/egle/about/organization/materials-management/energy","js_required":False},
    {"name":"MN DEED Energy","org":"MN DEED","state":"MN","url":"https://mn.gov/deed/business/financing-business/energy/","js_required":False},
    {"name":"WI PSC Energy Efficiency","org":"WI PSC","state":"WI","url":"https://psc.wi.gov/Pages/Programs/EnergyEfficiency.aspx","js_required":False},
    {"name":"TX SECO Programs","org":"TX SECO","state":"TX","url":"https://comptroller.texas.gov/programs/seco/","js_required":False},
    {"name":"AZ Commerce Energy","org":"AZ Commerce Authority","state":"AZ","url":"https://www.azcommerce.com/programs/energy","js_required":False},
    {"name":"CO OEDIT Energy Office","org":"CO OEDIT","state":"CO","url":"https://oedit.colorado.gov/energy-office","js_required":False},
    {"name":"CA Energy Commission Programs","org":"CEC","state":"CA","url":"https://www.energy.ca.gov/programs-and-topics/programs","js_required":False},
    {"name":"OR DOE Energy Incentives","org":"Oregon DOE","state":"OR","url":"https://www.oregon.gov/energy/energy-oregon/Pages/Incentives.aspx","js_required":False},
    {"name":"WA Commerce Energy Efficiency","org":"WA Commerce","state":"WA","url":"https://www.commerce.wa.gov/growing-the-economy/energy/energy-efficiency/","js_required":False},
    {"name":"NV Governor's Office Energy","org":"NV GOE","state":"NV","url":"https://goe.nv.gov/Programs/Programs/","js_required":False},
    {"name":"UT Governor's Energy Dev","org":"Utah GED","state":"UT","url":"https://energy.utah.gov/programs/","js_required":False},
    {"name":"MD Clean Energy Center","org":"MCEC","state":"MD","url":"https://www.mdcleanenergy.org/business","js_required":False},
    {"name":"KY Energy & Environment Cabinet","org":"KY EEC","state":"KY","url":"https://eec.ky.gov/Energy","js_required":False},
    {"name":"TN Energy Programs","org":"TN Department of Agriculture","state":"TN","url":"https://www.tn.gov/agriculture/energy.html","js_required":False},
    {"name":"SC Energy Office","org":"SC Energy Office","state":"SC","url":"https://energy.sc.gov/programs","js_required":False},
    {"name":"NE DEE Energy Programs","org":"Nebraska DEE","state":"NE","url":"https://dee.ne.gov/Programs","js_required":False},
    {"name":"KS Commerce Energy","org":"KS Commerce","state":"KS","url":"https://www.kansascommerce.gov/energy/","js_required":False},
    {"name":"ND DOC Energy Programs","org":"ND Commerce","state":"ND","url":"https://www.commerce.nd.gov/energy","js_required":False},
    {"name":"MT DEQ Energy Bureau","org":"Montana DEQ","state":"MT","url":"https://deq.mt.gov/energy/energyproduction","js_required":False},
    {"name":"ID Energy Division","org":"Idaho Commerce","state":"ID","url":"https://commerce.idaho.gov/energy/","js_required":False},
    {"name":"NM Energy Minerals & Natural Resources","org":"NM EMNRD","state":"NM","url":"https://www.emnrd.nm.gov/eed/","js_required":False},
]


def scrape_state_offices(conn, states=None):
    sources = STATE_ENERGY_OFFICES
    if states:
        sources = [s for s in STATE_ENERGY_OFFICES if s["state"] in states]
    print(f"\n[State Offices] Scraping {len(sources)} state energy office pages...")
    total_saved = 0
    for src in sources:
        print(f"  [{src['state']}] {src['name']}")
        try:
            text = scrape_source(src)
            if not text:
                continue
            saved = save_source_programs(conn, src, text, "Energy Efficiency")
            total_saved += saved
            conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
                (src["name"], datetime.datetime.utcnow().isoformat(), saved, "ok", None))
            conn.commit()
            print(f"    {saved} snippets saved.")
        except Exception as e:
            print(f"    [error] {e}")
            conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
                (src["name"], datetime.datetime.utcnow().isoformat(), 0, "error", str(e)))
            conn.commit()
        time.sleep(1.0)
    print(f"  Total state snippets saved: {total_saved}")


# ─── 7. Demand Response Programs ──────────────────────────────────────────────

DEMAND_RESPONSE_SOURCES = [
    {"name":"PJM Interconnection Demand Response","org":"PJM","state":"All","capex_category":"Demand Response",
     "url":"https://www.pjm.com/markets-and-operations/demand-response","incentive_type":"Capacity Payment",
     "notes":"PJM's emergency and economic demand response programs. Industrial and commercial facilities can earn capacity payments for load curtailment."},
    {"name":"NYISO Demand Response Programs","org":"NYISO","state":"NY","capex_category":"Demand Response",
     "url":"https://www.nyiso.com/demand-response","incentive_type":"Capacity + Energy Payment",
     "notes":"NY's DMNC, SCR, and EDRP programs pay facilities to reduce load during peak events. Payments range from $50–$500/kW-year."},
    {"name":"CAISO Demand Response","org":"CAISO","state":"CA","capex_category":"Demand Response",
     "url":"https://www.caiso.com/participate/Pages/DemandResponse/Default.aspx","incentive_type":"Energy + Capacity Payment",
     "notes":"California ISO demand response programs. Available through utilities and aggregators. Industrial loads of 100kW+ qualify."},
    {"name":"ERCOT Demand Response (Texas)","org":"ERCOT","state":"TX","capex_category":"Demand Response",
     "url":"https://www.ercot.com/services/programs/load","incentive_type":"Energy Payment",
     "notes":"ERCOT load participation programs for large C&I customers. Payments tied to real-time energy market prices during scarcity events."},
    {"name":"Eversource Demand Response MA","org":"Eversource","state":"MA","capex_category":"Demand Response",
     "url":"https://www.eversource.com/content/ma-c/business/save-money-energy/demand-response","incentive_type":"Capacity Payment",
     "notes":"Commercial demand response program paying $50–$150/kW-year for summer peak load curtailment."},
    {"name":"Con Edison Demand Management","org":"Con Edison","state":"NY","capex_category":"Demand Response",
     "url":"https://www.coned.com/en/business-partners/demand-management-programs","incentive_type":"Demand Reduction Credit",
     "notes":"Peak load management programs for NYC commercial and industrial accounts. Includes both voluntary and automated demand response options."},
]


def scrape_demand_response(conn):
    print("\n[Demand Response] Saving demand response programs...")
    for src in DEMAND_RESPONSE_SOURCES:
        program = {
            "name": src["name"],
            "org": src["org"],
            "program_type": "Utility" if src["org"] not in ["PJM","NYISO","CAISO","ERCOT"] else "Grid Operator",
            "capex_category": "Demand Response",
            "state": src["state"],
            "equipment": json.dumps(["Building Controls","HVAC","Chiller","Lighting","Battery Storage"]),
            "incentive_amount": "See program ($/kW-year)",
            "incentive_type": src["incentive_type"],
            "max_incentive": "Varies by load size",
            "deadline": "Ongoing",
            "eligible_sectors": "Commercial, Industrial",
            "stacking_allowed": "Yes",
            "source_url": src["url"],
            "notes": src["notes"],
            "raw_text": "",
            "source": "DemandResponse",
        }
        upsert_program(conn, program)
        time.sleep(0.3)

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("DemandResponse", datetime.datetime.utcnow().isoformat(), len(DEMAND_RESPONSE_SOURCES), "ok", None))
    conn.commit()
    print(f"  Saved {len(DEMAND_RESPONSE_SOURCES)} demand response programs.")


# ─── 8. Water & Wastewater Programs ───────────────────────────────────────────

WATER_SOURCES = [
    {"name":"CA DWR Water Efficiency Grants","org":"CA Dept Water Resources","state":"CA",
     "url":"https://water.ca.gov/Programs/Integrated-Regional-Water-Management","capex_category":"Water & Wastewater",
     "incentive_type":"Grant","notes":"Grants for water recycling, efficiency, and conservation projects."},
    {"name":"TX TWDB Water Conservation Loans","org":"TX Water Development Board","state":"TX",
     "url":"https://www.twdb.texas.gov/financial/programs/wcc/","capex_category":"Water & Wastewater",
     "incentive_type":"Low-interest loan","notes":"SWIFT and CWSRF loans for water system efficiency and conservation infrastructure."},
    {"name":"NY Environmental Facilities Corp Water Loans","org":"NY EFC","state":"NY",
     "url":"https://www.efc.ny.gov/clean-water-state-revolving-fund","capex_category":"Water & Wastewater",
     "incentive_type":"Low-interest loan","notes":"Below-market financing for water and wastewater infrastructure improvements."},
    {"name":"Metropolitan Water District Conservation Rebates","org":"MWD Southern CA","state":"CA",
     "url":"https://www.mwdh2o.com/act-now/conservation-programs/rebate-programs/","capex_category":"Water & Wastewater",
     "incentive_type":"Rebate","notes":"Rebates for commercial water efficiency equipment including cooling tower controls and efficient irrigation."},
    {"name":"Denver Water Conservation Rebates","org":"Denver Water","state":"CO",
     "url":"https://www.denverwater.org/for-business/business-rebates","capex_category":"Water & Wastewater",
     "incentive_type":"Rebate","notes":"Commercial rebates for water-efficient equipment."},
]


def scrape_water_programs(conn):
    print("\n[Water] Saving water & wastewater programs...")
    for src in WATER_SOURCES:
        text = scrape_source(src)
        program = {
            "name": src["name"],
            "org": src["org"],
            "program_type": "State",
            "capex_category": src["capex_category"],
            "state": src["state"],
            "equipment": json.dumps(["Water Efficiency","Wastewater","Pump","Boiler"]),
            "incentive_amount": extract_amount(text) if text else "See program",
            "incentive_type": src["incentive_type"],
            "max_incentive": "Varies",
            "deadline": "Rolling",
            "eligible_sectors": "Commercial, Industrial, Municipal",
            "stacking_allowed": "Yes",
            "source_url": src["url"],
            "notes": src["notes"],
            "raw_text": text[:600] if text else "",
            "source": "Water",
        }
        upsert_program(conn, program)
        time.sleep(0.5)

    conn.execute("INSERT INTO scrape_log VALUES (null,?,?,?,?,?)",
        ("Water", datetime.datetime.utcnow().isoformat(), len(WATER_SOURCES), "ok", None))
    conn.commit()
    print(f"  Saved {len(WATER_SOURCES)} water programs.")


# ─── 9. Export & Summary ──────────────────────────────────────────────────────

def export_to_csv(conn, path="capex_incentives_export.csv"):
    df = pd.read_sql("SELECT * FROM programs ORDER BY capex_category, state, program_type, name", conn)
    df.to_csv(path, index=False)
    print(f"\n[export] Saved {len(df)} programs to {path}")
    return df


def export_to_json(conn, path="capex_incentives.json"):
    df = pd.read_sql("SELECT * FROM programs ORDER BY capex_category, state, name", conn)
    df["equipment"] = df["equipment"].apply(lambda x: json.loads(x) if x else [])
    df.to_json(path, orient="records", indent=2)
    print(f"[export] Saved {len(df)} programs to {path}")


def print_summary(conn):
    total = conn.execute("SELECT COUNT(*) FROM programs").fetchone()[0]
    print(f"\n{'='*55}")
    print(f"  CAPEX INCENTIVE DATABASE SUMMARY")
    print(f"  Total programs: {total}")
    print(f"{'─'*55}")
    for row in conn.execute("SELECT capex_category, COUNT(*) FROM programs GROUP BY capex_category ORDER BY 2 DESC").fetchall():
        print(f"  {row[0] or 'Uncategorized':30s}: {row[1]}")
    print(f"{'─'*55}")
    for row in conn.execute("SELECT program_type, COUNT(*) FROM programs GROUP BY program_type ORDER BY 2 DESC").fetchall():
        print(f"  {row[0] or 'Unknown':30s}: {row[1]}")
    print(f"{'='*55}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CAPEX Incentive Scraper — Full Suite")
    parser.add_argument("--sources", nargs="+",
        choices=["all","dsire","federal","utilities","states","pace","greenbanks","demand","water"],
        default=["all"])
    parser.add_argument("--states", nargs="+", help="Limit state scraping e.g. --states NY CA TX")
    args = parser.parse_args()

    run_all = "all" in args.sources
    print(f"\n{'='*55}")
    print(f"  CAPEX INCENTIVE SCRAPER  v2.0")
    print(f"  {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}\n")

    conn = init_db(DB_PATH)
    equip_all = list(EQUIPMENT_KEYWORDS.keys())

    if run_all or "dsire"      in args.sources: fetch_dsire(conn, equipment_filter=equip_all)
    if run_all or "federal"    in args.sources: scrape_federal_sources(conn)
    if run_all or "utilities"  in args.sources: scrape_utilities(conn)
    if run_all or "states"     in args.sources: scrape_state_offices(conn, states=args.states)
    if run_all or "pace"       in args.sources: scrape_pace_sources(conn)
    if run_all or "greenbanks" in args.sources: scrape_green_banks(conn)
    if run_all or "demand"     in args.sources: scrape_demand_response(conn)
    if run_all or "water"      in args.sources: scrape_water_programs(conn)

    export_to_csv(conn)
    export_to_json(conn)
    print_summary(conn)
    print("Done. Run alert.py to send digest email.\n")
