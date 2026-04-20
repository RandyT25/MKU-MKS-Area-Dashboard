#!/usr/bin/env python3
"""
MKU & MKS Dashboard — Data Pipeline
Reads 5 Excel files from uploads/, updates docs/data.js preserving history.
"""

import json
import os
import re
import sys
import glob
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).parent.parent
UPLOADS_DIR = REPO_ROOT / "uploads"
DATA_JS     = REPO_ROOT / "docs" / "data.js"

# ── Sales name normalisation ──────────────────────────────────────────────────
SALES_MAP = {
    "I MADE LUIH":               "I Made Luih",
    "I MADE LUIH (SANUR)":       "I Made Luih",
    "I Made Luih (Sanur)":       "I Made Luih",
    "LANI":                      "Lani",
    "LANI P":                    "Lani",
    "Lani P":                    "Lani",
    "NI KETUT SRIASIH (GT)":     "Sriasih (GT)",
    "Ni Ketut Sriasih (GT)":     "Sriasih (GT)",
    "NI KETUT SRIASIH (MT)":     "Sriasih (MT)",
    "Ni Ketut Sriasih (MT)":     "Sriasih (MT)",
    "SRIASIH":                   "Sriasih (MT)",
    "PICROM STALYON":            "Picrom",
    "Picrom Stalyon":            "Picrom",
    "PICROM":                    "Picrom",
    "SALES RETAIL":              "Sales Retail",
    "JUNI":                      "Juni",
    "MONICA":                    "Monica",
    "SUJANA":                    "Sujana",
    "EKA":                       "Eka",
    "TAUFIK":                    "Taufik",
    "DEWI KRISTIANI":            "Dewi Kristiani",
    "WIRA":                      "Wira",
    "RIDWAN":                    "Ridwan",
    "REDI":                      "Redi",
    "GEK MAS":                   "Gek Mas",
    "NP1":                       "Ridwan",
    "NP 1":                      "Ridwan",
    "NP2":                       "Redi",
    "NP 2":                      "Redi",
    "NP3":                       "Gek Mas",
    "NP 3":                      "Gek Mas",
    "NP4":                       "Gek Mas",
    "KA":                        "KA",
    "Management Bali":           "Management Bali",
    "MANAGEMENT BALI":           "Management Bali",
}

def norm_sales(raw):
    if not raw or str(raw).strip().lower() in ("", "nan", "none"):
        return "Unknown"
    s = str(raw).strip()
    return SALES_MAP.get(s, s)


# ── Compress functions ────────────────────────────────────────────────────────
def compress_so(so_list):
    rep_rev = {}; prod_rev = {}; cust_rev = {}
    mku_rev = 0; mks_rev = 0
    for r in so_list:
        s = r["sales"]; p = r["product"]; c = r["customer"]
        rep_rev[s]  = rep_rev.get(s, 0)  + r["revenue"]
        prod_rev[p] = prod_rev.get(p, 0) + r["revenue"]
        if c not in cust_rev:
            cust_rev[c] = {"rev": 0, "so": 0, "sales": s, "div": r["division"]}
        cust_rev[c]["rev"] += r["revenue"]
        cust_rev[c]["so"]  += 1
        if r["division"] == "MKU Bali":
            mku_rev += r["revenue"]
        else:
            mks_rev += r["revenue"]
    return {
        "rev":      sum(r["revenue"] for r in so_list),
        "cnt":      len(so_list),
        "cust_cnt": len(set(r["customer"] for r in so_list)),
        "mku_rev":  mku_rev,
        "mks_rev":  mks_rev,
        "rep_rev":  rep_rev,
        "prod_rev": dict(sorted(prod_rev.items(), key=lambda x: -x[1])[:10]),
        "cust":     dict(sorted(cust_rev.items(), key=lambda x: -x[1]["rev"])[:20]),
    }


def compress_stock(mku_full, mks_full):
    return {
        "MKU": [s for s in mku_full if s["st"] != "ok"],
        "MKS": [s for s in mks_full if s["st"] != "ok"],
        "summary": {
            "mku_total": len(mku_full),
            "mks_total": len(mks_full),
            "mku_out":   len([s for s in mku_full if s["st"] == "out"]),
            "mks_out":   len([s for s in mks_full if s["st"] == "out"]),
            "mku_crit":  len([s for s in mku_full if s["st"] == "critical"]),
            "mks_crit":  len([s for s in mks_full if s["st"] == "critical"]),
            "mku_low":   len([s for s in mku_full if s["st"] == "low"]),
            "mks_low":   len([s for s in mks_full if s["st"] == "low"]),
        },
    }


def compress_del(mku_list, mks_list):
    all_del = mku_list + mks_list
    by_area = {}
    for r in all_del:
        a = (r.get("area") or "").strip() or "All Areas"
        if a not in by_area:
            by_area[a] = {"t": 0, "ok": 0}
        by_area[a]["t"] += 1
        if r.get("ket") == "FULFILLED":
            by_area[a]["ok"] += 1
    return {
        "tot":     len(all_del),
        "ful":     len([r for r in all_del if r.get("ket") == "FULFILLED"]),
        "by_area": by_area,
        "issues":  [r for r in all_del if r.get("ket") == "UNFULFILLED"],
    }


# ── File parsers ──────────────────────────────────────────────────────────────
def parse_so(path, date_str):
    """Parse Report_SO_MKU_MKS_[date].xlsx"""
    df = pd.read_excel(path, sheet_name="Sheet", header=None, skiprows=1)
    rows = []
    for _, row in df.iterrows():
        r = list(row)
        if pd.isna(r[0]) or pd.isna(r[1]):
            continue
        rows.append({
            "date":     date_str,
            "no_so":    str(r[1]).strip(),
            "division": str(r[2]).strip() if not pd.isna(r[2]) else "",
            "customer": str(r[3]).strip() if not pd.isna(r[3]) else "",
            "jt":       str(r[4]).strip() if not pd.isna(r[4]) else "",
            "sales":    norm_sales(r[5]),
            "product":  str(r[6]).strip() if not pd.isna(r[6]) else "",
            "so_pcs":   float(r[7]) if not pd.isna(r[7]) else 0.0,
            "unit":     str(r[8]).strip() if not pd.isna(r[8]) else "",
            "fj_pcs":   float(r[9])  if not pd.isna(r[9])  else 0.0,
            "revenue":  float(r[10]) if not pd.isna(r[10]) else 0.0,
            "bs_so":    float(r[11]) if not pd.isna(r[11]) else 0.0,
            "type":     str(r[12]).strip() if not pd.isna(r[12]) else "",
            "status":   str(r[13]).strip() if not pd.isna(r[13]) else "",
            "notes":    str(r[14]).strip() if not pd.isna(r[14]) else "",
        })
    if len(rows) < 100:
        raise ValueError(
            f"SO file has only {len(rows)} rows — looks like the wrong sheet "
            f"(Balian-only). Expected 100+ rows from 'Sheet' (not 'Sheet1')."
        )
    return rows


def parse_stock(path):
    """Parse Stok_MKU_[date].xlsx or Stok_MKS_[date].xlsx"""
    df = pd.read_excel(path, sheet_name="all product", header=None, skiprows=1)
    items = []
    for _, row in df.iterrows():
        r = list(row)
        code = r[0]
        if pd.isna(code) or str(code).strip().lower() in ("", "nan"):
            continue
        saldo = float(r[3]) if not pd.isna(r[3]) else 0.0
        buf   = float(r[8]) if not pd.isna(r[8]) else 0.0
        if saldo <= 0:
            st = "out"
        elif buf < 3:
            st = "critical"
        elif buf < 7:
            st = "low"
        else:
            st = "ok"
        items.append({
            "code":  str(code).strip(),
            "name":  str(r[1]).strip() if not pd.isna(r[1]) else "",
            "unit":  str(r[2]).strip() if not pd.isna(r[2]) else "",
            "saldo": saldo,
            "avg3m": float(r[7]) if not pd.isna(r[7]) else 0.0,
            "buf":   buf,
            "st":    st,
        })
    return items


def parse_delivery(path):
    """Parse MKU_[date].xlsx or MKS_[date].xlsx"""
    df = pd.read_excel(path, sheet_name="Sheet", header=None, skiprows=1)
    rows = []
    for _, row in df.iterrows():
        r = list(row)
        no_so = r[2]
        if pd.isna(no_so) or str(no_so).strip().lower() in ("", "nan", "no. so"):
            continue
        ket_raw = str(r[11]).strip() if not pd.isna(r[11]) else ""
        ket = "UNFULFILLED" if ket_raw.upper().startswith("UN") else "FULFILLED"
        rows.append({
            "no_so":    str(no_so).strip(),
            "area":     str(r[3]).strip()  if not pd.isna(r[3])  else "",
            "customer": str(r[4]).strip()  if not pd.isna(r[4])  else "",
            "sales":    norm_sales(r[5]),
            "product":  str(r[6]).strip()  if not pd.isna(r[6])  else "",
            "unit":     str(r[9]).strip()  if not pd.isna(r[9])  else "",
            "qty_bs":   float(r[10])       if not pd.isna(r[10]) else 0.0,
            "ket":      ket,
        })
    return rows


# ── Find upload files ─────────────────────────────────────────────────────────
def find_uploads():
    """Find the 5 expected Excel files in uploads/ and extract date."""
    files = list(UPLOADS_DIR.glob("*.xlsx"))
    if not files:
        print(f"ERROR: No .xlsx files found in {UPLOADS_DIR}")
        sys.exit(1)

    so_file = stk_mku = stk_mks = del_mku = del_mks = None
    date_str = None

    for f in files:
        name = f.name
        if re.match(r"Report_SO_MKU_MKS_", name, re.IGNORECASE):
            so_file = f
            m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
            if m:
                date_str = m.group(1)
            else:
                # Try DDMMYYYY or similar fallback
                m2 = re.search(r"(\d{2})(\d{2})(\d{4})", name)
                if m2:
                    date_str = f"{m2.group(3)}-{m2.group(2)}-{m2.group(1)}"
        elif re.match(r"Stok_MKU_", name, re.IGNORECASE):
            stk_mku = f
        elif re.match(r"Stok_MKS_", name, re.IGNORECASE):
            stk_mks = f
        elif re.match(r"MKU_", name, re.IGNORECASE):
            del_mku = f
        elif re.match(r"MKS_", name, re.IGNORECASE):
            del_mks = f

    missing = []
    if not so_file:  missing.append("Report_SO_MKU_MKS_[date].xlsx")
    if not stk_mku:  missing.append("Stok_MKU_[date].xlsx")
    if not stk_mks:  missing.append("Stok_MKS_[date].xlsx")
    if not del_mku:  missing.append("MKU_[date].xlsx")
    if not del_mks:  missing.append("MKS_[date].xlsx")
    if missing:
        print(f"ERROR: Missing upload files:\n  " + "\n  ".join(missing))
        sys.exit(1)

    if not date_str:
        # Fallback: use today's date
        date_str = datetime.today().strftime("%Y-%m-%d")
        print(f"WARNING: Could not extract date from SO filename. Using today: {date_str}")

    print(f"Found files for date: {date_str}")
    print(f"  SO:       {so_file.name}")
    print(f"  Stok MKU: {stk_mku.name}")
    print(f"  Stok MKS: {stk_mks.name}")
    print(f"  Del MKU:  {del_mku.name}")
    print(f"  Del MKS:  {del_mks.name}")
    return date_str, so_file, stk_mku, stk_mks, del_mku, del_mks


# ── Load existing data.js ─────────────────────────────────────────────────────
def load_existing():
    if not DATA_JS.exists():
        print("WARNING: docs/data.js not found — starting fresh.")
        return None
    content = DATA_JS.read_text(encoding="utf-8").strip()
    # Strip JS wrapper: "const RAW = {...};"
    if content.startswith("const RAW ="):
        content = content[len("const RAW ="):].strip()
    if content.endswith(";"):
        content = content[:-1]
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse existing data.js: {e}")
        sys.exit(1)


# ── Targets entry (hardcoded — update from screenshot each day) ───────────────
def build_targets_entry(date_str, raw):
    """
    Returns a targets entry for the new date.

    IMPORTANT: This function returns None, signalling the pipeline to
    COPY the previous day's targets_by_date entry as a placeholder.
    You must update targets_by_date[date_str] manually (or add screenshot
    parsing here) to reflect the actual daily achievement.

    To add real target data: replace this function's body with the actual
    values from your screenshot, or implement OCR-based parsing.
    """
    # ── Option A: Copy previous latest targets (placeholder) ──────────────────
    prev = raw.get("latest")
    if prev and prev in raw.get("targets_by_date", {}):
        prev_targets = raw["targets_by_date"][prev]
        print(f"  NOTE: targets_by_date['{date_str}'] copied from {prev}.")
        print(f"        Update manually with today's actual achievement values.")
        return prev_targets
    # ── Option B: Return empty scaffold ───────────────────────────────────────
    return {
        "targets": {
            "FOOD":     {"target": 6619279333, "achievement": 0},
            "BEVERAGE": {"target": 1269766212, "achievement": 0},
            "NESTLE":   {"target": 1791489362, "achievement": 0},
        },
        "nestle_areas": [
            {"area": "NP-1", "sales": "Ridwan",  "target": 279391895,  "achievement": 0, "pct": 0},
            {"area": "NP-2", "sales": "Redi",    "target": 419611959,  "achievement": 0, "pct": 0},
            {"area": "NP-3", "sales": "Gek Mas", "target": 1092485507, "achievement": 0, "pct": 0},
        ],
        "area_targets": [
            {"area": "UBUD",                     "sales": "Picrom",         "food_target": 396683478,  "bev_target": 49588709,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "DENPASAR - SANUR",         "sales": "I Made Luih",   "food_target": 535182371,  "bev_target": 172865092, "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "NAUGHTY NURIS (SANUR)",    "sales": "I Made Luih",   "food_target": 241473268,  "bev_target": 0,         "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "KUTA SEL - ULUWATU",       "sales": "Juni",          "food_target": 590712472,  "bev_target": 28200135,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "KUTA - INDUSTRI",          "sales": "Lani",          "food_target": 395063284,  "bev_target": 73528469,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "KUTA - HOTEL",             "sales": "Lani",          "food_target": 463918832,  "bev_target": 71898548,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "KUTA SEL - NUSA DUA",      "sales": "Monica",        "food_target": 941758345,  "bev_target": 73246884,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "SEMINYAK",                 "sales": "Sujana",        "food_target": 485287059,  "bev_target": 45099712,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "NAUGHTY NURIS (SEMINYAK)", "sales": "Sujana",        "food_target": 404133664,  "bev_target": 0,         "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "KUTA - LEGIAN",            "sales": "Eka",           "food_target": 384017309,  "bev_target": 119782698, "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "CANGGU 1",                 "sales": "Taufik",        "food_target": 611629596,  "bev_target": 40059909,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "CANGGU 2",                 "sales": "Dewi Kristiani","food_target": 366543023,  "bev_target": 45082463,  "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "GT + FOODY",               "sales": "Wira",          "food_target": 563350942,  "bev_target": 311125253, "food_ach": 0, "bev_ach": 0, "pct": 0},
            {"area": "MODERN + GENERAL TRADE",   "sales": "Sriasih",       "food_target": 239525689,  "bev_target": 239288340, "food_ach": 0, "bev_ach": 0, "pct": 0},
        ],
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("MKU & MKS Dashboard Pipeline")
    print("=" * 60)

    # 1. Find upload files
    date_str, so_file, stk_mku, stk_mks, del_mku, del_mks = find_uploads()

    # 2. Load existing data.js
    raw = load_existing()
    if raw is None:
        raw = {
            "dates":            [],
            "month":            datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %Y"),
            "latest":           None,
            "so":               [],
            "so_summary":       {},
            "stock_by_date":    {},
            "delivery_by_date": {},
            "targets_by_date":  {},
        }

    if date_str in raw["dates"] and date_str == raw.get("latest"):
        print(f"\nWARNING: {date_str} is already the latest date in data.js.")
        print("Re-processing will overwrite today's data. Continuing...")

    # 3. Compress previous latest day (if exists and different from new date)
    prev = raw.get("latest")
    if prev and prev != date_str:
        print(f"\nCompressing previous latest day: {prev}")
        raw["so_summary"][prev] = compress_so(raw.get("so", []))

        prev_stk = raw["stock_by_date"].get(prev, {})
        mku_f = prev_stk.get("MKU_full", prev_stk.get("MKU", []))
        mks_f = prev_stk.get("MKS_full", prev_stk.get("MKS", []))
        raw["stock_by_date"][prev] = compress_stock(mku_f, mks_f)

        prev_del = raw["delivery_by_date"].get(prev, {})
        mku_d = prev_del.get("mku_full", [])
        mks_d = prev_del.get("mks_full", [])
        raw["delivery_by_date"][prev] = compress_del(mku_d, mks_d)

    # 4. Parse new files
    print(f"\nParsing files for {date_str}...")

    print("  Parsing SO...")
    so_rows = parse_so(so_file, date_str)
    print(f"    → {len(so_rows)} SO rows")

    print("  Parsing stock MKU...")
    mku_stock = parse_stock(stk_mku)
    print(f"    → {len(mku_stock)} MKU stock items")

    print("  Parsing stock MKS...")
    mks_stock = parse_stock(stk_mks)
    print(f"    → {len(mks_stock)} MKS stock items")

    print("  Parsing delivery MKU...")
    del_mku_rows = parse_delivery(del_mku)
    print(f"    → {len(del_mku_rows)} MKU delivery rows")

    print("  Parsing delivery MKS...")
    del_mks_rows = parse_delivery(del_mks)
    print(f"    → {len(del_mks_rows)} MKS delivery rows")

    # 5. Build targets entry
    print(f"\nBuilding targets entry for {date_str}...")
    targets_entry = build_targets_entry(date_str, raw)

    # 6. Add new latest day
    print(f"\nUpdating RAW for {date_str}...")
    raw["latest"] = date_str
    raw["so"]     = so_rows
    raw["so_summary"][date_str] = compress_so(so_rows)

    stk = compress_stock(mku_stock, mks_stock)
    stk["MKU_full"] = mku_stock
    stk["MKS_full"] = mks_stock
    raw["stock_by_date"][date_str] = stk

    dl = compress_del(del_mku_rows, del_mks_rows)
    dl["mku_full"] = del_mku_rows
    dl["mks_full"] = del_mks_rows
    raw["delivery_by_date"][date_str] = dl

    raw["targets_by_date"][date_str] = targets_entry

    if date_str not in raw["dates"]:
        raw["dates"] = sorted(raw["dates"] + [date_str])

    # 7. Write data.js
    print(f"\nWriting docs/data.js...")
    DATA_JS.parent.mkdir(parents=True, exist_ok=True)
    output = "const RAW = " + json.dumps(raw, ensure_ascii=False, separators=(",", ":")) + ";"
    DATA_JS.write_text(output, encoding="utf-8")
    size_kb = DATA_JS.stat().st_size / 1024
    print(f"  ✓ docs/data.js written ({size_kb:.1f} KB)")
    print(f"  Dates in dataset: {raw['dates']}")
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
