#!/usr/bin/env python3
"""
MKU & MKS Dashboard — Data Pipeline
Reads 5 Excel files from uploads/, plus DATA_PENCAPAIAN for targets,
then updates docs/data.js preserving history.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).parent.parent
UPLOADS_DIR  = REPO_ROOT / "uploads"
DATA_JS      = REPO_ROOT / "docs" / "data.js"

# ── Month sheet name map (Bahasa) ─────────────────────────────────────────────
MONTH_SHEET = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MEI", 6: "JUN", 7: "JUL", 8: "AGU",
    9: "SEP", 10: "OKT", 11: "NOV", 12: "DES",
}

# ── Sales name normalisation ───────────────────────────────────────────────────
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

def fval(v):
    """Safe float, returns 0.0 for nan/None."""
    try:
        f = float(v)
        return 0.0 if (f != f) else f
    except (TypeError, ValueError):
        return 0.0

def rint(v):
    return int(round(fval(v)))


# ── Compress functions ─────────────────────────────────────────────────────────
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


# ── File parsers ───────────────────────────────────────────────────────────────
def parse_so(path, date_str):
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
            "so_pcs":   fval(r[7]),
            "unit":     str(r[8]).strip() if not pd.isna(r[8]) else "",
            "fj_pcs":   fval(r[9]),
            "revenue":  fval(r[10]),
            "bs_so":    fval(r[11]),
            "type":     str(r[12]).strip() if not pd.isna(r[12]) else "",
            "status":   str(r[13]).strip() if not pd.isna(r[13]) else "",
            "notes":    str(r[14]).strip() if not pd.isna(r[14]) else "",
        })
    if len(rows) < 100:
        raise ValueError(
            f"SO file has only {len(rows)} rows — looks like wrong sheet "
            f"(Balian-only). Expected 100+ from 'Sheet' (not 'Sheet1')."
        )
    return rows


def parse_stock(path):
    df = pd.read_excel(path, sheet_name="all product", header=None, skiprows=1)
    items = []
    for _, row in df.iterrows():
        r = list(row)
        code = r[0]
        if pd.isna(code) or str(code).strip().lower() in ("", "nan"):
            continue
        saldo = fval(r[3])
        buf   = fval(r[8])
        if saldo <= 0:   st = "out"
        elif buf < 3:    st = "critical"
        elif buf < 7:    st = "low"
        else:            st = "ok"
        items.append({
            "code":  str(code).strip(),
            "name":  str(r[1]).strip() if not pd.isna(r[1]) else "",
            "unit":  str(r[2]).strip() if not pd.isna(r[2]) else "",
            "saldo": saldo,
            "avg3m": fval(r[7]),
            "buf":   buf,
            "st":    st,
        })
    return items


def parse_delivery(path):
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
            "qty_bs":   fval(r[10]),
            "ket":      ket,
        })
    return rows


# ── Targets parser ─────────────────────────────────────────────────────────────
def parse_targets(path, date_str):
    """Parse targets + achievement from DATA_PENCAPAIAN Excel."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    sheet_name = MONTH_SHEET[dt.month]
    month_idx  = dt.month  # 1-based

    print(f"  Reading sheet '{sheet_name}'...")
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    rows = list(df.values)

    def cell(r, c):
        v = r[c] if c < len(r) else None
        return "" if v is None or str(v).strip().lower() == "nan" else str(v).strip()

    def find_row(keyword, col=1, start=0):
        for i in range(start, len(rows)):
            if keyword.lower() in cell(rows[i], col).lower():
                return i
        return None

    # ── MKS input table (col 1=name, 2=food, 3=bev, 4=balian) ───────────────
    input_start  = find_row("Nama_Kry",         col=1)
    input_end    = find_row("INPUT: DATA NESTLE", col=1)
    mks_input = {}
    if input_start is not None and input_end is not None:
        for i in range(input_start + 1, input_end):
            name = cell(rows[i], 1)
            if not name:
                continue
            mks_input[name] = {
                "food":   fval(rows[i][2]),
                "bev":    fval(rows[i][3]),
                "balian": fval(rows[i][4]) if len(rows[i]) > 4 else 0.0,
            }

    # ── Nestlé input table ────────────────────────────────────────────────────
    nestle_start = find_row("INPUT: DATA NESTLE", col=1)
    nestle_raw   = {}
    if nestle_start is not None:
        for i in range(nestle_start + 2, nestle_start + 12):
            if i >= len(rows): break
            name = cell(rows[i], 1)
            val  = fval(rows[i][2])
            nu   = name.upper().replace(" ", "").replace("-", "")
            if not name:
                nestle_raw["_blank"] = val
            elif "NP1" in nu: nestle_raw["NP1"] = val
            elif "NP2" in nu: nestle_raw["NP2"] = val
            elif "NP3" in nu: nestle_raw["NP3"] = val
            elif "NP4" in nu: nestle_raw["NP4"] = val
            elif "NP5" in nu: nestle_raw["NP5"] = val

    # ── Nestlé calculations ───────────────────────────────────────────────────
    ka_val    = mks_input.get("KA", {}).get("food", 0.0)
    blank_val = nestle_raw.get("_blank", 0.0)
    np5_calc  = blank_val + ka_val + nestle_raw.get("NP5", 0.0)
    np3_final = nestle_raw.get("NP3", 0.0) + nestle_raw.get("NP4", 0.0) + np5_calc
    np1_ach   = nestle_raw.get("NP1", 0.0)
    np2_ach   = nestle_raw.get("NP2", 0.0)

    # ── Global totals (FOOD / BEVERAGE / NESTLE rows) ─────────────────────────
    global_start = find_row("KATAGORI", col=1)
    food_total_t = food_total_a = 0
    bev_total_t  = bev_total_a  = 0
    nes_total_t  = nes_total_a  = 0
    if global_start is not None:
        for i in range(global_start + 1, global_start + 6):
            if i >= len(rows): break
            cat = cell(rows[i], 1).upper()
            if cat == "FOOD":
                food_total_t = rint(rows[i][3]); food_total_a = rint(rows[i][4])
            elif cat == "BEVERAGE":
                bev_total_t  = rint(rows[i][3]); bev_total_a  = rint(rows[i][4])
            elif cat == "NESTLE":
                nes_total_t  = rint(rows[i][3]); nes_total_a  = rint(rows[i][4])

    # ── Area table (Food & Beverages computed section) ────────────────────────
    AREA_NAME_MAP = {
        "UBUD":                          "UBUD",
        "DENPASAR - SANUR":              "DENPASAR - SANUR",
        "KUTA SELATAN - ULUWATU":        "KUTA SEL - ULUWATU",
        "KUTA - INDUSTRI":               "KUTA - INDUSTRI",
        "KUTA - HOTEL":                  "KUTA - HOTEL",
        "KUTA SELATAN - NUSA DUA":       "KUTA SEL - NUSA DUA",
        "SEMINYAK":                      "SEMINYAK",
        "KUTA - LEGIAN":                 "KUTA - LEGIAN",
        "CANGGU 1":                      "CANGGU 1",
        "CANGGU 2":                      "CANGGU 2",
        "GT + FOODY":                    "GT + FOODY",
        "MODERN TRADE + GENERAL TRADE":  "MODERN + GENERAL TRADE",
        "MODERN TRADE + GENERAL":        "MODERN + GENERAL TRADE",
    }
    SALES_NORM_MAP = {
        "PICROM": "Picrom", "I MADE LUIH": "I Made Luih",
        "NN MADE LUIH": "I Made Luih", "JUNI": "Juni",
        "LANI": "Lani", "MONICA": "Monica", "SUJANA": "Sujana",
        "NN SUJANA": "Sujana", "EKA": "Eka", "TAUFIK": "Taufik",
        "DEWI KRISTIANI": "Dewi Kristiani", "WIRA": "Wira",
        "SRIASIH": "Sriasih",
    }

    # Find the computed area table — exact match on "FOOD & BEVERAGES" (not the input label)
    fb_start = None
    for i, r in enumerate(rows):
        c = cell(r, 1)
        if c.upper() == "FOOD & BEVERAGES":
            fb_start = i
            break
    area_targets = []
    if fb_start is not None:
        i = fb_start + 2  # skip 2 header rows
        while i < len(rows):
            r     = rows[i]
            area_raw  = cell(r, 1)
            sales_raw = cell(r, 2)
            area_up   = area_raw.upper()

            if not area_raw:
                i += 1; continue
            if area_up in ("GRAND TOTAL", "NESTLE", "BALIAN", "CHANNEL / AREA", "AREA"):
                break

            # col layout: 3=food_t, 4=bev_t, 6=food_ach, 7=bev_ach
            food_t   = rint(r[3])
            bev_t    = rint(r[4])
            food_ach = rint(r[6])
            bev_ach  = rint(r[7])
            sales_n  = SALES_NORM_MAP.get(sales_raw.upper(), sales_raw)

            if "NAUGHTY NURIS" in area_up:
                if "MADE LUIH" in sales_raw.upper() or "NN MADE" in sales_raw.upper():
                    an = "NAUGHTY NURIS (SANUR)";    sales_n = "I Made Luih"
                else:
                    an = "NAUGHTY NURIS (SEMINYAK)"; sales_n = "Sujana"
                pct = round(food_ach / food_t * 100) if food_t > 0 else 0
                area_targets.append({
                    "area": an, "sales": sales_n,
                    "food_target": food_t, "bev_target": 0,
                    "food_ach": food_ach, "bev_ach": 0, "pct": pct
                })
                i += 1; continue

            matched = None
            for k, v in AREA_NAME_MAP.items():
                if k in area_up:
                    matched = v; break
            if not matched:
                i += 1; continue

            total_t = food_t + bev_t
            total_a = food_ach + bev_ach
            pct = round(total_a / total_t * 100) if total_t > 0 else 0
            area_targets.append({
                "area": matched, "sales": sales_n,
                "food_target": food_t, "bev_target": bev_t,
                "food_ach": food_ach, "bev_ach": bev_ach, "pct": pct
            })
            i += 1

    # ── Balian table ──────────────────────────────────────────────────────────
    balian_start = find_row("BALIAN", col=1)
    balian_by_area = {}
    if balian_start is not None:
        for i in range(balian_start + 2, balian_start + 20):
            if i >= len(rows): break
            r = rows[i]
            area_raw  = cell(r, 1)
            sales_raw = cell(r, 2)
            if not area_raw or area_raw.upper() in ("GRAND TOTAL", "AREA", ""):
                continue
            ach = fval(r[3])
            matched = None
            for k, v in AREA_NAME_MAP.items():
                if k in area_raw.upper():
                    matched = v; break
            if matched:
                sales_n = SALES_NORM_MAP.get(sales_raw.upper(), sales_raw)
                balian_by_area[matched] = {"sales": sales_n, "ach": rint(ach)}

    # ── Nestlé targets from TARGETS sheet ─────────────────────────────────────
    tdf  = pd.read_excel(path, sheet_name="TARGETS", header=None)
    trows = list(tdf.values)

    def get_nestle_target(keyword):
        col = 2 + (month_idx - 1)  # each month = 1 col in Nestlé section
        for r in trows:
            c1 = str(r[1]).strip() if r[1] is not None else ""
            if keyword.lower() in c1.lower():
                return fval(r[col]) if col < len(r) else 0.0
        return 0.0

    np1_t = get_nestle_target("NP - 1")
    np2_t = get_nestle_target("NP - 2")
    np3_t = get_nestle_target("NP - 3")

    # ── Assemble ──────────────────────────────────────────────────────────────
    np1_pct = round(np1_ach   / np1_t * 100) if np1_t > 0 else 0
    np2_pct = round(np2_ach   / np2_t * 100) if np2_t > 0 else 0
    np3_pct = round(np3_final / np3_t * 100) if np3_t > 0 else 0

    return {
        "targets": {
            "FOOD":     {"target": food_total_t, "achievement": food_total_a},
            "BEVERAGE": {"target": bev_total_t,  "achievement": bev_total_a},
            "NESTLE":   {"target": nes_total_t,  "achievement": nes_total_a},
        },
        "nestle_areas": [
            {"area": "NP-1", "sales": "Ridwan",  "target": rint(np1_t),
             "achievement": rint(np1_ach),   "pct": np1_pct},
            {"area": "NP-2", "sales": "Redi",    "target": rint(np2_t),
             "achievement": rint(np2_ach),   "pct": np2_pct},
            {"area": "NP-3", "sales": "Gek Mas", "target": rint(np3_t),
             "achievement": rint(np3_final), "pct": np3_pct},
        ],
        "area_targets": area_targets,
        "balian": balian_by_area,
    }


# ── Find upload files ──────────────────────────────────────────────────────────
def find_uploads():
    files = list(UPLOADS_DIR.glob("*.xlsx"))
    if not files:
        print(f"ERROR: No .xlsx files found in {UPLOADS_DIR}")
        sys.exit(1)

    so_file = stk_mku = stk_mks = del_mku = del_mks = pencapaian = None
    date_str = None

    for f in files:
        name = f.name
        if re.match(r"DATA_PENCAPAIAN", name, re.IGNORECASE):
            pencapaian = f
        elif re.match(r"Report_SO_MKU_MKS_", name, re.IGNORECASE):
            so_file = f
            m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
            if m:
                date_str = m.group(1)
            else:
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
    if not so_file:    missing.append("Report_SO_MKU_MKS_[date].xlsx")
    if not stk_mku:    missing.append("Stok_MKU_[date].xlsx")
    if not stk_mks:    missing.append("Stok_MKS_[date].xlsx")
    if not del_mku:    missing.append("MKU_[date].xlsx")
    if not del_mks:    missing.append("MKS_[date].xlsx")
    if not pencapaian: missing.append("DATA_PENCAPAIAN_[year].xlsx")
    if missing:
        print("ERROR: Missing upload files:\n  " + "\n  ".join(missing))
        sys.exit(1)

    if not date_str:
        date_str = datetime.today().strftime("%Y-%m-%d")
        print(f"WARNING: Could not extract date from SO filename. Using today: {date_str}")

    print(f"Found files for date: {date_str}")
    print(f"  SO:          {so_file.name}")
    print(f"  Stok MKU:    {stk_mku.name}")
    print(f"  Stok MKS:    {stk_mks.name}")
    print(f"  Del MKU:     {del_mku.name}")
    print(f"  Del MKS:     {del_mks.name}")
    print(f"  Pencapaian:  {pencapaian.name}")
    return date_str, so_file, stk_mku, stk_mks, del_mku, del_mks, pencapaian


# ── Load existing data.js ──────────────────────────────────────────────────────
def load_existing():
    if not DATA_JS.exists():
        print("WARNING: docs/data.js not found — starting fresh.")
        return None
    content = DATA_JS.read_text(encoding="utf-8").strip()
    if content.startswith("const RAW ="):
        content = content[len("const RAW ="):].strip()
    if content.endswith(";"):
        content = content[:-1]
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse existing data.js: {e}")
        sys.exit(1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("MKU & MKS Dashboard Pipeline")
    print("=" * 60)

    date_str, so_file, stk_mku, stk_mks, del_mku, del_mks, pencapaian = find_uploads()

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

    # Compress previous latest day
    prev = raw.get("latest")
    if prev and prev != date_str:
        print(f"\nCompressing previous latest day: {prev}")
        raw["so_summary"][prev] = compress_so(raw.get("so", []))
        prev_stk = raw["stock_by_date"].get(prev, {})
        raw["stock_by_date"][prev] = compress_stock(
            prev_stk.get("MKU_full", prev_stk.get("MKU", [])),
            prev_stk.get("MKS_full", prev_stk.get("MKS", []))
        )
        prev_del = raw["delivery_by_date"].get(prev, {})
        raw["delivery_by_date"][prev] = compress_del(
            prev_del.get("mku_full", []),
            prev_del.get("mks_full", [])
        )

    # Parse new files
    print(f"\nParsing files for {date_str}...")
    print("  Parsing SO...")
    so_rows = parse_so(so_file, date_str)
    print(f"    → {len(so_rows)} rows")

    print("  Parsing stock MKU...")
    mku_stock = parse_stock(stk_mku)
    print(f"    → {len(mku_stock)} items")

    print("  Parsing stock MKS...")
    mks_stock = parse_stock(stk_mks)
    print(f"    → {len(mks_stock)} items")

    print("  Parsing delivery MKU...")
    del_mku_rows = parse_delivery(del_mku)
    print(f"    → {len(del_mku_rows)} rows")

    print("  Parsing delivery MKS...")
    del_mks_rows = parse_delivery(del_mks)
    print(f"    → {len(del_mks_rows)} rows")

    print("  Parsing targets (DATA_PENCAPAIAN)...")
    targets_entry = parse_targets(pencapaian, date_str)
    t = targets_entry["targets"]
    print(f"    → FOOD:     {t['FOOD']['achievement']:>15,.0f} / {t['FOOD']['target']:>15,.0f}")
    print(f"    → BEVERAGE: {t['BEVERAGE']['achievement']:>15,.0f} / {t['BEVERAGE']['target']:>15,.0f}")
    print(f"    → NESTLE:   {t['NESTLE']['achievement']:>15,.0f} / {t['NESTLE']['target']:>15,.0f}")
    print(f"    → Balian areas: {len(targets_entry['balian'])}")

    # Update RAW
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

    # Write data.js
    print(f"\nWriting docs/data.js...")
    DATA_JS.parent.mkdir(parents=True, exist_ok=True)
    output = "const RAW = " + json.dumps(raw, ensure_ascii=False, separators=(",", ":")) + ";"
    DATA_JS.write_text(output, encoding="utf-8")
    size_kb = DATA_JS.stat().st_size / 1024
    print(f"  ✓ Written ({size_kb:.1f} KB) — dates: {raw['dates']}")
    print("\nPipeline complete. ✓")


if __name__ == "__main__":
    main()
