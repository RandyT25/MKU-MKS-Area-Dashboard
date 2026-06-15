#!/usr/bin/env python3
"""
MKU & MKS Dashboard — Data Pipeline (Multi-Day, Multi-Month)
Processes ALL unprocessed dates found in uploads/ in one run.
"""

import json, re, sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)

REPO_ROOT     = Path(__file__).parent.parent
UPLOADS_DIR   = REPO_ROOT / "uploads"
DATA_JS       = REPO_ROOT / "docs" / "data.js"
DATA_SALES_JS = REPO_ROOT / "docs" / "data_sales.js"

MONTH_SHEET = {
    1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MEI",
    6:"JUN",7:"JUL",8:"AGU",9:"SEP",10:"OKT",11:"NOV",12:"DES",
}

SALES_MAP = {
    "I MADE LUIH":"I Made Luih","I MADE LUIH (SANUR)":"I Made Luih","I Made Luih (Sanur)":"I Made Luih",
    "LANI":"Lani","LANI P":"Lani","Lani P":"Lani",
    "NI KETUT SRIASIH (GT)":"Sriasih (GT)","Ni Ketut Sriasih (GT)":"Sriasih (GT)",
    "NI KETUT SRIASIH (MT)":"Sriasih (MT)","Ni Ketut Sriasih (MT)":"Sriasih (MT)","SRIASIH":"Sriasih (MT)",
    "PICROM STALYON":"Picrom","Picrom Stalyon":"Picrom","PICROM":"Picrom",
    "SALES RETAIL":"Sales Retail","JUNI":"Juni","MONICA":"Monica","SUJANA":"Sujana",
    "EKA":"Eka","TAUFIK":"Taufik","DEWI KRISTIANI":"Dewi Kristiani","WIRA":"Wira",
    "RIDWAN":"Ridwan","REDI":"Redi","GEK MAS":"Gek Mas",
    "NP1":"Ridwan","NP 1":"Ridwan","NP2":"Redi","NP 2":"Redi",
    "NP3":"Gek Mas","NP 3":"Gek Mas","NP4":"Gek Mas",
    "KA":"KA","Management Bali":"Management Bali","MANAGEMENT BALI":"Management Bali",
}

AREA_NAME_MAP = {
    "UBUD":"UBUD","DENPASAR - SANUR":"DENPASAR - SANUR",
    "KUTA SELATAN - ULUWATU":"KUTA SEL - ULUWATU","KUTA - INDUSTRI":"KUTA - INDUSTRI",
    "KUTA - HOTEL":"KUTA - HOTEL","KUTA SELATAN - NUSA DUA":"KUTA SEL - NUSA DUA",
    "SEMINYAK":"SEMINYAK","KUTA - LEGIAN":"KUTA - LEGIAN","CANGGU 1":"CANGGU 1",
    "CANGGU 2":"CANGGU 2","GT + FOODY":"GT + FOODY",
    "MODERN TRADE + GENERAL TRADE":"MODERN + GENERAL TRADE",
    "MODERN TRADE + GENERAL":"MODERN + GENERAL TRADE",
}

SALES_NORM_MAP = {
    "PICROM":"Picrom","I MADE LUIH":"I Made Luih","NN MADE LUIH":"I Made Luih",
    "JUNI":"Juni","LANI":"Lani","MONICA":"Monica","SUJANA":"Sujana","NN SUJANA":"Sujana",
    "EKA":"Eka","TAUFIK":"Taufik","DEWI KRISTIANI":"Dewi Kristiani","WIRA":"Wira","SRIASIH":"Sriasih",
}

MONTHS_ID = {
    "jan":"01","feb":"02","mar":"03","apr":"04","april":"04",
    "mei":"05","may":"05",
    "jun":"06","juni":"06","june":"06",
    "jul":"07","juli":"07",
    "agu":"08","aug":"08","agus":"08",
    "sep":"09","sept":"09",
    "okt":"10","oct":"10",
    "nov":"11",
    "des":"12","dec":"12",
}

# ── Helpers ────────────────────────────────────────────────────────

def norm_sales(raw):
    if not raw or str(raw).strip().lower() in ("","nan","none"): return "Unknown"
    s = str(raw).strip()
    return SALES_MAP.get(s, s)

def fval(v):
    try:
        f = float(v)
        return 0.0 if f != f else f
    except: return 0.0

def rint(v): return int(round(fval(v)))

def norm_name(f): return f.lower().replace(" ","_")

def extract_date_from_name(name):
    """Extract YYYY-MM-DD from filename. Handles:
       '21 Mei 2026', '2026-05-21', '2026_05_21', '21_05_2026', '02 JUN 26'
    """
    # Pattern: YYYY-MM-DD or YYYY_MM_DD
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", name)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Pattern: DD Mon YYYY or DD Mon YY  e.g. "21 Mei 2026", "02 JUN 26", "3 Juni 2026"
    m = re.search(r"(\d{1,2})[\s_-]+([a-z]{3,5})[\s_-]+(\d{2,4})", name.lower())
    if m:
        mon = MONTHS_ID.get(m.group(2))
        yr = m.group(3)
        if len(yr) == 2: yr = "20" + yr
        if mon: return f"{yr}-{mon}-{int(m.group(1)):02d}"
    # Pattern: just a day number with month context (e.g. "MKU 21.xlsx")
    # handled separately in grouping
    return None

def extract_day_only(name):
    """For files like 'MKU 21.xlsx' or 'MKU 21.xls' — extract just the day number."""
    m = re.search(r"(?<!\d)(\d{1,2})\.xlsx?", name.lower())
    if m: return int(m.group(1))
    return None

# ── Compressors ───────────────────────────────────────────────────

def compress_so(so_list):
    rep_rev={};prod_rev={};cust_rev={};mku_rev=0;mks_rev=0
    for r in so_list:
        s=r["sales"];p=r["product"];c=r["customer"]
        rep_rev[s]=rep_rev.get(s,0)+r["revenue"]
        prod_rev[p]=prod_rev.get(p,0)+r["revenue"]
        if c not in cust_rev: cust_rev[c]={"rev":0,"so":0,"sales":s,"div":r["division"]}
        cust_rev[c]["rev"]+=r["revenue"];cust_rev[c]["so"]+=1
        if r["division"]=="MKU Bali": mku_rev+=r["revenue"]
        else: mks_rev+=r["revenue"]
    return {"rev":sum(r["revenue"] for r in so_list),"cnt":len(so_list),
        "cust_cnt":len(set(r["customer"] for r in so_list)),
        "mku_rev":mku_rev,"mks_rev":mks_rev,"rep_rev":rep_rev,
        "prod_rev":dict(sorted(prod_rev.items(),key=lambda x:-x[1])[:10]),
        "cust":dict(sorted(cust_rev.items(),key=lambda x:-x[1]["rev"])[:20])}

def compress_stock(mku_full, mks_full):
    return {"MKU":[s for s in mku_full if s["st"]!="ok"],
        "MKS":[s for s in mks_full if s["st"]!="ok"],
        "summary":{"mku_total":len(mku_full),"mks_total":len(mks_full),
            "mku_out":sum(1 for s in mku_full if s["st"]=="out"),
            "mks_out":sum(1 for s in mks_full if s["st"]=="out"),
            "mku_crit":sum(1 for s in mku_full if s["st"]=="critical"),
            "mks_crit":sum(1 for s in mks_full if s["st"]=="critical"),
            "mku_low":sum(1 for s in mku_full if s["st"]=="low"),
            "mks_low":sum(1 for s in mks_full if s["st"]=="low")}}

def compress_del(mku_list, mks_list):
    all_del=mku_list+mks_list;by_area={}
    for r in all_del:
        a=(r.get("area") or "").strip() or "All Areas"
        if a not in by_area: by_area[a]={"t":0,"ok":0}
        by_area[a]["t"]+=1
        if r.get("ket")=="FULFILLED": by_area[a]["ok"]+=1
    issues=[r for r in all_del if r.get("ket")=="UNFULFILLED"]
    return {"tot":len(all_del),
        "ful":sum(1 for r in all_del if r.get("ket")=="FULFILLED"),
        "by_area":by_area,"issues":issues,
        "lost_rev":int(sum(r.get("revenue",0) for r in issues))}

# ── Parsers ───────────────────────────────────────────────────────

def parse_so(path, date_str):
    # Read header to detect column layout
    df_hdr = pd.read_excel(path, sheet_name="Sheet", header=None, nrows=1)
    hdr = [str(v).strip().upper() if not pd.isna(v) else "" for v in df_hdr.iloc[0]]

    # Detect format by looking for key columns
    # New format (2026): Tgl Kirim, Tgl SO, No SO, Nama Divisi, Nama Customer, Syarat Bayar, Nama Sales, Kode Brg, Nama Brg, SO(Pcs), Satuan, FJ(Pcs), Sub T.Jual, BS-SO, Tipe, Status, Ket
    # Old format: Tgl Kirim, No SO, Nama Divisi, Nama Customer, JT, Nama Sales, Nama Barang, SO(Pcs), Satuan, FJ(Pcs), sub T.Jual, BS-SO, Tipe, Status, Ket

    # Find No SO column
    no_so_col = None
    for i, h in enumerate(hdr):
        if "NO" in h and "SO" in h: no_so_col = i; break

    # Find key columns by name
    col_map = {}
    for i, h in enumerate(hdr):
        if h in ("NO SO","NO. SO","NOMOR SO","NO SO ") or h=="NO SO": col_map["no_so"]=i
        elif "NO" in h and "SO" in h and "TGL" not in h and "no_so" not in col_map: col_map["no_so"]=i
        elif "DIVISI" in h or "DIVISION" in h: col_map["division"]=i
        elif "CUSTOMER" in h or "NAMA CUST" in h: col_map["customer"]=i
        elif "SYARAT" in h or h=="JT": col_map["jt"]=i
        elif "SALES" in h and "NO" not in h: col_map["sales"]=i
        elif "KODE" in h: col_map["product"]=i
        elif "NAMA BRG" in h or "NAMA BARANG" in h: col_map["product_name"]=i
        elif ("SO" in h and "PCS" in h) and "BS" not in h and "FJ" not in h: col_map["so_pcs"]=i
        elif "SATUAN" in h or h=="UNIT": col_map["unit"]=i
        elif "FJ" in h and "PCS" in h: col_map["fj_pcs"]=i
        elif "T.JUAL" in h or "TJUAL" in h or "SUB T" in h: col_map["revenue"]=i
        elif "BS-SO" in h or "BS SO" in h: col_map["bs_so"]=i
        elif "TIPE" in h or "TYPE" in h: col_map["type"]=i
        elif "STATUS" in h: col_map["status"]=i
        elif "KET" in h or "NOTE" in h: col_map["notes"]=i

    print(f"    SO col_map: {col_map}")

    df = pd.read_excel(path, sheet_name="Sheet", header=None, skiprows=1)
    rows = []
    for _, row in df.iterrows():
        r = list(row)
        no_so_i = col_map.get("no_so", 2)
        if len(r) <= no_so_i: continue
        no_so_val = str(r[no_so_i]).strip() if not pd.isna(r[no_so_i]) else ""
        if not no_so_val or no_so_val.upper() in ("NO. SO","NO SO","NOMOR SO","NAN","NO SO"): continue
        if not no_so_val.startswith("SO-"): continue

        def gc(key, default=""): 
            i=col_map.get(key)
            if i is None or i>=len(r) or pd.isna(r[i]): return default
            return str(r[i]).strip()
        def gv(key):
            i=col_map.get(key)
            if i is None or i>=len(r): return 0.0
            return fval(r[i])

        revenue = gv("revenue")
        so_pcs = gv("so_pcs")
        rows.append({"date":date_str,
            "no_so":no_so_val,
            "division":gc("division"),
            "customer":gc("customer"),
            "jt":gc("jt"),
            "sales":norm_sales(gc("sales")),
            "product":gc("product"),
            "so_pcs":so_pcs,
            "unit":gc("unit"),
            "fj_pcs":gv("fj_pcs"),
            "revenue":revenue,
            "bs_so":gv("bs_so") or revenue,
            "type":gc("type"),
            "status":gc("status"),
            "notes":gc("notes")})
    if len(rows) < 10:
        raise ValueError(f"SO file has only {len(rows)} rows — wrong sheet?")
    return rows

def parse_stock(path):
    df = pd.read_excel(path, sheet_name="all product", header=None, skiprows=1)
    ncols = df.shape[1]
    # 8-col compact format: [code,name,unit,saldo,avg3m,buf3m,sales_period,buf_period]
    # 11-col full format:   [code,name,unit,saldo,m1,m2,m3,avg3m,buf3m,sales_period,buf_period]
    avg3m_col = 4 if ncols <= 8 else 7
    buf_col   = 7 if ncols <= 8 else 10
    items = []
    for _, row in df.iterrows():
        r = list(row); code = r[0]
        if pd.isna(code) or str(code).strip().lower() in ("","nan"): continue
        saldo=fval(r[3]); buf=fval(r[buf_col])
        st="out" if saldo<=0 else "critical" if buf<3 else "low" if buf<7 else "ok"
        items.append({"code":str(code).strip(),
            "name":str(r[1]).strip() if not pd.isna(r[1]) else "",
            "unit":str(r[2]).strip() if not pd.isna(r[2]) else "",
            "saldo":saldo,"avg3m":fval(r[avg3m_col]),"buf":buf,"st":st})
    return items

def parse_delivery(path):
    xl = pd.ExcelFile(path)
    sheet = "Sheet" if "Sheet" in xl.sheet_names else xl.sheet_names[0]
    # Detect format: legacy .xls has 12 cols with a 4-row header;
    # standard .xlsx has 13+ cols with a 1-row header.
    probe = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=1)
    ncols = probe.shape[1]
    is_legacy = ncols <= 12
    skip = 4 if is_legacy else 1
    df = pd.read_excel(xl, sheet_name=sheet, header=None, skiprows=skip)
    rows = []
    for _, row in df.iterrows():
        r = list(row); no_so = r[2]
        if pd.isna(no_so) or str(no_so).strip().lower() in ("","nan","no. so"): continue
        no_so_s = str(no_so).strip()
        if not no_so_s.startswith("SO-"): continue
        if is_legacy:
            # col: 0=seq, 1=date, 2=no_so, 3=customer, 4=sales,
            #      5=prod_code, 6=prod_name, 7=brand, 8=so_kg,
            #      9=unit, 10=bs_pcs, 11=keterangan
            ket_raw = str(r[11]).strip() if len(r) > 11 and not pd.isna(r[11]) else ""
            rows.append({"no_so":no_so_s,
                "area":"",
                "customer":str(r[3]).strip() if not pd.isna(r[3]) else "",
                "sales":norm_sales(r[4]),
                "product":str(r[5]).strip() if not pd.isna(r[5]) else "",
                "unit":str(r[9]).strip() if len(r) > 9 and not pd.isna(r[9]) else "",
                "qty_bs":fval(r[10]) if len(r) > 10 else 0.0,
                "ket":"UNFULFILLED" if ket_raw.upper().startswith("UN") else "FULFILLED"})
        else:
            ket_raw = str(r[12]).strip() if len(r) > 12 and not pd.isna(r[12]) else ""
            rows.append({"no_so":no_so_s,
                "area":str(r[3]).strip() if not pd.isna(r[3]) else "",
                "customer":str(r[4]).strip() if not pd.isna(r[4]) else "",
                "sales":norm_sales(r[5]),
                "product":str(r[6]).strip() if not pd.isna(r[6]) else "",
                "unit":str(r[9]).strip() if not pd.isna(r[9]) else "",
                "qty_bs":fval(r[11]),
                "ket":"UNFULFILLED" if ket_raw.upper().startswith("UN") else "FULFILLED"})
    return rows

def get_cumulative_so_rev(months_data, date_str):
    """Get cumulative SO revenue per sales rep up to and including date_str."""
    mk = date_str[:7]
    mo = months_data.get(mk, {})
    ss = mo.get("so_summary", {})
    dates_in_month = sorted(d for d in ss if d <= date_str)
    rep_rev = {}
    for d in dates_in_month:
        for rep, rev in (ss[d].get("rep_rev", {})).items():
            rep_rev[rep] = rep_rev.get(rep, 0) + rev
    return rep_rev

def get_cumulative_so_rev(months_data, date_str):
    """Get cumulative SO revenue per sales rep up to and including date_str."""
    mk = date_str[:7]
    mo = months_data.get(mk, {})
    ss = mo.get("so_summary", {})
    dates_in_month = sorted(d for d in ss if d <= date_str)
    rep_rev = {}
    for d in dates_in_month:
        for rep, rev in (ss[d].get("rep_rev", {})).items():
            rep_rev[rep] = rep_rev.get(rep, 0) + rev
    return rep_rev

def parse_targets(path, date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    sheet_name = MONTH_SHEET[dt.month]
    month_idx = dt.month
    print(f"    Targets: reading sheet '{sheet_name}'...")
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    rows = list(df.values)

    def cell(r, c):
        v = r[c] if c < len(r) else None
        return "" if v is None or str(v).strip().lower() == "nan" else str(v).strip()

    def find_row(keyword, col=1, start=0):
        for i in range(start, len(rows)):
            if keyword.lower() in cell(rows[i], col).lower(): return i
        return None

    # MKS input section
    input_start = find_row("Nama_Kry", col=1)
    input_end = find_row("INPUT: DATA NESTLE", col=1)
    mks_input = {}
    if input_start is not None and input_end is not None:
        for i in range(input_start+1, input_end):
            name = cell(rows[i], 1)
            if not name: continue
            mks_input[name] = {"food":fval(rows[i][2]),"bev":fval(rows[i][3]),
                "balian":fval(rows[i][4]) if len(rows[i])>4 else 0.0}

    # Nestlé input section
    nestle_start = find_row("INPUT: DATA NESTLE", col=1)
    nestle_raw = {}
    if nestle_start is not None:
        for i in range(nestle_start+2, nestle_start+12):
            if i >= len(rows): break
            name = cell(rows[i], 1); val = fval(rows[i][2])
            nu = name.upper().replace(" ","").replace("-","")
            if not name: nestle_raw["_blank"] = val
            elif "NP1" in nu: nestle_raw["NP1"] = val
            elif "NP2" in nu: nestle_raw["NP2"] = val
            elif "NP3" in nu: nestle_raw["NP3"] = val
            elif "NP4" in nu: nestle_raw["NP4"] = val
            elif "NP5" in nu: nestle_raw["NP5"] = val

    ka_val = mks_input.get("KA",{}).get("food",0.0)
    np5_calc = nestle_raw.get("_blank",0.0)+ka_val+nestle_raw.get("NP5",0.0)
    np3_final = nestle_raw.get("NP3",0.0)+nestle_raw.get("NP4",0.0)+np5_calc
    np1_ach = nestle_raw.get("NP1",0.0)
    np2_ach = nestle_raw.get("NP2",0.0)

    # Global category totals
    global_start = find_row("KATAGORI", col=1)
    food_total_t=food_total_a=bev_total_t=bev_total_a=nes_total_t=nes_total_a=0
    if global_start is not None:
        for i in range(global_start+1, global_start+6):
            if i >= len(rows): break
            cat = cell(rows[i], 1).upper()
            if cat=="FOOD": food_total_t=rint(rows[i][3]); food_total_a=rint(rows[i][4])
            elif cat=="BEVERAGE": bev_total_t=rint(rows[i][3]); bev_total_a=rint(rows[i][4])
            elif cat=="NESTLE": nes_total_t=rint(rows[i][3]); nes_total_a=rint(rows[i][4])

    # Area targets
    fb_start = None
    for i, r in enumerate(rows):
        if cell(r,1).upper() == "FOOD & BEVERAGES": fb_start=i; break
    area_targets = []
    if fb_start is not None:
        i = fb_start+2
        while i < len(rows):
            r=rows[i]; area_raw=cell(r,1); sales_raw=cell(r,2); area_up=area_raw.upper()
            if not area_raw: i+=1; continue
            if area_up in ("GRAND TOTAL","NESTLE","BALIAN","CHANNEL / AREA","AREA"): break
            food_t=rint(r[3]); bev_t=rint(r[4]); food_ach=rint(r[6]); bev_ach=rint(r[7])
            sales_n = SALES_NORM_MAP.get(sales_raw.upper(), sales_raw)
            if "NAUGHTY NURIS" in area_up:
                an = "NAUGHTY NURIS (SANUR)" if "MADE LUIH" in sales_raw.upper() or "NN MADE" in sales_raw.upper() else "NAUGHTY NURIS (SEMINYAK)"
                sales_n = "I Made Luih" if "SANUR" in an else "Sujana"
                pct_v = round(food_ach/food_t*100) if food_t>0 else 0
                area_targets.append({"area":an,"sales":sales_n,"food_target":food_t,"bev_target":0,"food_ach":food_ach,"bev_ach":0,"pct":pct_v})
                i+=1; continue
            matched = next((v for k,v in AREA_NAME_MAP.items() if k in area_up), None)
            if not matched: i+=1; continue
            total_t=food_t+bev_t; total_a=food_ach+bev_ach
            pct_v = round(total_a/total_t*100) if total_t>0 else 0
            area_targets.append({"area":matched,"sales":sales_n,"food_target":food_t,"bev_target":bev_t,
                "food_ach":food_ach,"bev_ach":bev_ach,"pct":pct_v})
            i+=1

    # Balian
    balian_start = find_row("BALIAN", col=1)
    balian_rows = []
    if balian_start is not None:
        for i in range(balian_start+2, balian_start+25):
            if i >= len(rows): break
            r=rows[i]; area_raw=cell(r,1); sales_raw=cell(r,2)
            if not area_raw or area_raw.upper() in ("GRAND TOTAL","AREA","","NESTLE","CHANNEL / AREA"):
                if area_raw.upper() in ("NESTLE","CHANNEL / AREA"): break
                continue
            ach = fval(r[3]); sales_n = SALES_NORM_MAP.get(sales_raw.upper(), sales_raw)
            matched = next((v for k,v in AREA_NAME_MAP.items() if k in area_raw.upper()), None)
            if "NAUGHTY NURIS" in area_raw.upper():
                matched = "NAUGHTY NURIS (SANUR)" if "MADE LUIH" in sales_raw.upper() or "NN MADE" in sales_raw.upper() else "NAUGHTY NURIS (SEMINYAK)"
                sales_n = "I Made Luih" if "SANUR" in matched else "Sujana"
            if not matched: matched = area_raw.strip()
            balian_rows.append({"area":matched,"sales":sales_n,"ach":rint(ach)})

    # Nestlé targets from TARGETS sheet
    tdf = pd.read_excel(path, sheet_name="TARGETS", header=None)
    trows = list(tdf.values)
    def get_nestle_target(keyword):
        col = 3+(month_idx-1)
        for r in trows:
            c1 = str(r[1]).strip() if r[1] is not None else ""
            if keyword.lower() in c1.lower():
                return fval(r[col]) if col < len(r) else 0.0
        return 0.0

    np1_t=get_nestle_target("NP - 1"); np2_t=get_nestle_target("NP - 2"); np3_t=get_nestle_target("NP - 3")
    np1_pct=round(np1_ach/np1_t*100) if np1_t>0 else 0
    np2_pct=round(np2_ach/np2_t*100) if np2_t>0 else 0
    np3_pct=round(np3_final/np3_t*100) if np3_t>0 else 0

    return {"targets":{"FOOD":{"target":food_total_t,"achievement":food_total_a},
        "BEVERAGE":{"target":bev_total_t,"achievement":bev_total_a},
        "NESTLE":{"target":nes_total_t,"achievement":nes_total_a}},
        "nestle_areas":[
            {"area":"NP-1","sales":"Ridwan","target":rint(np1_t),"achievement":rint(np1_ach),"pct":np1_pct},
            {"area":"NP-2","sales":"Redi","target":rint(np2_t),"achievement":rint(np2_ach),"pct":np2_pct},
            {"area":"NP-3","sales":"Gek Mas","target":rint(np3_t),"achievement":rint(np3_final),"pct":np3_pct}],
        "area_targets":area_targets,"balian":balian_rows}

# ── File grouping ─────────────────────────────────────────────────

def group_files_by_date():
    """
    Group all xlsx files in uploads/ by date.
    Handles:
      - Files with full date in name: "Report SO MKU MKS 21 Mei 2026.xlsx"
      - Files with day only: "MKU 21.xlsx", "MKS 21.xlsx"
      - Single pencapaian file: "DATA_PENCAPAIAN_2026.xlsx"
    Returns: dict of {date_str: {so, stk_mku, stk_mks, del_mku, del_mks}}
             and pencapaian path
    """
    files = list(UPLOADS_DIR.glob("*.xlsx")) + list(UPLOADS_DIR.glob("*.xls"))
    if not files:
        print(f"ERROR: No .xlsx files in {UPLOADS_DIR}"); sys.exit(1)

    pencapaian = None
    pencapaian_candidates = []
    # date_files: date_str -> {role -> path}
    date_files = {}

    # First pass: find pencapaian and files with full dates
    for f in files:
        n = norm_name(f.name)
        if "data_pencapaian" in n:
            pencapaian_candidates.append(f)
            continue

        date_str = extract_date_from_name(f.name)

        if date_str:
            if date_str not in date_files:
                date_files[date_str] = {}
            assign_role(f, n, date_files[date_str])

    if not pencapaian_candidates:
        print("ERROR: DATA_PENCAPAIAN file not found in uploads/"); sys.exit(1)
    # Use pencapaian file with latest date in filename, fallback to newest modified
    def pencapaian_date(f):
        import re
        m=re.search(r'(\d{1,2})\s+(\w+)',f.name.lower())
        MONTHS={'jan':1,'feb':2,'mar':3,'apr':4,'mei':5,'may':5,'jun':6,'jul':7,'agu':8,'sep':9,'okt':10,'nov':11,'des':12}
        if m: return MONTHS.get(m.group(2),0)*100+int(m.group(1))
        return 0
    pencapaian = max(pencapaian_candidates, key=lambda f: f.stat().st_mtime)
    if len(pencapaian_candidates) > 1:
        print(f"WARNING: Multiple DATA_PENCAPAIAN files found — using newest: {pencapaian.name}")
        for c in pencapaian_candidates:
            print(f"  {'→ SELECTED' if c == pencapaian else '  ignored '}: {c.name}")

    # Second pass: files with day-only names (MKU 21.xlsx, MKS 21.xlsx)
    # We need to know the year+month — infer from other files or use today
    known_dates = sorted(date_files.keys())
    if known_dates:
        ref_ym = known_dates[-1][:7]  # use latest known YYYY-MM
    else:
        ref_ym = datetime.today().strftime("%Y-%m")

    for f in files:
        n = norm_name(f.name)
        if "data_pencapaian" in n: continue
        if extract_date_from_name(f.name): continue  # already handled

        day = extract_day_only(f.name)
        if day is None: continue
        date_str = f"{ref_ym}-{day:02d}"
        if date_str not in date_files:
            date_files[date_str] = {}
        assign_role(f, n, date_files[date_str])

    return date_files, pencapaian

def assign_role(f, n, d):
    """Assign file to correct role in a date's file dict."""
    if "report_so" in n or "report so" in f.name.lower():
        d["so"] = f
    elif re.search(r"stok.?mku", n) or ("stok" in n and "mku" in n):
        d["stk_mku"] = f
    elif re.search(r"stok.?mks", n) or ("stok" in n and "mks" in n):
        d["stk_mks"] = f
    elif n.startswith("mku") and "stok" not in n:
        d["del_mku"] = f
    elif n.startswith("mks") and "stok" not in n:
        d["del_mks"] = f

def load_existing():
    if not DATA_JS.exists():
        print("WARNING: No existing data.js — starting fresh.")
        return None
    content = DATA_JS.read_text(encoding="utf-8").strip()
    if content.startswith("const RAW ="): content = content[len("const RAW ="):].strip()
    if content.endswith(";"): content = content[:-1]
    try:
        raw = json.loads(content)
        if "months" not in raw:
            print("Migrating to multi-month structure...")
            month_key = raw["latest"][:7]
            dt = datetime.strptime(raw["latest"], "%Y-%m-%d")
            raw = {"latest":raw["latest"],"so":raw.get("so",[]),
                "months":{month_key:{"label":dt.strftime("%B %Y"),"dates":raw.get("dates",[]),
                    "so_summary":raw.get("so_summary",{}),"stock_by_date":raw.get("stock_by_date",{}),
                    "delivery_by_date":raw.get("delivery_by_date",{}),"targets_by_date":raw.get("targets_by_date",{})}}}
        return raw
    except json.JSONDecodeError as e:
        print(f"ERROR parsing data.js: {e}"); sys.exit(1)

# ── Main ──────────────────────────────────────────────────────────

def main():
    print("="*60)
    print("MKU & MKS Dashboard Pipeline (Multi-Day)")
    print("="*60)

    date_files, pencapaian = group_files_by_date()
    print(f"\nDATA_PENCAPAIAN: {pencapaian.name}")
    print(f"Dates found: {sorted(date_files.keys())}")

    raw = load_existing()
    if raw is None:
        raw = {"latest": None, "so": [], "months": {}}

    already_processed = set()
    for mk, mo in raw.get("months", {}).items():
        for d in mo.get("dates", []):
            already_processed.add(d)

    dates_to_process = sorted(date_files.keys())
    new_dates = [d for d in dates_to_process if d not in already_processed]
    reprocess = [d for d in dates_to_process if d in already_processed]

    if reprocess:
        print(f"\nAlready processed (will re-process latest only): {reprocess}")
    if not new_dates and not dates_to_process:
        print("Nothing to process."); return

    # Process all dates in chronological order
    # All dates get compressed (so_summary only, no full rows) except the LAST one
    all_dates = dates_to_process  # process all found, re-process included
    complete = [d for d in all_dates if all(r in date_files[d] for r in ["so","stk_mku","stk_mks","del_mku","del_mks"])]
    latest_date = max(complete) if complete else max(all_dates)

    for date_str in sorted(all_dates):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        month_key = date_str[:7]
        month_label = dt.strftime("%B %Y")
        files = date_files[date_str]

        # Check required files
        missing = [r for r in ["so","stk_mku","stk_mks","del_mku","del_mks"] if r not in files]
        if missing:
            print(f"\n⚠ SKIP {date_str} — missing: {missing}")
            print(f"  Found roles: {list(files.keys())}")
            print(f"  Found files: {[f.name for f in files.values()]}")
            continue

        is_latest = (date_str == latest_date)
        print(f"\n{'★ ' if is_latest else '  '}Processing {date_str} ({'latest — keeping full data' if is_latest else 'compressing'})...")

        # Ensure month exists
        if month_key not in raw["months"]:
            print(f"  New month: {month_label}")
            raw["months"][month_key] = {"label":month_label,"dates":[],
                "so_summary":{},"stock_by_date":{},"delivery_by_date":{},"targets_by_date":{}}
        m = raw["months"][month_key]

        # Parse
        try:
            so_rows = parse_so(files["so"], date_str)
            print(f"  SO: {len(so_rows)} rows")
        except Exception as e:
            print(f"  ERROR parsing SO: {e}"); continue

        try:
            mku_stock = parse_stock(files["stk_mku"])
            mks_stock = parse_stock(files["stk_mks"])
            print(f"  Stock MKU: {len(mku_stock)}, MKS: {len(mks_stock)}")
        except Exception as e:
            print(f"  ERROR parsing stock: {e}"); continue

        try:
            del_mku_rows = parse_delivery(files["del_mku"])
            del_mks_rows = parse_delivery(files["del_mks"])
            print(f"  Delivery MKU: {len(del_mku_rows)}, MKS: {len(del_mks_rows)}")
        except Exception as e:
            print(f"  ERROR parsing delivery: {e}"); continue

        try:
            targets_entry = parse_targets(pencapaian, date_str)
            t = targets_entry["targets"]
            print(f"  FOOD: {t['FOOD']['achievement']:,.0f}/{t['FOOD']['target']:,.0f} | BEV: {t['BEVERAGE']['achievement']:,.0f}/{t['BEVERAGE']['target']:,.0f} | NES: {t['NESTLE']['achievement']:,.0f}/{t['NESTLE']['target']:,.0f}")
        except Exception as e:
            print(f"  WARNING: targets skipped ({e})")
            targets_entry = {"targets":{"FOOD":{"target":0,"achievement":0},"BEVERAGE":{"target":0,"achievement":0},"NESTLE":{"target":0,"achievement":0}},"nestle_areas":[],"area_targets":[],"balian":[]}

        # Store so_summary for every date (compressed)
        m["so_summary"][date_str] = compress_so(so_rows)

        # Always overwrite targets so re-uploads of pencapaian take effect
        m["targets_by_date"][date_str] = targets_entry

        if is_latest:
            # Latest date: keep full data for live dashboard
            stk = compress_stock(mku_stock, mks_stock)
            stk["MKU_full"] = mku_stock
            stk["MKS_full"] = mks_stock
            m["stock_by_date"][date_str] = stk

            dl = compress_del(del_mku_rows, del_mks_rows)
            dl["mku_full"] = del_mku_rows
            dl["mks_full"] = del_mks_rows
            m["delivery_by_date"][date_str] = dl

            # Update top-level latest SO rows
            raw["latest"] = date_str
            raw["so"] = so_rows
        else:
            # Historical date: compress only
            m["stock_by_date"][date_str] = compress_stock(mku_stock, mks_stock)
            m["delivery_by_date"][date_str] = compress_del(del_mku_rows, del_mks_rows)

        if date_str not in m["dates"]:
            m["dates"] = sorted(m["dates"] + [date_str])

    # Write data.js
    DATA_JS.parent.mkdir(parents=True, exist_ok=True)
    output = "const RAW = " + json.dumps(raw, ensure_ascii=False, separators=(",",":")) + ";"
    DATA_JS.write_text(output, encoding="utf-8")
    print(f"\n✓ data.js written ({DATA_JS.stat().st_size/1024:.1f} KB)")
    print(f"  Months: {list(raw['months'].keys())}")
    for mk, mo in raw["months"].items():
        print(f"  {mk}: {mo['dates']}")

    # Write data_sales.js (latest day only)
    latest = raw["latest"]
    lm = raw["months"][latest[:7]]
    latest_tgt = lm["targets_by_date"].get(latest, {})
    latest_stk = lm["stock_by_date"].get(latest, {})
    sales_raw = {"latest":latest,"month":lm["label"],
        "targets_by_date":{latest: latest_tgt},
        "stock_by_date":{latest:{"MKU_full":latest_stk.get("MKU_full",[]),
            "MKS_full":latest_stk.get("MKS_full",[]),"summary":latest_stk.get("summary",{})}},
        "so":raw["so"]}
    DATA_SALES_JS.write_text("const RAW = "+json.dumps(sales_raw,ensure_ascii=False,separators=(",",":"))+";",encoding="utf-8")
    print(f"✓ data_sales.js written ({DATA_SALES_JS.stat().st_size/1024:.1f} KB)")
    print(f"\nPipeline complete. Latest: {raw['latest']} ✓")

if __name__ == "__main__":
    main()
