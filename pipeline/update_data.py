#!/usr/bin/env python3
"""
MKU & MKS Dashboard — Data Pipeline (Multi-Month)
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

REPO_ROOT    = Path(__file__).parent.parent
UPLOADS_DIR  = REPO_ROOT / "uploads"
DATA_JS      = REPO_ROOT / "docs" / "data.js"
DATA_SALES_JS = REPO_ROOT / "docs" / "data_sales.js"

MONTH_SHEET = {
    1:"JAN",2:"FEB",3:"MAR",4:"APR",
    5:"MEI",6:"JUN",7:"JUL",8:"AGU",
    9:"SEP",10:"OKT",11:"NOV",12:"DES",
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

def norm_sales(raw):
    if not raw or str(raw).strip().lower() in ("","nan","none"): return "Unknown"
    s = str(raw).strip()
    return SALES_MAP.get(s, s)

def fval(v):
    try:
        f = float(v)
        return 0.0 if (f != f) else f
    except: return 0.0

def rint(v): return int(round(fval(v)))

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

def compress_stock(mku_full,mks_full):
    return {"MKU":[s for s in mku_full if s["st"]!="ok"],
        "MKS":[s for s in mks_full if s["st"]!="ok"],
        "summary":{"mku_total":len(mku_full),"mks_total":len(mks_full),
            "mku_out":len([s for s in mku_full if s["st"]=="out"]),
            "mks_out":len([s for s in mks_full if s["st"]=="out"]),
            "mku_crit":len([s for s in mku_full if s["st"]=="critical"]),
            "mks_crit":len([s for s in mks_full if s["st"]=="critical"]),
            "mku_low":len([s for s in mku_full if s["st"]=="low"]),
            "mks_low":len([s for s in mks_full if s["st"]=="low"])}}

def compress_del(mku_list,mks_list):
    all_del=mku_list+mks_list;by_area={}
    for r in all_del:
        a=(r.get("area") or "").strip() or "All Areas"
        if a not in by_area: by_area[a]={"t":0,"ok":0}
        by_area[a]["t"]+=1
        if r.get("ket")=="FULFILLED": by_area[a]["ok"]+=1
    return {"tot":len(all_del),"ful":len([r for r in all_del if r.get("ket")=="FULFILLED"]),
        "by_area":by_area,"issues":[r for r in all_del if r.get("ket")=="UNFULFILLED"]}

def parse_so(path,date_str):
    df=pd.read_excel(path,sheet_name="Sheet",header=None,skiprows=1)
    rows=[]
    for _,row in df.iterrows():
        r=list(row)
        if pd.isna(r[0]) or pd.isna(r[1]): continue
        rows.append({"date":date_str,"no_so":str(r[1]).strip(),
            "division":str(r[2]).strip() if not pd.isna(r[2]) else "",
            "customer":str(r[3]).strip() if not pd.isna(r[3]) else "",
            "jt":str(r[4]).strip() if not pd.isna(r[4]) else "",
            "sales":norm_sales(r[5]),"product":str(r[6]).strip() if not pd.isna(r[6]) else "",
            "so_pcs":fval(r[7]),"unit":str(r[8]).strip() if not pd.isna(r[8]) else "",
            "fj_pcs":fval(r[9]),"revenue":fval(r[10]),"bs_so":fval(r[11]),
            "type":str(r[12]).strip() if not pd.isna(r[12]) else "",
            "status":str(r[13]).strip() if not pd.isna(r[13]) else "",
            "notes":str(r[14]).strip() if not pd.isna(r[14]) else ""})
    if len(rows)<100: raise ValueError(f"SO file has only {len(rows)} rows — wrong sheet?")
    return rows

def parse_stock(path):
    df=pd.read_excel(path,sheet_name="all product",header=None,skiprows=1)
    items=[]
    for _,row in df.iterrows():
        r=list(row);code=r[0]
        if pd.isna(code) or str(code).strip().lower() in ("","nan"): continue
        saldo=fval(r[3]);buf=fval(r[8])
        st="out" if saldo<=0 else "critical" if buf<3 else "low" if buf<7 else "ok"
        items.append({"code":str(code).strip(),"name":str(r[1]).strip() if not pd.isna(r[1]) else "",
            "unit":str(r[2]).strip() if not pd.isna(r[2]) else "",
            "saldo":saldo,"avg3m":fval(r[7]),"buf":buf,"st":st})
    return items

def parse_delivery(path):
    df=pd.read_excel(path,sheet_name="Sheet",header=None,skiprows=1)
    rows=[]
    for _,row in df.iterrows():
        r=list(row);no_so=r[2]
        if pd.isna(no_so) or str(no_so).strip().lower() in ("","nan","no. so"): continue
        ket_raw=str(r[11]).strip() if not pd.isna(r[11]) else ""
        rows.append({"no_so":str(no_so).strip(),
            "area":str(r[3]).strip() if not pd.isna(r[3]) else "",
            "customer":str(r[4]).strip() if not pd.isna(r[4]) else "",
            "sales":norm_sales(r[5]),"product":str(r[6]).strip() if not pd.isna(r[6]) else "",
            "unit":str(r[9]).strip() if not pd.isna(r[9]) else "",
            "qty_bs":fval(r[10]),"ket":"UNFULFILLED" if ket_raw.upper().startswith("UN") else "FULFILLED"})
    return rows

def parse_targets(path,date_str):
    dt=datetime.strptime(date_str,"%Y-%m-%d")
    sheet_name=MONTH_SHEET[dt.month];month_idx=dt.month
    print(f"  Reading sheet '{sheet_name}'...")
    df=pd.read_excel(path,sheet_name=sheet_name,header=None)
    rows=list(df.values)

    def cell(r,c):
        v=r[c] if c<len(r) else None
        return "" if v is None or str(v).strip().lower()=="nan" else str(v).strip()

    def find_row(keyword,col=1,start=0):
        for i in range(start,len(rows)):
            if keyword.lower() in cell(rows[i],col).lower(): return i
        return None

    input_start=find_row("Nama_Kry",col=1);input_end=find_row("INPUT: DATA NESTLE",col=1)
    mks_input={}
    if input_start is not None and input_end is not None:
        for i in range(input_start+1,input_end):
            name=cell(rows[i],1)
            if not name: continue
            mks_input[name]={"food":fval(rows[i][2]),"bev":fval(rows[i][3]),
                "balian":fval(rows[i][4]) if len(rows[i])>4 else 0.0}

    nestle_start=find_row("INPUT: DATA NESTLE",col=1);nestle_raw={}
    if nestle_start is not None:
        for i in range(nestle_start+2,nestle_start+12):
            if i>=len(rows): break
            name=cell(rows[i],1);val=fval(rows[i][2])
            nu=name.upper().replace(" ","").replace("-","")
            if not name: nestle_raw["_blank"]=val
            elif "NP1" in nu: nestle_raw["NP1"]=val
            elif "NP2" in nu: nestle_raw["NP2"]=val
            elif "NP3" in nu: nestle_raw["NP3"]=val
            elif "NP4" in nu: nestle_raw["NP4"]=val
            elif "NP5" in nu: nestle_raw["NP5"]=val

    ka_val=mks_input.get("KA",{}).get("food",0.0)
    np5_calc=nestle_raw.get("_blank",0.0)+ka_val+nestle_raw.get("NP5",0.0)
    np3_final=nestle_raw.get("NP3",0.0)+nestle_raw.get("NP4",0.0)+np5_calc
    np1_ach=nestle_raw.get("NP1",0.0);np2_ach=nestle_raw.get("NP2",0.0)

    global_start=find_row("KATAGORI",col=1)
    food_total_t=food_total_a=bev_total_t=bev_total_a=nes_total_t=nes_total_a=0
    if global_start is not None:
        for i in range(global_start+1,global_start+6):
            if i>=len(rows): break
            cat=cell(rows[i],1).upper()
            if cat=="FOOD": food_total_t=rint(rows[i][3]);food_total_a=rint(rows[i][4])
            elif cat=="BEVERAGE": bev_total_t=rint(rows[i][3]);bev_total_a=rint(rows[i][4])
            elif cat=="NESTLE": nes_total_t=rint(rows[i][3]);nes_total_a=rint(rows[i][4])

    AREA_NAME_MAP={"UBUD":"UBUD","DENPASAR - SANUR":"DENPASAR - SANUR",
        "KUTA SELATAN - ULUWATU":"KUTA SEL - ULUWATU","KUTA - INDUSTRI":"KUTA - INDUSTRI",
        "KUTA - HOTEL":"KUTA - HOTEL","KUTA SELATAN - NUSA DUA":"KUTA SEL - NUSA DUA",
        "SEMINYAK":"SEMINYAK","KUTA - LEGIAN":"KUTA - LEGIAN","CANGGU 1":"CANGGU 1",
        "CANGGU 2":"CANGGU 2","GT + FOODY":"GT + FOODY",
        "MODERN TRADE + GENERAL TRADE":"MODERN + GENERAL TRADE",
        "MODERN TRADE + GENERAL":"MODERN + GENERAL TRADE"}
    SALES_NORM_MAP={"PICROM":"Picrom","I MADE LUIH":"I Made Luih","NN MADE LUIH":"I Made Luih",
        "JUNI":"Juni","LANI":"Lani","MONICA":"Monica","SUJANA":"Sujana","NN SUJANA":"Sujana",
        "EKA":"Eka","TAUFIK":"Taufik","DEWI KRISTIANI":"Dewi Kristiani","WIRA":"Wira","SRIASIH":"Sriasih"}

    fb_start=None
    for i,r in enumerate(rows):
        if cell(r,1).upper()=="FOOD & BEVERAGES": fb_start=i;break
    area_targets=[]
    if fb_start is not None:
        i=fb_start+2
        while i<len(rows):
            r=rows[i];area_raw=cell(r,1);sales_raw=cell(r,2);area_up=area_raw.upper()
            if not area_raw: i+=1;continue
            if area_up in ("GRAND TOTAL","NESTLE","BALIAN","CHANNEL / AREA","AREA"): break
            food_t=rint(r[3]);bev_t=rint(r[4]);food_ach=rint(r[6]);bev_ach=rint(r[7])
            sales_n=SALES_NORM_MAP.get(sales_raw.upper(),sales_raw)
            if "NAUGHTY NURIS" in area_up:
                an="NAUGHTY NURIS (SANUR)" if "MADE LUIH" in sales_raw.upper() or "NN MADE" in sales_raw.upper() else "NAUGHTY NURIS (SEMINYAK)"
                sales_n="I Made Luih" if "SANUR" in an else "Sujana"
                pct=round(food_ach/food_t*100) if food_t>0 else 0
                area_targets.append({"area":an,"sales":sales_n,"food_target":food_t,"bev_target":0,"food_ach":food_ach,"bev_ach":0,"pct":pct})
                i+=1;continue
            matched=next((v for k,v in AREA_NAME_MAP.items() if k in area_up),None)
            if not matched: i+=1;continue
            total_t=food_t+bev_t;total_a=food_ach+bev_ach
            pct=round(total_a/total_t*100) if total_t>0 else 0
            area_targets.append({"area":matched,"sales":sales_n,"food_target":food_t,"bev_target":bev_t,"food_ach":food_ach,"bev_ach":bev_ach,"pct":pct})
            i+=1

    balian_start=find_row("BALIAN",col=1);balian_rows=[]
    if balian_start is not None:
        for i in range(balian_start+2,balian_start+25):
            if i>=len(rows): break
            r=rows[i];area_raw=cell(r,1);sales_raw=cell(r,2)
            if not area_raw or area_raw.upper() in ("GRAND TOTAL","AREA","","NESTLE","CHANNEL / AREA"):
                if area_raw.upper() in ("NESTLE","CHANNEL / AREA"): break
                continue
            ach=fval(r[3]);sales_n=SALES_NORM_MAP.get(sales_raw.upper(),sales_raw)
            matched=next((v for k,v in AREA_NAME_MAP.items() if k in area_raw.upper()),None)
            if "NAUGHTY NURIS" in area_raw.upper():
                matched="NAUGHTY NURIS (SANUR)" if "MADE LUIH" in sales_raw.upper() or "NN MADE" in sales_raw.upper() else "NAUGHTY NURIS (SEMINYAK)"
                sales_n="I Made Luih" if "SANUR" in matched else "Sujana"
            if not matched: matched=area_raw.strip()
            balian_rows.append({"area":matched,"sales":sales_n,"ach":rint(ach)})

    tdf=pd.read_excel(path,sheet_name="TARGETS",header=None);trows=list(tdf.values)
    def get_nestle_target(keyword):
        col=3+(month_idx-1)  # col1=CHANNEL, col2=SALES, col3=JAN, col4=FEB...
        for r in trows:
            c1=str(r[1]).strip() if r[1] is not None else ""
            if keyword.lower() in c1.lower(): return fval(r[col]) if col<len(r) else 0.0
        return 0.0

    np1_t=get_nestle_target("NP - 1");np2_t=get_nestle_target("NP - 2");np3_t=get_nestle_target("NP - 3")
    np1_pct=round(np1_ach/np1_t*100) if np1_t>0 else 0
    np2_pct=round(np2_ach/np2_t*100) if np2_t>0 else 0
    np3_pct=round(np3_final/np3_t*100) if np3_t>0 else 0

    return {"targets":{"FOOD":{"target":food_total_t,"achievement":food_total_a},
        "BEVERAGE":{"target":bev_total_t,"achievement":bev_total_a},
        "NESTLE":{"target":nes_total_t,"achievement":nes_total_a}},
        "nestle_areas":[{"area":"NP-1","sales":"Ridwan","target":rint(np1_t),"achievement":rint(np1_ach),"pct":np1_pct},
            {"area":"NP-2","sales":"Redi","target":rint(np2_t),"achievement":rint(np2_ach),"pct":np2_pct},
            {"area":"NP-3","sales":"Gek Mas","target":rint(np3_t),"achievement":rint(np3_final),"pct":np3_pct}],
        "area_targets":area_targets,"balian":balian_rows}

def norm_name(f): return f.lower().replace(" ","_")

def extract_date(name):
    m=re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})",name)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    MONTHS={"jan":"01","feb":"02","mar":"03","apr":"04","mei":"05","may":"05",
        "jun":"06","jul":"07","agu":"08","aug":"08","sep":"09","okt":"10","oct":"10","nov":"11","des":"12","dec":"12"}
    m=re.search(r"(\d{1,2})[\s_-]?([a-z]{3})[\s_-]?(\d{4})",name.lower())
    if m:
        mon=MONTHS.get(m.group(2))
        if mon: return f"{m.group(3)}-{mon}-{int(m.group(1)):02d}"
    return None

def find_uploads():
    files=list(UPLOADS_DIR.glob("*.xlsx"))
    if not files: print(f"ERROR: No .xlsx files in {UPLOADS_DIR}");sys.exit(1)
    so_file=stk_mku=stk_mks=del_mku=del_mks=pencapaian=None;date_str=None
    for f in files:
        n=norm_name(f.name)
        if "data_pencapaian" in n: pencapaian=f
        elif "report_so_mku_mks" in n or "report so mku mks" in f.name.lower():
            so_file=f;date_str=extract_date(f.name)
        elif re.search(r"stok.?mku",n): stk_mku=f
        elif re.search(r"stok.?mks",n): stk_mks=f
        elif re.match(r"mku",n) and "stok" not in n: del_mku=f
        elif re.match(r"mks",n) and "stok" not in n: del_mks=f
    missing=[x for x,y in [("Report SO",so_file),("Stok MKU",stk_mku),("Stok MKS",stk_mks),
        ("MKU delivery",del_mku),("MKS delivery",del_mks),("DATA_PENCAPAIAN",pencapaian)] if not y]
    if missing: print("ERROR: Missing:",missing);sys.exit(1)
    if not date_str: date_str=datetime.today().strftime("%Y-%m-%d");print(f"WARNING: Using today {date_str}")
    print(f"Date: {date_str} | SO:{so_file.name} | Pencapaian:{pencapaian.name}")
    return date_str,so_file,stk_mku,stk_mks,del_mku,del_mks,pencapaian

def load_existing():
    if not DATA_JS.exists(): print("WARNING: Starting fresh.");return None
    content=DATA_JS.read_text(encoding="utf-8").strip()
    if content.startswith("const RAW ="): content=content[len("const RAW ="):].strip()
    if content.endswith(";"): content=content[:-1]
    try:
        raw=json.loads(content)
        # Migrate old flat structure to new multi-month structure
        if "months" not in raw:
            print("Migrating to multi-month structure...")
            month_key=raw["latest"][:7]  # e.g. "2026-04"
            dt=datetime.strptime(raw["latest"],"%Y-%m-%d")
            label=dt.strftime("%B %Y")
            raw={"latest":raw["latest"],"so":raw.get("so",[]),
                "months":{month_key:{"label":label,"dates":raw.get("dates",[]),
                    "so_summary":raw.get("so_summary",{}),"stock_by_date":raw.get("stock_by_date",{}),
                    "delivery_by_date":raw.get("delivery_by_date",{}),"targets_by_date":raw.get("targets_by_date",{})}}}
        return raw
    except json.JSONDecodeError as e:
        print(f"ERROR: {e}");sys.exit(1)

def main():
    print("="*60);print("MKU & MKS Dashboard Pipeline (Multi-Month)");print("="*60)
    date_str,so_file,stk_mku,stk_mks,del_mku,del_mks,pencapaian=find_uploads()
    dt=datetime.strptime(date_str,"%Y-%m-%d")
    month_key=date_str[:7]  # e.g. "2026-05"
    month_label=dt.strftime("%B %Y")

    raw=load_existing()
    if raw is None:
        raw={"latest":None,"so":[],"months":{}}

    # Ensure this month exists
    if month_key not in raw["months"]:
        print(f"\nNew month detected: {month_label}")
        raw["months"][month_key]={"label":month_label,"dates":[],
            "so_summary":{},"stock_by_date":{},"delivery_by_date":{},"targets_by_date":{}}

    m=raw["months"][month_key]

    # Compress previous latest day if same month
    prev=raw.get("latest")
    if prev and prev!=date_str and prev[:7]==month_key:
        print(f"\nCompressing {prev}...")
        m["so_summary"][prev]=compress_so(raw.get("so",[]))
        prev_stk=m["stock_by_date"].get(prev,{})
        m["stock_by_date"][prev]=compress_stock(
            prev_stk.get("MKU_full",prev_stk.get("MKU",[])),
            prev_stk.get("MKS_full",prev_stk.get("MKS",[])))
        prev_del=m["delivery_by_date"].get(prev,{})
        m["delivery_by_date"][prev]=compress_del(prev_del.get("mku_full",[]),prev_del.get("mks_full",[]))

    # Parse files
    print(f"\nParsing {date_str}...")
    so_rows=parse_so(so_file,date_str);print(f"  SO: {len(so_rows)} rows")
    mku_stock=parse_stock(stk_mku);print(f"  Stok MKU: {len(mku_stock)}")
    mks_stock=parse_stock(stk_mks);print(f"  Stok MKS: {len(mks_stock)}")
    del_mku_rows=parse_delivery(del_mku);print(f"  Del MKU: {len(del_mku_rows)}")
    del_mks_rows=parse_delivery(del_mks);print(f"  Del MKS: {len(del_mks_rows)}")
    targets_entry=parse_targets(pencapaian,date_str)
    t=targets_entry["targets"]
    print(f"  FOOD: {t['FOOD']['achievement']:,.0f} / {t['FOOD']['target']:,.0f}")
    print(f"  BEV:  {t['BEVERAGE']['achievement']:,.0f} / {t['BEVERAGE']['target']:,.0f}")
    print(f"  NES:  {t['NESTLE']['achievement']:,.0f} / {t['NESTLE']['target']:,.0f}")

    # Update
    raw["latest"]=date_str;raw["so"]=so_rows
    m["so_summary"][date_str]=compress_so(so_rows)
    stk=compress_stock(mku_stock,mks_stock);stk["MKU_full"]=mku_stock;stk["MKS_full"]=mks_stock
    m["stock_by_date"][date_str]=stk
    dl=compress_del(del_mku_rows,del_mks_rows);dl["mku_full"]=del_mku_rows;dl["mks_full"]=del_mks_rows
    m["delivery_by_date"][date_str]=dl
    m["targets_by_date"][date_str]=targets_entry
    if date_str not in m["dates"]: m["dates"]=sorted(m["dates"]+[date_str])

    # Write data.js
    DATA_JS.parent.mkdir(parents=True,exist_ok=True)
    output="const RAW = "+json.dumps(raw,ensure_ascii=False,separators=(",",":"))+  ";"
    DATA_JS.write_text(output,encoding="utf-8")
    print(f"\n✓ data.js written ({DATA_JS.stat().st_size/1024:.1f} KB)")
    print(f"  Months: {list(raw['months'].keys())}")
    print(f"  {month_key} dates: {m['dates']}")

    # Write data_sales.js
    sales_raw={"latest":date_str,"month":month_label,
        "targets_by_date":{date_str:targets_entry},
        "stock_by_date":{date_str:{"MKU_full":mku_stock,"MKS_full":mks_stock,"summary":stk["summary"]}},
        "so":so_rows}
    DATA_SALES_JS.write_text("const RAW = "+json.dumps(sales_raw,ensure_ascii=False,separators=(",",":"))+";",encoding="utf-8")
    print(f"✓ data_sales.js written ({DATA_SALES_JS.stat().st_size/1024:.1f} KB)")
    print("\nPipeline complete. ✓")

if __name__=="__main__":
    main()
