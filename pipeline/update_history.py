#!/usr/bin/env python3
"""
MKU & MKS — History Pipeline
Reads data_penjualan_global_[year].xlsx from uploads/
Generates docs/customers.js for the Sales App
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed.")
    sys.exit(1)

REPO_ROOT     = Path(__file__).parent.parent
UPLOADS_DIR   = REPO_ROOT / "uploads"
CUSTOMERS_JS  = REPO_ROOT / "docs" / "customers.js"

MONTH_ORDER = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

SALES_MAP = {
    "I MADE LUIH":"I Made Luih","I MADE LUIH (SANUR)":"I Made Luih",
    "I Made Luih (Sanur)":"I Made Luih","NN MADE LUIH":"I Made Luih",
    "NN Made Luih":"I Made Luih",
    "LANI":"Lani","LANI P":"Lani","Lani P":"Lani",
    "NI KETUT SRIASIH (GT)":"Sriasih (GT)","Ni Ketut Sriasih (GT)":"Sriasih (GT)",
    "NI KETUT SRIASIH (MT)":"Sriasih (MT)","Ni Ketut Sriasih (MT)":"Sriasih (MT)",
    "SRIASIH":"Sriasih (MT)",
    "PICROM STALYON":"Picrom","Picrom Stalyon":"Picrom","PICROM":"Picrom",
    "SALES RETAIL":"Sales Retail","JUNI":"Juni","MONICA":"Monica",
    "SUJANA":"Sujana","NN SUJANA":"Sujana","NN Sujana":"Sujana",
    "EKA":"Eka","TAUFIK":"Taufik","DEWI KRISTIANI":"Dewi Kristiani",
    "WIRA":"Wira","RIDWAN":"Ridwan","REDI":"Redi","GEK MAS":"Gek Mas",
    "NP1":"Ridwan","NP 1":"Ridwan","NP2":"Redi","NP 2":"Redi",
    "NP3":"Gek Mas","NP 3":"Gek Mas","NP4":"Gek Mas","NP5":"Gek Mas",
    "KA":"KA","Management Bali":"Management Bali","MANAGEMENT BALI":"Management Bali",
}

def norm_sales(s):
    if not s or str(s).strip().lower() in ("","nan"): return "Unknown"
    return SALES_MAP.get(str(s).strip(), str(s).strip())

def fval(v):
    try: f=float(v); return 0.0 if f!=f else f
    except: return 0.0

def find_global_file():
    files = list(UPLOADS_DIR.glob("data_penjualan_global*.xlsx"))
    if not files:
        print("ERROR: No data_penjualan_global*.xlsx found in uploads/")
        sys.exit(1)
    f = sorted(files)[-1]
    print(f"Found: {f.name}")
    return f

def main():
    print("="*60)
    print("MKU & MKS — History Pipeline")
    print("="*60)

    path = find_global_file()
    print("Reading file...")
    df = pd.read_excel(path, header=0)
    df.columns = [c.strip() for c in df.columns]
    print(f"  {len(df)} rows loaded")

    # Normalize
    df["sales_norm"] = df["Sales"].apply(norm_sales)
    df["revenue"] = df["Grand Total"].apply(fval)
    df["qty"] = df["Qty"].apply(fval)
    df["month"] = df["nama bulan"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["cust_code"] = df["Kode_Cust"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["cust_name"] = df["Nama Cust"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["cust_group"] = df["Grup Cust"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["area"] = df["Nama_Wil"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["area_code"] = df["Kode_Wil"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["prod_code"] = df["Kode_Brg"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["prod_name"] = df["Nama Brg"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["category"] = df["Kategori Barang"].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df["division"] = df["Nama_Div"].apply(lambda x: str(x).strip() if pd.notna(x) else "")

    months_present = sorted([m for m in df["month"].unique() if m],
                            key=lambda x: MONTH_ORDER.index(x) if x in MONTH_ORDER else 99)

    # ── Feature 1 & 2: Customer profiles per rep ─────────────────────────────
    print("Building customer profiles...")
    by_rep = {}
    for rep, rep_df in df.groupby("sales_norm"):
        customers = {}
        for ccode, c_df in rep_df.groupby("cust_code"):
            if not ccode or ccode == "nan": continue
            monthly = {}
            for mon, m_df in c_df.groupby("month"):
                monthly[mon] = int(m_df["revenue"].sum())

            # Top products for this customer
            prod_agg = c_df.groupby(["prod_code","prod_name","month"]).agg(
                qty=("qty","sum"), value=("revenue","sum")
            ).reset_index()
            products = []
            for _, pr in prod_agg.iterrows():
                if pr["value"] > 0:
                    products.append({
                        "code": pr["prod_code"],
                        "name": pr["prod_name"],
                        "month": pr["month"],
                        "qty": round(pr["qty"],2),
                        "value": int(pr["value"])
                    })
            products.sort(key=lambda x: -x["value"])

            last_month = max((m for m in monthly if monthly[m]>0),
                           key=lambda x: MONTH_ORDER.index(x) if x in MONTH_ORDER else 0,
                           default="")

            customers[ccode] = {
                "name": c_df["cust_name"].iloc[0],
                "group": c_df["cust_group"].iloc[0],
                "area": c_df["area"].iloc[0],
                "area_code": c_df["area_code"].iloc[0],
                "division": c_df["division"].iloc[0],
                "total": int(c_df["revenue"].sum()),
                "last_month": last_month,
                "monthly": monthly,
                "products": products[:50],  # top 50 products
            }

        # Sort customers by total spend
        sorted_custs = dict(sorted(customers.items(), key=lambda x: -x[1]["total"]))
        by_rep[rep] = {"customers": sorted_custs}

    # ── Feature 3: Area performance ───────────────────────────────────────────
    print("Building area performance...")
    areas = {}
    for (acode, aname), a_df in df.groupby(["area_code","area"]):
        monthly = {}
        for mon, m_df in a_df.groupby("month"):
            monthly[mon] = int(m_df["revenue"].sum())
        areas[acode] = {
            "name": aname,
            "total": int(a_df["revenue"].sum()),
            "monthly": monthly,
            "division": a_df["division"].iloc[0] if len(a_df) else "",
        }

    # ── Feature 4: Product performance ───────────────────────────────────────
    print("Building product performance...")
    prod_agg = df.groupby(["prod_code","prod_name","category"]).agg(
        total_qty=("qty","sum"),
        total_value=("revenue","sum")
    ).reset_index()
    prod_monthly = df.groupby(["prod_code","month"])["revenue"].sum().reset_index()
    pm_dict = {}
    for _, r in prod_monthly.iterrows():
        pm_dict.setdefault(r["prod_code"], {})[r["month"]] = int(r["revenue"])

    products = []
    for _, r in prod_agg.iterrows():
        if r["total_value"] > 0:
            products.append({
                "code": r["prod_code"],
                "name": r["prod_name"],
                "category": r["category"],
                "total_qty": round(r["total_qty"],2),
                "total_value": int(r["total_value"]),
                "monthly": pm_dict.get(r["prod_code"],{}),
            })
    products.sort(key=lambda x: -x["total_value"])

    # ── Feature 5: Customer segment breakdown ─────────────────────────────────
    print("Building segment breakdown...")
    segments = {}
    for seg, s_df in df.groupby("cust_group"):
        monthly = {}
        for mon, m_df in s_df.groupby("month"):
            monthly[mon] = int(m_df["revenue"].sum())
        # Top customers in segment
        top_custs = s_df.groupby(["cust_code","cust_name"])["revenue"].sum().reset_index()
        top_custs = top_custs.sort_values("revenue", ascending=False).head(10)
        segments[seg] = {
            "total": int(s_df["revenue"].sum()),
            "cust_count": s_df["cust_code"].nunique(),
            "monthly": monthly,
            "top_customers": [
                {"code":r["cust_code"],"name":r["cust_name"],"value":int(r["revenue"])}
                for _,r in top_custs.iterrows()
            ]
        }

    # ── Feature 6: Month-on-month overall ────────────────────────────────────
    print("Building monthly trends...")
    monthly_total = {}
    for mon, m_df in df.groupby("month"):
        monthly_total[mon] = {
            "total": int(m_df["revenue"].sum()),
            "orders": int(m_df["cust_code"].count()),
            "customers": int(m_df["cust_code"].nunique()),
            "by_division": {
                div: int(d_df["revenue"].sum())
                for div, d_df in m_df.groupby("division")
            }
        }

    # Monthly by rep
    monthly_by_rep = {}
    for rep, rep_df in df.groupby("sales_norm"):
        monthly_by_rep[rep] = {}
        for mon, m_df in rep_df.groupby("month"):
            monthly_by_rep[rep][mon] = int(m_df["revenue"].sum())

    # ── Assemble ──────────────────────────────────────────────────────────────
    output = {
        "generated": datetime.today().strftime("%Y-%m-%d"),
        "year": int(df["tahun"].iloc[0]) if "tahun" in df.columns else 2026,
        "months": months_present,
        "by_rep": by_rep,
        "areas": areas,
        "products": products[:200],  # top 200 products
        "segments": segments,
        "monthly_total": monthly_total,
        "monthly_by_rep": monthly_by_rep,
    }

    CUSTOMERS_JS.parent.mkdir(parents=True, exist_ok=True)
    out_str = "const CUSTOMERS = " + json.dumps(output, ensure_ascii=False, separators=(",",":")) + ";"
    CUSTOMERS_JS.write_text(out_str, encoding="utf-8")
    size_kb = CUSTOMERS_JS.stat().st_size / 1024
    print(f"\n✓ customers.js written ({size_kb:.1f} KB)")
    print(f"  Reps: {len(by_rep)}")
    print(f"  Areas: {len(areas)}")
    print(f"  Products: {len(products)}")
    print(f"  Segments: {len(segments)}")
    print(f"  Months: {months_present}")
    print("\nHistory pipeline complete. ✓")

if __name__ == "__main__":
    main()
