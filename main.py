from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import re, math, json, glob, os
from typing import Optional, List

app = FastAPI(title="Housing Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MONTH_ORDER = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
               "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}

def _clean(obj):
    if isinstance(obj, list):  return [_clean(i) for i in obj]
    if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def safe_json(data):
    return JSONResponse(content=_clean(data))

# ── load & enrich ──────────────────────────────────────────────────────────
# Auto-discover all .xlsx files in the data/ folder (drop any into data/ to add)
# Tries multiple base directories so it works regardless of where uvicorn is launched from
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CWD        = os.getcwd()
_CANDIDATES = [
    os.path.join(_SCRIPT_DIR, "data"),   # relative to main.py (most reliable)
    os.path.join(_CWD,        "data"),   # relative to working dir
    _SCRIPT_DIR,                          # legacy: xlsx files next to main.py
    _CWD,                                 # legacy: xlsx files in working dir
]
_xlsx_files = []
_DATA_DIR   = None
for _candidate in _CANDIDATES:
    _found = sorted(glob.glob(os.path.join(_candidate, "*.xlsx")))
    if _found:
        _xlsx_files = _found
        _DATA_DIR   = _candidate
        break

if not _xlsx_files:
    raise RuntimeError(
        f"No .xlsx files found. Tried: {_CANDIDATES}\n"
        f"Create a 'data/' folder next to main.py and drop your .xlsx files there."
    )

print(f"[data] Data dir : {_DATA_DIR}")
print(f"[data] Loading  : {[os.path.basename(f) for f in _xlsx_files]}")
_raw = pd.concat([pd.read_excel(f) for f in _xlsx_files], ignore_index=True)
print(f"[data] Rows: {len(_raw):,}  |  provinces: {sorted(_raw['province'].dropna().unique().tolist())}")

# Canonical snapshot label: "Feb 2026"
_raw["period"] = _raw["Month"].astype(str) + " " + _raw["Year"].astype(str)
_raw["period_ord"] = _raw["Month"].map(MONTH_ORDER) + (_raw["Year"].astype(int)-2000)*100

# Normalise municipality: take last segment after comma  e.g. "La Conarda, Betera" -> "Betera"
def _clean_municipality(s):
    if pd.isna(s): return s
    s = str(s).strip()
    return s.split(",")[-1].strip() if "," in s else s

_raw["municipality"] = _raw["municipality"].apply(_clean_municipality)

# Normalise province to Title Case  e.g. "alicante" -> "Alicante", "Valencia" stays "Valencia"
_raw["province"] = _raw["province"].apply(lambda s: str(s).strip().title() if pd.notna(s) else s)

# Full df (with duplicates) used only for snapshot-aware queries; deduplicated per sub+period for unit data
_full = _raw.copy()
df = _raw.drop_duplicates(subset=["sub_listing_id","period"]).copy()

PERIODS_SORTED = sorted(df["period"].unique(), key=lambda p: df[df["period"]==p]["period_ord"].iloc[0])
LATEST_PERIOD  = PERIODS_SORTED[-1]
PREV_PERIOD    = PERIODS_SORTED[-2] if len(PERIODS_SORTED) > 1 else None

# Per-province latest period (provinces may have different update cadences)
_prov_latest = (
    df.groupby("province")["period_ord"]
    .max()
    .reset_index()
    .rename(columns={"period_ord": "max_ord"})
)
_prov_latest = dict(zip(_prov_latest["province"], _prov_latest["max_ord"]))

# Mark each row: is it the latest snapshot for its province?
df["_is_latest"] = df.apply(lambda r: r["period_ord"] == _prov_latest.get(r["province"], -1), axis=1)
_full["_is_latest"] = _full.apply(lambda r: r["period_ord"] == _prov_latest.get(r["province"], -1), axis=1)

def _latest_df(df_src=None):
    """Return rows that represent the latest period for each province."""
    d = df_src if df_src is not None else df
    return d[d["_is_latest"]]

def _year(s):
    if pd.isna(s): return None
    m = re.search(r"(\d{4})", str(s))
    return int(m.group(1)) if m else None

def _quarter(s):
    if pd.isna(s): return None
    s2, year = str(s).lower(), _year(s)
    if year is None: return None
    if "immediate" in s2: return f"{year} Q1"
    for q, kw in [("Q1",["january","february","march","first quarter","first semester"]),
                  ("Q2",["april","may","june","second quarter","second semester"]),
                  ("Q3",["july","august","september","third quarter"]),
                  ("Q4",["october","november","december","fourth quarter"])]:
        if any(k in s2 for k in kw): return f"{year} {q}"
    return f"{year} Q2"

def _esg(s):
    if pd.isna(s): return None
    m = re.search(r"Consumption: ([A-E])", str(s))
    return m.group(1) if m else None

def _floor_num(f):
    if pd.isna(f) or str(f).strip() in ["-","ext."]: return None
    if "ground" in str(f).lower(): return 0
    m = re.search(r"Floor (\d+)", str(f), re.I)
    return int(m.group(1)) if m else None

def _parse_amenities(s):
    if pd.isna(s): s = ""
    s = str(s)
    m_bed  = re.search(r"(\d+)\s+bedroom", s)
    m_bath = re.search(r"(\d+)\s+bathroom", s)
    m_fa   = re.search(r"(\d+)\s+m.*?floor area", s)
    return {"bedrooms": int(m_bed.group(1)) if m_bed else (0 if "No bedroom" in s else None),
            "bathrooms": int(m_bath.group(1)) if m_bath else None,
            "floor_area_m2": int(m_fa.group(1)) if m_fa else None,
            "has_terrace":  "Terrace" in s, "has_parking": "Parking" in s,
            "has_pool": "Swimming pool" in s, "has_garden": "Garden" in s,
            "has_lift": "lift" in s.lower(), "has_ac": "Air conditioning" in s,
            "has_storage": "Storage room" in s, "has_wardrobes": "wardrobe" in s.lower()}

for _d in [df, _full]:
    _d["delivery_year"]    = _d["delivery_date"].apply(_year)
    _d["delivery_quarter"] = _d["delivery_date"].apply(_quarter)
    _d["esg_grade"]        = _d["esg_certificate"].apply(_esg)
    _d["floor_num"]        = _d["floor"].apply(_floor_num)
    _am = _d["amenities"].apply(_parse_amenities).apply(pd.Series)
    for col in _am.columns:
        _d[col] = _am[col]

def _filter(municipality=None, unit_type=None, year=None, esg=None, period=None, province=None, df_src=None):
    base = df_src if df_src is not None else df
    # When no period specified, use per-province latest (so all provinces show even if not in sync)
    if period:
        d = base[base["period"].isin(period)].copy()
    else:
        d = _latest_df(base).copy()
    if province:     d = d[d["province"].isin(province)]
    if municipality: d = d[d["municipality"].isin(municipality)]
    if unit_type:    d = d[d["unit_type"].isin(unit_type)]
    if year:         d = d[d["delivery_year"].isin([int(y) for y in year])]
    if esg:          d = d[d["esg_grade"].isin(esg)]
    return d

# ══════════════════════════════════════════════════════════════════════════
#  META
# ══════════════════════════════════════════════════════════════════════════
@app.get("/data-sources")
def get_data_sources():
    """Returns list of loaded data files and row counts per province."""
    sources = []
    for f in _xlsx_files:
        name = os.path.basename(f)
        province_name = name.replace("_all_units.xlsx","").replace("_"," ").title()
        count = int(df[df["province"] == province_name.lower()]["sub_listing_id"].nunique()) \
                if province_name.lower() in df["province"].str.lower().values else None
        sources.append({"file": name, "province": province_name})
    return safe_json({"sources": sources, "total_rows": len(df), "provinces": sorted(df["province"].dropna().unique().tolist())})


@app.get("/filters")
def get_filters():
    # Build province -> municipalities mapping
    province_munis = {}
    for _, row in df[["province","municipality"]].drop_duplicates().iterrows():
        p = str(row["province"]) if pd.notna(row["province"]) else "Other"
        m = str(row["municipality"]) if pd.notna(row["municipality"]) else None
        if m:
            province_munis.setdefault(p, [])
            if m not in province_munis[p]:
                province_munis[p].append(m)
    for p in province_munis:
        province_munis[p] = sorted(province_munis[p])

    return {"municipalities": sorted(df["municipality"].dropna().unique().tolist()),
            "provinces":      sorted(df["province"].dropna().unique().tolist()),
            "province_munis": province_munis,
            "unit_types":     sorted(df["unit_type"].dropna().unique().tolist()),
            "delivery_years": sorted([int(y) for y in df["delivery_year"].dropna().unique()]),
            "esg_grades":     sorted(df["esg_grade"].dropna().unique().tolist()),
            "periods":        PERIODS_SORTED,
            "latest_period":  LATEST_PERIOD,
            "prev_period":    PREV_PERIOD}

# ══════════════════════════════════════════════════════════════════════════
#  SUMMARY / SNAPSHOT  (latest period by default)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/stats")
def get_stats(municipality: Optional[List[str]] = Query(None),
              province:     Optional[List[str]] = Query(None),
              unit_type:    Optional[List[str]] = Query(None),
              year:         Optional[List[str]] = Query(None),
              esg:          Optional[List[str]] = Query(None),
              period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    p = _filter(municipality, unit_type, year, esg, [PREV_PERIOD], province) if PREV_PERIOD else None
    def _s(d): return {"total_units": len(d),
                       "avg_price":    round(float(d["price"].mean()))   if len(d) else 0,
                       "avg_price_m2": round(float(d["price_per_m2"].mean()),1) if len(d) else 0,
                       "avg_size":     round(float(d["size"].mean()),1)  if len(d) else 0,
                       "total_developments": int(d["listing_id"].nunique())}
    cur = _s(d)
    cur["prev"] = _s(p) if p is not None else None
    return cur

@app.get("/charts/price-by-unit-type")
def price_by_unit_type(municipality: Optional[List[str]] = Query(None),
                       province:     Optional[List[str]] = Query(None),
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None),
                       period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    r = d.groupby("unit_type").agg(avg_price=("price","mean"), count=("price","count"), avg_size=("size","mean"), avg_price_m2=("price_per_m2","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0); r["avg_size"] = r["avg_size"].round(1); r["avg_price_m2"] = r["avg_price_m2"].round(0)
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    r["_s"] = r["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    return safe_json(r.sort_values("_s").drop("_s",axis=1).to_dict(orient="records"))

@app.get("/charts/delivery-timeline")
def delivery_timeline(municipality: Optional[List[str]] = Query(None),
                      province:     Optional[List[str]] = Query(None),
                      unit_type:    Optional[List[str]] = Query(None),
                      year:         Optional[List[str]] = Query(None),
                      esg:          Optional[List[str]] = Query(None),
                      period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province).dropna(subset=["delivery_quarter"])
    r = d.groupby("delivery_quarter").agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.sort_values("delivery_quarter").to_dict(orient="records"))

@app.get("/charts/price-distribution")
def price_distribution(municipality: Optional[List[str]] = Query(None),
                       province:     Optional[List[str]] = Query(None),
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None),
                       period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = d.copy(); d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    result = d2.groupby("bin", observed=True).agg(count=("price","size"), avg_price_m2=("price_per_m2","mean")).reset_index()
    result["avg_price_m2"] = result["avg_price_m2"].round(0)
    return safe_json(result.to_dict(orient="records"))

@app.get("/charts/municipality-overview")
def municipality_overview(municipality: Optional[List[str]] = Query(None),
                          province:     Optional[List[str]] = Query(None),
                          unit_type:    Optional[List[str]] = Query(None),
                          year:         Optional[List[str]] = Query(None),
                          esg:          Optional[List[str]] = Query(None),
                          period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    r = d.groupby("municipality").agg(units=("price","count"), listings=("listing_id","nunique"),
                                      avg_price=("price","mean"), avg_price_m2=("price_per_m2","mean")).reset_index()
    r["avg_price"]    = r["avg_price"].round(0)
    r["avg_price_m2"] = r["avg_price_m2"].round(1)
    return safe_json(r.sort_values("units", ascending=False).to_dict(orient="records"))

@app.get("/charts/esg-breakdown")
def esg_breakdown(municipality: Optional[List[str]] = Query(None),
                  province:     Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None),
                  period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    r = d.groupby("esg_grade", dropna=False).agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["esg_grade"] = r["esg_grade"].fillna("Unknown")
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.to_dict(orient="records"))

@app.get("/charts/size-vs-price")
def size_vs_price(municipality: Optional[List[str]] = Query(None),
                  province:     Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None),
                  period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period, province)
    cols = ["sub_listing_id","listing_id","size","price","price_per_m2",
            "unit_type","municipality","city_area","property_name","floor",
            "bedrooms","unit_url"]
    d2 = d[[c for c in cols if c in d.columns]].dropna(subset=["size","price"])
    if len(d2) > 800: d2 = d2.sample(800, random_state=42)
    rows = []
    for _, r in d2.iterrows():
        lat, lng = _listing_coords(r["listing_id"], str(r["municipality"]) if pd.notna(r.get("municipality")) else "")
        row = {c: r[c] for c in d2.columns}
        row["lat"] = lat or 39.47
        row["lng"] = lng or -0.38
        for k in list(row.keys()):
            v = row[k]
            if hasattr(v, 'item'): row[k] = v.item()
            if isinstance(v, float) and v != v: row[k] = None
        rows.append(row)
    return safe_json(rows)

# ══════════════════════════════════════════════════════════════════════════
#  TEMPORAL  — market-wide month-over-month
# ══════════════════════════════════════════════════════════════════════════
@app.get("/temporal/market-trend")
def market_trend(municipality: Optional[List[str]] = Query(None),
                 province:     Optional[List[str]] = Query(None),
                 unit_type:    Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, province=province)
    r = d.groupby(["period","period_ord"]).agg(
        avg_price    =("price","mean"),
        avg_price_m2 =("price_per_m2","mean"),
        total_units  =("sub_listing_id","nunique"),
        avg_size     =("size","mean"),
    ).reset_index().sort_values("period_ord")
    r["avg_price"]    = r["avg_price"].round(0)
    r["avg_price_m2"] = r["avg_price_m2"].round(1)
    r["avg_size"]     = r["avg_size"].round(1)
    return safe_json(r.drop("period_ord",axis=1).to_dict(orient="records"))

@app.get("/temporal/unit-type-trend")
def unit_type_trend(municipality: Optional[List[str]] = Query(None),
                    province:     Optional[List[str]] = Query(None)):
    d = _filter(municipality, province=province)
    r = d.groupby(["period","period_ord","unit_type"]).agg(
        avg_price=("price","mean"), count=("sub_listing_id","nunique")
    ).reset_index().sort_values(["unit_type","period_ord"])
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.drop("period_ord",axis=1).to_dict(orient="records"))

@app.get("/temporal/municipality-trend")
def municipality_trend(municipality: Optional[List[str]] = Query(None),
                       province:     Optional[List[str]] = Query(None)):
    d = _filter(municipality, province=province) if municipality else df
    r = d.groupby(["municipality","period","period_ord"]).agg(
        avg_price    =("price","mean"),
        avg_price_m2 =("price_per_m2","mean"),
        units        =("sub_listing_id","nunique"),
    ).reset_index().sort_values(["municipality","period_ord"])
    r["avg_price"]    = r["avg_price"].round(0)
    r["avg_price_m2"] = r["avg_price_m2"].round(1)
    return safe_json(r.drop("period_ord",axis=1).to_dict(orient="records"))

@app.get("/temporal/inventory-trend")
def inventory_trend(municipality: Optional[List[str]] = Query(None),
                    province:     Optional[List[str]] = Query(None),
                    unit_type:    Optional[List[str]] = Query(None)):
    """Units available per period, plus new/removed vs prior period."""
    d = _filter(municipality, unit_type, province=province)
    result = []
    prev_ids = None
    for period in PERIODS_SORTED:
        ids = set(d[d["period"]==period]["sub_listing_id"].unique())
        new     = len(ids - prev_ids) if prev_ids is not None else 0
        removed = len(prev_ids - ids) if prev_ids is not None else 0
        result.append({"period":period, "total":len(ids), "new":new, "removed":removed})
        prev_ids = ids
    return result

# ══════════════════════════════════════════════════════════════════════════
#  DRILL-DOWN — municipality
# ══════════════════════════════════════════════════════════════════════════
@app.get("/drilldown/municipality/{municipality}")
def drilldown_municipality(municipality: str):
    d = df[df["municipality"] == municipality]
    if d.empty:
        return safe_json({"listings":[],"stats":{},"unit_type_mix":[],"price_dist":[],"trend":[]})

    # latest period snapshot for listings (use per-province latest)
    dl = d[d["_is_latest"]]
    listings_grp = dl.groupby(["listing_id","property_name","developer","delivery_date","esg_grade"], dropna=False).agg(
        units        =("sub_listing_id","nunique"),
        min_price    =("price","min"), max_price=("price","max"),
        avg_price    =("price","mean"), avg_price_m2=("price_per_m2","mean"),
        avg_size     =("size","mean"),
        unit_types   =("unit_type", lambda x: ", ".join(sorted(x.unique().tolist()))),
        has_pool=("has_pool","max"), has_parking=("has_parking","max"),
        has_terrace=("has_terrace","max"), has_lift=("has_lift","max"),
    ).reset_index()
    for c in ["avg_price","min_price","max_price"]:
        listings_grp[c] = listings_grp[c].round(0)
    listings_grp["avg_price_m2"] = listings_grp["avg_price_m2"].round(1)
    listings_grp["avg_size"]     = listings_grp["avg_size"].round(1)
    listings_grp["esg_grade"]    = listings_grp["esg_grade"].where(pd.notna(listings_grp["esg_grade"]), None)

    stats = {"total_units": int(dl["sub_listing_id"].nunique()),
             "total_listings": int(dl["listing_id"].nunique()),
             "avg_price":    round(float(dl["price"].mean())),
             "avg_price_m2": round(float(dl["price_per_m2"].mean()),1),
             "price_range":  [int(dl["price"].min()), int(dl["price"].max())]}

    mix = dl.groupby("unit_type").size().reset_index(name="count")
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    mix["_s"] = mix["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    mix = mix.sort_values("_s").drop("_s",axis=1)

    # Full per-type stats for summary table
    ut_stats = dl.groupby("unit_type").agg(
        count     =("sub_listing_id","nunique"),
        min_price =("price","min"),
        avg_price =("price","mean"),
        max_price =("price","max"),
        avg_size  =("size","mean"),
        avg_pm2   =("price_per_m2","mean"),
    ).reset_index()
    ut_stats["min_price"] = ut_stats["min_price"].round(0)
    ut_stats["avg_price"] = ut_stats["avg_price"].round(0)
    ut_stats["max_price"] = ut_stats["max_price"].round(0)
    ut_stats["avg_size"]  = ut_stats["avg_size"].round(1)
    ut_stats["avg_pm2"]   = ut_stats["avg_pm2"].round(0)
    ut_stats["_s"] = ut_stats["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    ut_stats = ut_stats.sort_values("_s").drop("_s",axis=1)

    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = dl.copy(); d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    price_dist_grp = d2.groupby("bin", observed=True).agg(count=("price","size"), avg_price_m2=("price_per_m2","mean")).reset_index()
    price_dist_grp["avg_price_m2"] = price_dist_grp["avg_price_m2"].round(0)
    price_dist = price_dist_grp

    # month-over-month trend for this municipality
    trend = d.groupby(["period","period_ord"]).agg(
        avg_price    =("price","mean"),
        avg_price_m2 =("price_per_m2","mean"),
        total_units  =("sub_listing_id","nunique"),
    ).reset_index().sort_values("period_ord")
    trend["avg_price"]    = trend["avg_price"].round(0)
    trend["avg_price_m2"] = trend["avg_price_m2"].round(1)

    return safe_json({"listings": listings_grp.to_dict(orient="records"),
                      "stats": stats,
                      "unit_type_mix": mix.to_dict(orient="records"),
                      "unit_type_stats": ut_stats.to_dict(orient="records"),
                      "price_dist": price_dist.to_dict(orient="records"),
                      "trend": trend.drop("period_ord",axis=1).to_dict(orient="records")})

# ══════════════════════════════════════════════════════════════════════════
#  DRILL-DOWN — listing (development)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/drilldown/listing/{listing_id}")
def drilldown_listing(listing_id: int):
    d = df[df["listing_id"] == listing_id]
    if d.empty: return safe_json({})

    meta = d[d["_is_latest"]].iloc[0] if not d[d["_is_latest"]].empty else d.iloc[0]
    dl   = d[d["_is_latest"]]

    apt_cols = ["sub_listing_id","unit_type","price","size","price_per_m2",
                "floor","floor_num","bedrooms","bathrooms","floor_area_m2",
                "has_terrace","has_parking","has_pool","has_garden",
                "has_lift","has_ac","has_storage","has_wardrobes","unit_url"]
    if "last_updated" in dl.columns: apt_cols = apt_cols + ["last_updated"]
    apts = dl[apt_cols].copy()
    for col in ["floor_num","bedrooms","bathrooms","floor_area_m2"]:
        apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")
    for col in ["has_terrace","has_parking","has_pool","has_garden","has_lift","has_ac","has_storage","has_wardrobes"]:
        apts[col] = apts[col].fillna(False).astype(bool)
    apts = apts.sort_values(["unit_type","price"])
    apt_records = _clean(apts.to_dict(orient="records"))
    apt_records = [{k:(None if str(v)=="<NA>" else v) for k,v in r.items()} for r in apt_records]
    # attach coords (same for all apts in same listing)
    lat, lng = _listing_coords(listing_id, str(meta["municipality"]) if pd.notna(meta.get("municipality")) else "")
    for r in apt_records:
        r["lat"] = lat or 39.47
        r["lng"] = lng or -0.38

    floor_price = dl.dropna(subset=["floor_num"])[["floor_num","price","unit_type","size","sub_listing_id"]].copy()
    floor_price["floor_num"] = floor_price["floor_num"].astype(int)

    unit_comp = dl.groupby("unit_type").agg(count=("price","count"), avg_price=("price","mean"),
        min_price=("price","min"), max_price=("price","max"),
        avg_size=("size","mean"), avg_price_m2=("price_per_m2","mean")).reset_index()
    for c in ["avg_price","min_price","max_price"]:
        unit_comp[c] = unit_comp[c].round(0)
    unit_comp["avg_price_m2"] = unit_comp["avg_price_m2"].round(1)
    unit_comp["avg_size"]     = unit_comp["avg_size"].round(1)
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    unit_comp["_s"] = unit_comp["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    unit_comp = unit_comp.sort_values("_s").drop("_s",axis=1)

    # ── LISTING TIME SERIES: avg price, inventory, unit mix per period ──────
    listing_trend = d.groupby(["period","period_ord"]).agg(
        avg_price    =("price","mean"),
        avg_price_m2 =("price_per_m2","mean"),
        total_units  =("sub_listing_id","nunique"),
        min_price    =("price","min"),
        max_price    =("price","max"),
    ).reset_index().sort_values("period_ord")
    listing_trend["avg_price"]    = listing_trend["avg_price"].round(0)
    listing_trend["avg_price_m2"] = listing_trend["avg_price_m2"].round(1)
    listing_trend["min_price"]    = listing_trend["min_price"].round(0)
    listing_trend["max_price"]    = listing_trend["max_price"].round(0)

    # per-unit-type trend
    ut_trend = d.groupby(["unit_type","period","period_ord"]).agg(
        avg_price=("price","mean"), count=("sub_listing_id","nunique")
    ).reset_index().sort_values(["unit_type","period_ord"])
    ut_trend["avg_price"] = ut_trend["avg_price"].round(0)

    # ── PER-APARTMENT TIME SERIES ─────────────────────────────────────────
    apt_trend = d.groupby(["sub_listing_id","period","period_ord"]).agg(
        price        =("price","first"),
        price_per_m2 =("price_per_m2","first"),
    ).reset_index().sort_values(["sub_listing_id","period_ord"])
    # attach metadata from latest record
    apt_meta = d.drop_duplicates("sub_listing_id")[["sub_listing_id","unit_type","floor","size","unit_url","bedrooms"]]
    apt_trend = apt_trend.merge(apt_meta, on="sub_listing_id", how="left")
    apt_trend["price_per_m2"] = apt_trend["price_per_m2"].round(1)
    apt_trend_records = _clean(apt_trend.drop("period_ord",axis=1).to_dict(orient="records"))

    return safe_json({
        "listing_id":    int(listing_id),
        "property_name": str(meta["property_name"]),
        "developer":     str(meta["developer"]),
        "municipality":  str(meta["municipality"]),
        "delivery_date": str(meta["delivery_date"]),
        "esg_grade":     str(meta["esg_grade"]) if pd.notna(meta["esg_grade"]) else None,
        "description":   str(meta["description"]) if pd.notna(meta.get("description")) else None,
        "total_units":   int(len(dl)),
        "periods":       PERIODS_SORTED,
        "apartments":    apt_records,
        "floor_price":   floor_price.to_dict(orient="records"),
        "unit_comparison": unit_comp.to_dict(orient="records"),
        "listing_trend": listing_trend.drop("period_ord",axis=1).to_dict(orient="records"),
        "unit_type_trend": ut_trend.drop("period_ord",axis=1).to_dict(orient="records"),
        "apt_trend":     apt_trend_records,
    })

# ══════════════════════════════════════════════════════════════════════════
#  APARTMENT PRICE MATRIX  — wide format, one row per apartment, col per period
# ══════════════════════════════════════════════════════════════════════════
@app.get("/drilldown/listing/{listing_id}/price-matrix")
def price_matrix(listing_id: int):
    d = df[df["listing_id"] == listing_id]
    if d.empty: return safe_json({"periods":[], "rows":[]})

    periods = PERIODS_SORTED  # use global sorted periods

    rows = []
    for sub_id, grp in d.groupby("sub_listing_id"):
        meta = grp.iloc[0]
        row = {
            "sub_listing_id": int(sub_id),
            "unit_type":  str(meta["unit_type"]),
            "floor":      str(meta["floor"]) if pd.notna(meta["floor"]) else "—",
            "size":       float(meta["size"])       if pd.notna(meta["size"])    else None,
            "bedrooms":   int(meta["bedrooms"])     if pd.notna(meta.get("bedrooms")) else None,
            "bathrooms":  int(meta["bathrooms"])    if pd.notna(meta.get("bathrooms")) else None,
            "has_terrace":bool(meta.get("has_terrace", False)),
            "has_parking":bool(meta.get("has_parking", False)),
            "has_pool":   bool(meta.get("has_pool",   False)),
            "has_lift":   bool(meta.get("has_lift",   False)),
            "has_ac":     bool(meta.get("has_ac",     False)),
            "has_storage":bool(meta.get("has_storage",False)),
            "unit_url":   str(meta["unit_url"])     if pd.notna(meta.get("unit_url")) else None,
        }

        price_vals = []
        for period in periods:
            pr = grp[grp["period"] == period]
            if len(pr):
                p  = int(pr["price"].iloc[0])
                m2 = round(float(pr["price_per_m2"].iloc[0]), 1)
                price_vals.append(p)
            else:
                p, m2 = None, None
            row[f"price_{period}"] = p
            row[f"ppm2_{period}"]  = m2

        # change vs first available period
        valid = [v for v in price_vals if v is not None]
        if len(valid) >= 2:
            row["price_change"]     = valid[-1] - valid[0]
            row["price_change_pct"] = round((valid[-1] - valid[0]) / valid[0] * 100, 2)
        else:
            row["price_change"]     = 0
            row["price_change_pct"] = 0.0

        # latest price for default sort
        row["latest_price"]    = valid[-1] if valid else None
        row["latest_ppm2"]     = row.get(f"ppm2_{periods[-1]}")
        rows.append(row)

    return safe_json({"periods": periods, "rows": rows})

# ══════════════════════════════════════════════════════════════════════════
#  GEOCOORDINATES  — municipality centroid lookup
# ══════════════════════════════════════════════════════════════════════════
MUNI_COORDS = {
    "Aiora, Valencia":(38.93,-0.97),"Alaquás":(39.46,-0.46),"Albal":(39.38,-0.43),
    "Albalat Dels Sorells":(39.55,-0.37),"Alberic":(39.12,-0.52),"Alcàsser":(39.40,-0.44),
    "Aldaia":(39.46,-0.48),"Alfafar":(39.41,-0.39),"Alfara de Baronia":(39.74,-0.28),
    "Alfara del Patriarca":(39.56,-0.39),"Algar de Palancia":(39.75,-0.34),
    "Algemesi":(39.18,-0.44),"Almardá, Sagunto/Sagunt":(39.68,-0.25),
    "Almàssera":(39.53,-0.36),"Antigua Moreria, Sagunto/Sagunt":(39.68,-0.27),
    "Arrancapins, Valencia":(39.47,-0.39),"Barranquet - El Salvador, Godella":(39.53,-0.42),
    "Barrio de Favara, Valencia":(39.44,-0.38),"Benaguasil":(39.59,-0.56),
    "Beneixida":(39.12,-0.58),"Benetusser":(39.42,-0.40),"Beniarjo":(38.93,-0.19),
    "Benicalap, Valencia":(39.50,-0.40),"Benimàmet":(39.49,-0.43),
    "Beniopa - San Pere, Gandia":(38.97,-0.18),"Beniparrell":(39.39,-0.42),
    "Benipeixcar - El Raval, Gandia":(38.98,-0.18),"Benisano":(39.60,-0.59),
    "Bonrepos i Mirambell":(39.55,-0.38),"Burjassot":(39.51,-0.41),
    "Catarroja":(39.40,-0.40),"Chiva":(39.47,-0.71),"Cullera":(39.16,-0.25),
    "Gandia":(38.97,-0.18),"Godella":(39.53,-0.42),"La Eliana":(39.57,-0.53),
    "L'Eliana":(39.57,-0.53),"Llíria":(39.62,-0.60),"Manises":(39.49,-0.46),
    "Massamagrell":(39.57,-0.34),"Mislata":(39.48,-0.42),"Moncada":(39.55,-0.40),
    "Montserrat":(39.35,-0.47),"Museros":(39.58,-0.36),"Náquera":(39.60,-0.41),
    "Paiporta":(39.42,-0.41),"Paterna":(39.50,-0.44),"Picanya":(39.43,-0.43),
    "Picassent":(39.36,-0.46),"Puçol":(39.62,-0.32),"Quart de Poblet":(39.47,-0.45),
    "Riba-roja de Túria":(39.56,-0.61),"Rocafort":(39.54,-0.41),"Sagunto":(39.68,-0.27),
    "Sedaví":(39.42,-0.39),"Silla":(39.36,-0.41),"Tavernes de la Valldigna":(39.07,-0.27),
    "Torrent":(39.43,-0.46),"Valencia":(39.47,-0.38),"Vilamarxant":(39.58,-0.65),
    "Xàtiva":(38.99,-0.52),"Xirivella":(39.46,-0.42),
    # Extra city-district entries
    "Campanar, Valencia":(39.49,-0.40),"Camins al Grau, Valencia":(39.47,-0.36),
    "El Pla del Real, Valencia":(39.48,-0.38),"Extramurs, Valencia":(39.47,-0.39),
    "La Saïdia, Valencia":(39.49,-0.37),"L'Eixample, Valencia":(39.47,-0.38),
    "Olivereta, Valencia":(39.46,-0.40),"Poblats Maritims, Valencia":(39.46,-0.33),
    "Quatre Carreres, Valencia":(39.44,-0.37),"Rascanya, Valencia":(39.50,-0.38),
    "Sant Marcel·lí, Valencia":(39.44,-0.39),"Tavernes Blanques":(39.53,-0.37),
    "Torrefiel, Valencia":(39.50,-0.38),"Zona Parc Central - Hort de Trenor, Torrent":(39.43,-0.47),
    "Natzaret, Valencia":(39.45,-0.34),"El Saler, Valencia":(39.35,-0.32),
    "Borbotó, Valencia":(39.54,-0.40),"Massarrojos, Valencia":(39.55,-0.39),
    "Castellar-l'Oliveral, Valencia":(39.41,-0.37),"Pobles del Nord, Valencia":(39.55,-0.38),
    "Benimaclet, Valencia":(39.49,-0.36),"Nou Moles, Valencia":(39.47,-0.40),
    "Fonteta de Sant Lluis, Valencia":(39.44,-0.39),"Jesús, Valencia":(39.45,-0.39),
    "Patraix, Valencia":(39.46,-0.40),"Sant Pau-Bon Pastor, Valencia":(39.48,-0.40),
    "Tormos, Valencia":(39.50,-0.38),"La Roqueta, Valencia":(39.47,-0.38),
}

def _get_coords(municipality):
    if not municipality: return None, None
    # exact match
    if municipality in MUNI_COORDS:
        return MUNI_COORDS[municipality]
    # try substring
    for k, v in MUNI_COORDS.items():
        if municipality.lower() in k.lower() or k.lower() in municipality.lower():
            return v
    return None, None

# Per-listing precise coordinates (pre-geocoded from city_area)
import os as _os
_LISTING_COORDS_PATH = _os.path.join(_os.path.dirname(__file__), "listing_coords.json")
try:
    with open(_LISTING_COORDS_PATH) as _f:
        LISTING_COORDS = {int(k): v for k, v in json.load(_f).items()}
except Exception:
    LISTING_COORDS = {}

def _listing_coords(listing_id, municipality=""):
    """Return (lat, lng) — per-listing precise coords first, then municipality fallback."""
    c = LISTING_COORDS.get(int(listing_id) if listing_id else -1)
    if c:
        return c["lat"], c["lng"]
    lat, lng = _get_coords(str(municipality))
    return (lat or 39.47), (lng or -0.38)

def _parse_address(city_area):
    """Return {street, municipality, comarca, province} from city_area string."""
    if pd.isna(city_area): return {}
    parts = [p.strip() for p in str(city_area).split(",")]
    if len(parts) < 2: return {"raw": str(city_area)}
    province = parts[-1]
    comarca  = parts[-2] if len(parts) >= 3 else None
    rest     = parts[:-2]
    import re as _re
    if rest and _re.match(r'^(Calle|Carrer|Avinguda|Avda|Avenida|Plaza|Plaça|Urb)', rest[0], _re.I):
        street = _re.sub(r' NN$', '', rest[0])
        muni   = rest[1] if len(rest) > 1 else None
    else:
        street = None
        muni   = rest[0] if rest else None
    return {"street": street, "municipality": muni, "comarca": comarca, "province": province}

def _comarca(city_area):
    d = _parse_address(city_area)
    return d.get("comarca")

# Add derived columns to df
for _d in [df]:
    _d["comarca"]      = _d["city_area"].apply(_comarca)
    _d["addr_street"]  = _d["city_area"].apply(lambda x: _parse_address(x).get("street"))
    _d["addr_comarca"] = _d["city_area"].apply(lambda x: _parse_address(x).get("comarca"))

# ══════════════════════════════════════════════════════════════════════════
#  MAP DATA  — all listings with coords
# ══════════════════════════════════════════════════════════════════════════
@app.get("/map/listings")
def map_listings(municipality: Optional[List[str]] = Query(None)):
    d = _latest_df()
    grp = d.groupby(["listing_id","property_name","municipality","city_area","comarca"], dropna=False).agg(
        units      = ("sub_listing_id","nunique"),
        avg_price  = ("price","mean"),
        min_price  = ("price","min"),
    ).reset_index()
    rows = []
    for _, r in grp.iterrows():
        lat, lng = _listing_coords(r["listing_id"], str(r["municipality"]) if pd.notna(r["municipality"]) else "")
        addr = _parse_address(r["city_area"])
        rows.append({
            "listing_id":  int(r["listing_id"]),
            "property_name": str(r["property_name"]),
            "municipality": str(r["municipality"]) if pd.notna(r["municipality"]) else "",
            "comarca":     str(r["comarca"])     if pd.notna(r["comarca"])     else "",
            "street":      addr.get("street","") or "",
            "lat": lat, "lng": lng,
            "units":     int(r["units"]),
            "avg_price": round(float(r["avg_price"])),
            "min_price": int(r["min_price"]),
        })
    return safe_json(rows)

# ══════════════════════════════════════════════════════════════════════════
#  NEARBY COMPARISON  — same comarca, different listings
# ══════════════════════════════════════════════════════════════════════════
@app.get("/nearby/listings/{listing_id}")
def nearby_listings(listing_id: int):
    base = df[df["listing_id"]==listing_id]
    if base.empty: return safe_json({"comarca":"","listings":[]})
    comarca = base["comarca"].iloc[0]
    if pd.isna(comarca): return safe_json({"comarca":"","listings":[]})

    # All listings in same comarca (latest period)
    d = df[(df["comarca"]==comarca) & df["_is_latest"]]
    grp = d.groupby(["listing_id","property_name","municipality","city_area","developer","delivery_date","esg_grade"], dropna=False).agg(
        units        = ("sub_listing_id","nunique"),
        avg_price    = ("price","mean"),
        min_price    = ("price","min"),
        max_price    = ("price","max"),
        avg_price_m2 = ("price_per_m2","mean"),
        avg_size     = ("size","mean"),
        unit_types   = ("unit_type", lambda x: ", ".join(sorted(set(x.dropna())))),
    ).reset_index()
    grp["avg_price"]    = grp["avg_price"].round(0)
    grp["avg_price_m2"] = grp["avg_price_m2"].round(1)
    grp["avg_size"]     = grp["avg_size"].round(1)
    grp["esg_grade"]    = grp["esg_grade"].where(pd.notna(grp["esg_grade"]), None)

    # Add coords
    rows = []
    for _, r in grp.iterrows():
        lat, lng = _listing_coords(r["listing_id"], str(r["municipality"]))
        addr = _parse_address(r["city_area"])
        rows.append({
            **{k: (_clean(r[k]) if not isinstance(r[k], (str,bool,type(None))) else r[k]) for k in r.index},
            "is_current": int(r["listing_id"]) == listing_id,
            "lat": lat or 39.47, "lng": lng or -0.38,
            "street": addr.get("street","") or "",
            "addr_comarca": str(comarca),
        })
    return safe_json({"comarca": str(comarca), "listings": rows})

@app.get("/nearby/apartments/{listing_id}")
def nearby_apartments(listing_id: int, unit_type: Optional[str] = None):
    """Compare apartments across nearby listings (same comarca)."""
    base = df[df["listing_id"]==listing_id]
    if base.empty: return safe_json({"comarca":"","apartments":[]})
    comarca = base["comarca"].iloc[0]
    if pd.isna(comarca): return safe_json({"comarca":"","apartments":[]})

    d = df[(df["comarca"]==comarca) & df["_is_latest"]].copy()
    if unit_type:
        d = d[d["unit_type"]==unit_type]

    apt_cols = ["sub_listing_id","listing_id","property_name","municipality","unit_type",
                "price","size","price_per_m2","floor","bedrooms","bathrooms",
                "has_terrace","has_parking","has_pool","has_lift","has_ac","unit_url","city_area"]
    if "last_updated" in d.columns: apt_cols = apt_cols + ["last_updated"]
    apts = d[apt_cols].drop_duplicates("sub_listing_id").copy()
    for col in ["bedrooms","bathrooms"]:
        apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")

    rows = []
    for _, r in apts.iterrows():
        lat, lng = _listing_coords(r["listing_id"], str(r["municipality"]))
        addr = _parse_address(r["city_area"])
        row = {c: r[c] for c in apt_cols}
        row["lat"] = lat or 39.47
        row["lng"] = lng or -0.38
        row["street"] = addr.get("street","") or ""
        row["is_current_listing"] = int(r["listing_id"]) == listing_id
        for k in row:
            if hasattr(row[k], 'item'):
                row[k] = row[k].item()
            if isinstance(row[k], float) and (row[k] != row[k]):
                row[k] = None
        rows.append(row)

    rows.sort(key=lambda r: r.get("price") or 999999)
    return safe_json({"comarca": str(comarca), "apartments": rows})

# ══════════════════════════════════════════════════════════════════════════
#  LISTING DETAIL — enhanced with address + coords
# ══════════════════════════════════════════════════════════════════════════
@app.get("/listing/meta/{listing_id}")
def listing_meta(listing_id: int):
    d = df[df["listing_id"]==listing_id]
    if d.empty: return safe_json({})
    r = d.iloc[0]
    lat, lng = _listing_coords(listing_id, str(r["municipality"]))
    addr = _parse_address(r["city_area"])
    return safe_json({
        "listing_id":   int(listing_id),
        "property_name":str(r["property_name"]),
        "municipality": str(r["municipality"]) if pd.notna(r["municipality"]) else "",
        "city_area":    str(r["city_area"])    if pd.notna(r["city_area"])    else "",
        "comarca":      str(r["comarca"])      if pd.notna(r["comarca"])      else "",
        "street":       addr.get("street","") or "",
        "lat": lat or 39.47, "lng": lng or -0.38,
    })

# ══════════════════════════════════════════════════════════════════════════
#  DELISTED — developments + apartments present in prev period but not latest
# ══════════════════════════════════════════════════════════════════════════
@app.get("/delisted/listings")
def delisted_listings(province: Optional[List[str]] = Query(None),
                      municipality: Optional[List[str]] = Query(None)):
    if not PREV_PERIOD:
        return safe_json({"listings": [], "summary": {}, "periods": {"prev": None, "latest": None}})

    d = df.copy()
    if province:     d = d[d["province"].isin(province)]
    if municipality: d = d[d["municipality"].isin(municipality)]

    prev_ids   = set(d[d["period"]==PREV_PERIOD]["listing_id"].unique())
    latest_ids = set(d[d["period"]==LATEST_PERIOD]["listing_id"].unique())
    delisted   = prev_ids - latest_ids

    if not delisted:
        return safe_json({"listings": [], "summary": {"count":0,"units":0}, "periods": {"prev":PREV_PERIOD,"latest":LATEST_PERIOD}})

    # Get data from the prev period for these listings
    dp = d[(d["listing_id"].isin(delisted)) & (d["period"]==PREV_PERIOD)]
    grp = dp.groupby(["listing_id","property_name","developer","municipality","city_area","esg_grade","delivery_date"], dropna=False).agg(
        units        =("sub_listing_id","nunique"),
        avg_price    =("price","mean"),
        min_price    =("price","min"),
        max_price    =("price","max"),
        avg_price_m2 =("price_per_m2","mean"),
        avg_size     =("size","mean"),
        unit_types   =("unit_type", lambda x: ", ".join(sorted(x.unique().tolist()))),
        has_pool     =("has_pool","max"),
        has_parking  =("has_parking","max"),
        has_terrace  =("has_terrace","max"),
        has_lift     =("has_lift","max"),
    ).reset_index()
    for c in ["avg_price","min_price","max_price"]:
        grp[c] = grp[c].round(0)
    grp["avg_price_m2"] = grp["avg_price_m2"].round(1)
    grp["avg_size"]     = grp["avg_size"].round(1)
    grp["esg_grade"]    = grp["esg_grade"].where(pd.notna(grp["esg_grade"]), None)

    # Attach coords
    records = grp.to_dict(orient="records")
    for r in records:
        lat, lng = _listing_coords(int(r["listing_id"]), str(r["municipality"]))
        r["lat"] = lat or 39.47
        r["lng"] = lng or -0.38

    summary = {
        "count": len(records),
        "units": int(dp["sub_listing_id"].nunique()),
        "avg_price": round(float(dp["price"].mean())) if len(dp) else 0,
        "avg_price_m2": round(float(dp["price_per_m2"].mean()), 1) if len(dp) else 0,
    }
    return safe_json({"listings": records, "summary": summary,
                      "periods": {"prev": PREV_PERIOD, "latest": LATEST_PERIOD}})


@app.get("/delisted/apartments/{listing_id}")
def delisted_apartments(listing_id: int):
    """All apartments from a delisted listing (from prev period)."""
    if not PREV_PERIOD:
        return safe_json({"apartments": []})
    dp = df[(df["listing_id"]==listing_id) & (df["period"]==PREV_PERIOD)]
    if dp.empty:
        return safe_json({"apartments": []})

    apt_cols = ["sub_listing_id","unit_type","price","size","price_per_m2",
                "floor","floor_num","bedrooms","bathrooms",
                "has_terrace","has_parking","has_pool","has_lift","has_ac","unit_url","last_updated"]
    apts = dp[[c for c in apt_cols if c in dp.columns]].copy()
    for col in ["floor_num","bedrooms","bathrooms"]:
        if col in apts.columns:
            apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")
    for col in ["has_terrace","has_parking","has_pool","has_lift","has_ac"]:
        if col in apts.columns:
            apts[col] = apts[col].fillna(False).astype(bool)
    apts = apts.sort_values(["unit_type","price"])
    records = _clean(apts.to_dict(orient="records"))
    records = [{k:(None if str(v)=="<NA>" else v) for k,v in r.items()} for r in records]

    meta = dp.iloc[0]
    return safe_json({
        "property_name": str(meta["property_name"]),
        "municipality":  str(meta["municipality"]),
        "developer":     str(meta["developer"]) if pd.notna(meta.get("developer")) else None,
        "last_period":   PREV_PERIOD,
        "apartments":    records,
    })
