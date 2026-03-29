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
    # pandas nullable integers/booleans (Int64, boolean dtype) → unwrap via .item()
    if hasattr(obj, 'item'):
        try: return _clean(obj.item())
        except: pass
    # pandas NA / NaT / NaN scalar
    try:
        if pd.isna(obj): return None
    except (TypeError, ValueError): pass
    # numpy arrays
    if hasattr(obj, 'tolist'): return _clean(obj.tolist())
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

# Load geocoded streets CSV if present
_STREETS_CSV = os.path.join(_DATA_DIR, "combined_geocoded.csv")
_GEOCODED_STREETS = pd.DataFrame()
if os.path.exists(_STREETS_CSV):
    try:
        _GEOCODED_STREETS = pd.read_csv(_STREETS_CSV)
        # Normalise street names: clean encoding issues
        _GEOCODED_STREETS["street"] = _GEOCODED_STREETS["street"].apply(
            lambda s: str(s).encode("latin1").decode("utf-8", errors="replace") if isinstance(s, str) else s
        )
        print(f"[data] Geocoded streets loaded: {len(_GEOCODED_STREETS):,}")
    except Exception as e:
        print(f"[data] Could not load geocoded streets: {e}")

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
    sl = s.lower()
    if "semi-detached" in sl or "semidetached" in sl or "semi detached" in sl:
        ht = "Semi-detached house"
    elif "detached house" in sl:
        ht = "Detached house"
    elif "terraced house" in sl:
        ht = "Terraced house"
    elif "flat" in sl or "apartment" in sl:
        ht = "Flat"
    else:
        ht = "Not Mentioned"
    return {"bedrooms": int(m_bed.group(1)) if m_bed else (0 if "No bedroom" in s else None),
            "bathrooms": int(m_bath.group(1)) if m_bath else None,
            "floor_area_m2": int(m_fa.group(1)) if m_fa else None,
            "has_terrace":  "Terrace" in s, "has_parking": "Parking" in s,
            "has_pool": "Swimming pool" in s, "has_garden": "Garden" in s,
            "has_lift": "lift" in s.lower(), "has_ac": "Air conditioning" in s,
            "has_storage": "Storage room" in s, "has_wardrobes": "wardrobe" in s.lower(),
            "house_type": ht}

for _d in [_raw, df, _full]:
    _d["delivery_year"]    = _d["delivery_date"].apply(_year)
    _d["delivery_quarter"] = _d["delivery_date"].apply(_quarter)
    _d["esg_grade"]        = _d["esg_certificate"].apply(_esg)
    _d["floor_num"]        = _d["floor"].apply(_floor_num)
    _am = _d["amenities"].apply(_parse_amenities).apply(pd.Series)
    for col in _am.columns:
        _d[col] = _am[col]

# Property classification — from a 'property_type' column if present, else derived from unit_type
_PROP_TYPE_COL = next((c for c in ["property_type","tipo_propiedad","property_class","type"] if c in df.columns), None)
_UT_TO_CLASS = {t: "Apartment" for t in ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]}

def _get_property_class(row):
    if _PROP_TYPE_COL and pd.notna(row.get(_PROP_TYPE_COL)):
        raw = str(row[_PROP_TYPE_COL]).strip().lower()
        if any(x in raw for x in ["semi","pareado"]): return "Semi-detached"
        if any(x in raw for x in ["detached","chalet","villa","casa","house"]): return "Detached"
        if any(x in raw for x in ["terrace","terraced","adosado"]): return "Terraced"
        if any(x in raw for x in ["country","rural","finca","cortijo"]): return "Country House"
        if any(x in raw for x in ["apartment","flat","piso","duplex"]): return "Apartment"
    return _UT_TO_CLASS.get(str(row.get("unit_type","")), "Apartment")

for _d in [df, _full]:
    _d["property_class"] = _d.apply(_get_property_class, axis=1)

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
    result = d2.groupby("bin", observed=True).agg(count=("price","size")).reset_index()
    m2_bins   = [0,1000,1500,2000,2500,3000,3500,4000,5000,6000,100000]
    m2_labels = ["<1k","1-1.5k","1.5-2k","2-2.5k","2.5-3k","3-3.5k","3.5-4k","4-5k","5-6k",">6k"]
    d3 = d[d["price_per_m2"].notna() & (d["price_per_m2"] > 0)].copy()
    d3["bin"] = pd.cut(d3["price_per_m2"], bins=m2_bins, labels=m2_labels)
    m2_result = d3.groupby("bin", observed=True).agg(count=("price_per_m2","size")).reset_index()
    return safe_json({"price_dist": result.to_dict(orient="records"), "m2_dist": m2_result.to_dict(orient="records")})

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
        lat, lng, map_url = _listing_coords(r["listing_id"], str(r["municipality"]) if pd.notna(r.get("municipality")) else "")
        row = {c: r[c] for c in d2.columns}
        row["lat"] = lat or 39.47
        row["lng"] = lng or -0.38
        row["map_url"] = map_url
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
    d = _filter(municipality, unit_type, province=province, df_src=_full)
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
        unit_types   =("unit_type",  lambda x: ", ".join(sorted(x.dropna().unique().tolist()))),
        house_types  =("house_type", lambda x: ", ".join(sorted(t for t in x.dropna().unique() if t and t != "Not Mentioned"))),
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
    price_dist_grp = d2.groupby("bin", observed=True).agg(count=("price","size")).reset_index()
    price_dist = price_dist_grp

    # Separate €/m² distribution with proper m² bins
    m2_bins   = [0,1000,1500,2000,2500,3000,3500,4000,5000,6000,100000]
    m2_labels = ["<1k","1-1.5k","1.5-2k","2-2.5k","2.5-3k","3-3.5k","3.5-4k","4-5k","5-6k",">6k"]
    d3 = dl[dl["price_per_m2"].notna() & (dl["price_per_m2"] > 0)].copy()
    d3["bin"] = pd.cut(d3["price_per_m2"], bins=m2_bins, labels=m2_labels)
    m2_dist_grp = d3.groupby("bin", observed=True).agg(count=("price_per_m2","size")).reset_index()
    m2_dist = m2_dist_grp

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
                      "m2_dist": m2_dist.to_dict(orient="records"),
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
    if "house_type"   in dl.columns: apt_cols = apt_cols + ["house_type"]
    apts = dl[apt_cols].copy()
    for col in ["floor_num","bedrooms","bathrooms","floor_area_m2"]:
        apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")
    for col in ["has_terrace","has_parking","has_pool","has_garden","has_lift","has_ac","has_storage","has_wardrobes"]:
        apts[col] = apts[col].fillna(False).astype(bool)
    apts = apts.sort_values(["unit_type","price"])
    apt_records = _clean(apts.to_dict(orient="records"))
    apt_records = [{k:(None if str(v)=="<NA>" else v) for k,v in r.items()} for r in apt_records]
    # attach coords (same for all apts in same listing)
    lat, lng, map_url = _listing_coords(listing_id, str(meta["municipality"]) if pd.notna(meta.get("municipality")) else "")
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
        "description":   next((str(meta[c]) for c in ["description","property_description","descripcion","desc","comments"] if c in d.columns and pd.notna(meta.get(c))), None),
        "total_units":   int(len(dl)),
        "periods":       PERIODS_SORTED,
        "apartments":    apt_records,
        "floor_price":   floor_price.to_dict(orient="records"),
        "unit_comparison": unit_comp.to_dict(orient="records"),
        "listing_trend": listing_trend.drop("period_ord",axis=1).to_dict(orient="records"),
        "unit_type_trend": ut_trend.drop("period_ord",axis=1).to_dict(orient="records"),
        "apt_trend":     apt_trend_records,
        "unit_url_sample": str(dl["unit_url"].dropna().iloc[0]) if "unit_url" in dl.columns and dl["unit_url"].notna().any() else None,
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
            "last_updated": str(meta["last_updated"]) if pd.notna(meta.get("last_updated")) else None,
            "house_type": str(meta["house_type"]) if pd.notna(meta.get("house_type")) else None,
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

# Per-listing precise coordinates — built from latitude/longitude columns in Excel
# Falls back to municipality geocoding for listings without coords
def _build_listing_coords():
    coords = {}
    for lid, grp in _raw.groupby("listing_id"):
        row = grp.iloc[0]
        lat = row.get("latitude") if "latitude" in grp.columns else None
        lng = row.get("longitude") if "longitude" in grp.columns else None
        map_url = row.get("map") if "map" in grp.columns else None
        if pd.notna(lat) and pd.notna(lng) and float(lat) != 0 and float(lng) != 0:
            coords[int(lid)] = {
                "lat": float(lat),
                "lng": float(lng),
                "map_url": str(map_url) if pd.notna(map_url) else None,
            }
    return coords

LISTING_COORDS = _build_listing_coords()

def _listing_coords(listing_id, municipality=""):
    """Return (lat, lng, map_url) — from Excel coords first, then municipality fallback."""
    c = LISTING_COORDS.get(int(listing_id) if listing_id else -1)
    if c:
        return c["lat"], c["lng"], c.get("map_url")
    lat, lng = _get_coords(str(municipality))
    return (lat or 39.47), (lng or -0.38), None

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
        lat, lng, map_url = _listing_coords(r["listing_id"], str(r["municipality"]) if pd.notna(r["municipality"]) else "")
        addr = _parse_address(r["city_area"])
        # Skip listings without real coords (fallback would cluster at 39.47,-0.38)
        if not lat or not lng: continue
        rows.append({
            "listing_id":  int(r["listing_id"]),
            "property_name": str(r["property_name"]),
            "municipality": str(r["municipality"]) if pd.notna(r["municipality"]) else "",
            "comarca":     str(r["comarca"])     if pd.notna(r["comarca"])     else "",
            "street":      addr.get("street","") or "",
            "lat": lat, "lng": lng, "map_url": map_url,
            "units":     int(r["units"]),
            "avg_price": round(float(r["avg_price"])),
            "min_price": int(r["min_price"]),
        })
    return safe_json(rows)


def _haversine_km(lat1, lng1, lat2, lng2):
    """Distance in km between two lat/lng points."""
    import math as _math
    R = 6371
    dlat = _math.radians(lat2 - lat1)
    dlng = _math.radians(lng2 - lng1)
    a = _math.sin(dlat/2)**2 + _math.cos(_math.radians(lat1)) * _math.cos(_math.radians(lat2)) * _math.sin(dlng/2)**2
    return R * 2 * _math.atan2(_math.sqrt(a), _math.sqrt(1-a))

# ══════════════════════════════════════════════════════════════════════════
#  NEARBY COMPARISON  — same comarca, different listings
# ══════════════════════════════════════════════════════════════════════════
@app.get("/nearby/listings/{listing_id}")
def nearby_listings(listing_id: int, radius_km: Optional[float] = None):
    base = df[df["listing_id"]==listing_id]
    if base.empty: return safe_json({"comarca":"","listings":[]})
    comarca = base["comarca"].iloc[0]
    base_lat, base_lng, _ = _listing_coords(listing_id, str(base.iloc[0]["municipality"]))

    # Filter by radius if provided, else fall back to comarca
    if radius_km and base_lat and base_lng:
        # Use only listings that have real coords (not fallback)
        nearby_ids = [
            lid for lid, c in LISTING_COORDS.items()
            if _haversine_km(base_lat, base_lng, c["lat"], c["lng"]) <= radius_km
        ]
        d = df[df["listing_id"].isin(nearby_ids) & df["_is_latest"]]
    elif pd.isna(comarca):
        return safe_json({"comarca":"","listings":[]})
    else:
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
        lat, lng, map_url = _listing_coords(r["listing_id"], str(r["municipality"]))
        addr = _parse_address(r["city_area"])
        rows.append({
            **{k: (_clean(r[k]) if not isinstance(r[k], (str,bool,type(None))) else r[k]) for k in r.index},
            "is_current": int(r["listing_id"]) == listing_id,
            "lat": lat or 39.47, "lng": lng or -0.38,
            "map_url": map_url,
            "street": addr.get("street","") or "",
            "addr_comarca": str(comarca),
        })
    return safe_json({"comarca": str(comarca), "listings": rows})

@app.get("/nearby/apartments/{listing_id}")
def nearby_apartments(listing_id: int, unit_type: Optional[str] = None, radius_km: Optional[float] = None):
    """Compare apartments across nearby listings (same comarca or radius)."""
    base = df[df["listing_id"]==listing_id]
    if base.empty: return safe_json({"comarca":"","apartments":[]})
    comarca = base["comarca"].iloc[0]
    base_lat, base_lng, _ = _listing_coords(listing_id, str(base.iloc[0]["municipality"]))

    if radius_km and base_lat and base_lng:
        nearby_ids = [
            lid for lid, c in LISTING_COORDS.items()
            if _haversine_km(base_lat, base_lng, c["lat"], c["lng"]) <= radius_km
        ]
        d = df[df["listing_id"].isin(nearby_ids) & df["_is_latest"]].copy()
    elif pd.isna(comarca):
        return safe_json({"comarca":"","apartments":[]})
    else:
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
        lat, lng, map_url = _listing_coords(r["listing_id"], str(r["municipality"]))
        addr = _parse_address(r["city_area"])
        row = {c: r[c] for c in apt_cols}
        row["lat"] = lat or 39.47
        row["lng"] = lng or -0.38
        row["map_url"] = map_url
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
    lat, lng, map_url = _listing_coords(listing_id, str(r["municipality"]))
    addr = _parse_address(r["city_area"])
    return safe_json({
        "listing_id":   int(listing_id),
        "property_name":str(r["property_name"]),
        "municipality": str(r["municipality"]) if pd.notna(r["municipality"]) else "",
        "city_area":    str(r["city_area"])    if pd.notna(r["city_area"])    else "",
        "comarca":      str(r["comarca"])      if pd.notna(r["comarca"])      else "",
        "street":       addr.get("street","") or "",
        "lat": lat or 39.47, "lng": lng or -0.38, "map_url": map_url,
    })

# ══════════════════════════════════════════════════════════════════════════
#  DELISTED — developments + apartments present in prev period but not latest
# ══════════════════════════════════════════════════════════════════════════
@app.get("/delisted/listings")
def delisted_listings(province: Optional[List[str]] = Query(None),
                      municipality: Optional[List[str]] = Query(None)):
    d = df.copy()
    if province:     d = d[d["province"].isin(province)]
    if municipality: d = d[d["municipality"].isin(municipality)]

    # Per-province aware: a listing is delisted if it has rows in a non-latest period
    # but no rows in its province's latest period (uses the pre-computed _is_latest flag)
    has_latest     = set(d[d["_is_latest"]]["listing_id"].unique())
    has_non_latest = set(d[~d["_is_latest"]]["listing_id"].unique())
    delisted       = has_non_latest - has_latest

    if not delisted:
        return safe_json({"listings": [], "summary": {"count":0,"units":0}, "periods": {"prev":PREV_PERIOD,"latest":LATEST_PERIOD}})

    # For each delisted listing, use its most recent (non-latest) period snapshot
    d_non_latest = d[(d["listing_id"].isin(delisted)) & (~d["_is_latest"])]
    max_ords = d_non_latest.groupby("listing_id")["period_ord"].max().reset_index().rename(columns={"period_ord":"_max_ord"})
    d_non_latest = d_non_latest.merge(max_ords, on="listing_id")
    dp = d_non_latest[d_non_latest["period_ord"] == d_non_latest["_max_ord"]].drop("_max_ord", axis=1)
    grp = dp.groupby(["listing_id","property_name","developer","municipality","city_area","esg_grade","delivery_date"], dropna=False).agg(
        units        =("sub_listing_id","nunique"),
        avg_price    =("price","mean"),
        min_price    =("price","min"),
        max_price    =("price","max"),
        avg_price_m2 =("price_per_m2","mean"),
        avg_size     =("size","mean"),
        unit_types   =("unit_type",  lambda x: ", ".join(sorted(x.dropna().unique().tolist()))),
        house_types  =("house_type", lambda x: ", ".join(sorted(t for t in x.dropna().unique() if t and t != "Not Mentioned"))),
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
        lat, lng, map_url = _listing_coords(int(r["listing_id"]), str(r["municipality"]))
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
    """All apartments from a delisted listing (from its last non-latest period)."""
    d_lid = df[df["listing_id"]==listing_id]
    d_non_latest = d_lid[~d_lid["_is_latest"]]
    if d_non_latest.empty:
        return safe_json({"apartments": []})
    # Use the most recent non-latest period for this listing
    max_ord = d_non_latest["period_ord"].max()
    dp = d_non_latest[d_non_latest["period_ord"] == max_ord]
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
        "last_period":   str(dp.iloc[0]["period"]) if not dp.empty else PREV_PERIOD,
        "apartments":    records,
    })

# ══════════════════════════════════════════════════════════════════════════
#  IMAGE CLASSIFICATION — detect floor plans via Pillow image analysis
# ══════════════════════════════════════════════════════════════════════════
import json as _json, os as _os
_IMG_CACHE_FILE = _os.path.join(_os.path.dirname(__file__), "image_cache.json")
_IMG_CACHE: dict = {}

# Load persisted cache on startup
try:
    with open(_IMG_CACHE_FILE, "r") as _f:
        _IMG_CACHE = _json.load(_f)
except Exception:
    _IMG_CACHE = {}

def _save_img_cache():
    try:
        with open(_IMG_CACHE_FILE, "w") as _f:
            _json.dump(_IMG_CACHE, _f)
    except Exception:
        pass

def _is_floorplan(url: str) -> bool:
    """
    Classify an image as a floor plan using Pillow pixel analysis.
    Floor plans are nearly monochrome documents: very high white ratio + very low colour saturation.
    Thresholds calibrated on real Idealista data:
      - property photos:  white_ratio 0.0–0.31, saturation 35–79
      - floor plans:      white_ratio 0.7–0.85, saturation 2–8
    Results are cached to disk so each URL is only downloaded once.
    """
    if url in _IMG_CACHE:
        return _IMG_CACHE[url]

    result = False
    try:
        import requests as _req
        from PIL import Image as _PILImg
        import numpy as _np
        import io as _io

        resp = _req.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
        img = _PILImg.open(_io.BytesIO(resp.content)).convert("RGB")

        # Downscale for speed (200×200 is plenty for colour stats)
        img.thumbnail((200, 200))
        arr = _np.array(img, dtype=float)

        # White pixel ratio: all channels > 210
        white_ratio = (_np.all(arr > 210, axis=2)).mean()

        # Colour saturation proxy: mean channel range per pixel
        ch_max = arr.max(axis=2)
        ch_min = arr.min(axis=2)
        avg_sat = (ch_max - ch_min).mean()

        # Floor plan: mostly white AND nearly monochrome
        result = bool(white_ratio > 0.50 and avg_sat < 20)

    except Exception:
        result = False

    _IMG_CACHE[url] = result
    # Persist every 20 new entries
    if len(_IMG_CACHE) % 20 == 0:
        _save_img_cache()
    return result

def _classify_images_parallel(urls: list) -> dict:
    """Download + classify a list of URLs concurrently. Returns {url: is_floorplan}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {}
    uncached = [u for u in urls if u not in _IMG_CACHE]
    if uncached:
        with ThreadPoolExecutor(max_workers=8) as pool:
            fut = {pool.submit(_is_floorplan, u): u for u in uncached}
            for f in as_completed(fut):
                u = fut[f]
                results[u] = f.result()
        _save_img_cache()
    for u in urls:
        results[u] = _IMG_CACHE.get(u, results.get(u, False))
    return results

# ══════════════════════════════════════════════════════════════════════════
#  LISTING PHOTOS — read from images column in data
# ══════════════════════════════════════════════════════════════════════════
@app.get("/listing/photos/{listing_id}")
def listing_photos(listing_id: int):
    import re as _re, ast as _ast
    rows = df[df["listing_id"] == listing_id]
    if rows.empty or "images" not in rows.columns:
        return safe_json({"photos": []})

    _FLOORPLAN_RE = _re.compile(r'plano|planta|floor[_\-]plan|blueprint|fp_', _re.IGNORECASE)

    seen, all_urls = set(), []
    for raw in rows["images"].dropna():
        s = str(raw).strip()
        if not s or s == "0":
            continue
        urls = []
        try:
            parsed = _ast.literal_eval(s)
            if isinstance(parsed, list):
                urls = [str(u).strip() for u in parsed]
        except Exception:
            urls = [u.strip().strip("'").strip('"') for u in s.strip("[]").split(",")]

        for url in urls:
            if not url.startswith("http"):
                continue
            m = _re.search(r'/([a-f0-9]+)\.(webp|jpg)$', url)
            key = m.group(1) if m else url
            if key in seen:
                continue
            seen.add(key)
            all_urls.append(url)

    # Step 1: URL-keyword split (fast, no download needed)
    fp_by_url  = [u for u in all_urls if     _FLOORPLAN_RE.search(u)]
    ph_by_url  = [u for u in all_urls if not _FLOORPLAN_RE.search(u)]

    # Step 2: For the remaining "photos", run pixel analysis to catch
    #         floor-plan images whose URLs have no keywords
    classifications = _classify_images_parallel(ph_by_url)
    photos      = [u for u in ph_by_url if not classifications.get(u, False)]
    fp_by_pixel = [u for u in ph_by_url if     classifications.get(u, False)]

    floor_plans = fp_by_url + fp_by_pixel
    return safe_json({"photos": photos, "floor_plans": floor_plans})

# ── Search page endpoints ─────────────────────────────────────────────────

@app.get("/search/options")
def search_options(municipality: Optional[List[str]] = Query(None)):
    """Return all municipalities and all area/locality/street parts from city_area + geocoded CSV."""
    munis = sorted(_raw["municipality"].dropna().unique().tolist())

    base = _raw.copy()
    if municipality:
        base = base[base["municipality"].isin(municipality)]

    # Always exclude province-level names and municipality names
    exclude = {"valencia", "alicante", "spain", "españa", "comunitat valenciana"}
    all_munis = {m.lower() for m in _raw["municipality"].dropna().unique()}

    locations = set()

    # Extract from city_area column
    for val in base["city_area"].dropna():
        parts = [p.strip() for p in str(val).split(",")]
        for part in parts[:-1]:
            clean = re.sub(r'\s+NN\s*$', '', part, flags=re.IGNORECASE).strip()
            clean = re.sub(r'\s+\d+[a-zA-Z]*\s*$', '', clean).strip()
            if len(clean) < 3: continue
            if clean.lower() in exclude: continue
            if clean.lower() in all_munis: continue
            if re.match(r'^\d+$', clean): continue
            locations.add(clean)

    # Add geocoded streets from CSV
    if not _GEOCODED_STREETS.empty:
        geo = _GEOCODED_STREETS.copy()
        if municipality:
            # Filter to matching municipalities (case-insensitive)
            munis_lower = {m.lower() for m in municipality}
            geo = geo[geo["municipality"].str.lower().isin(munis_lower)]
        for val in geo["street"].dropna():
            clean = str(val).strip()
            clean = re.sub(r'\s+\d+[a-zA-Z]*\s*$', '', clean).strip()
            if len(clean) < 3: continue
            if clean.lower() in exclude: continue
            if clean.lower() in all_munis: continue
            locations.add(clean)

    # Build street → coords lookup for geocoded streets (validated against municipality listings)
    street_coords = {}
    if not _GEOCODED_STREETS.empty:
        geo_for_coords = _GEOCODED_STREETS.copy()
        if municipality:
            munis_lower = {m.lower() for m in municipality}
            geo_for_coords = geo_for_coords[geo_for_coords["municipality"].str.lower().isin(munis_lower)]

        # Compute municipality listing centroid for distance validation
        _opt_centroid = None
        if municipality:
            _opt_lids = list(base["listing_id"].unique())[:100]
            _opt_mc = [(LISTING_COORDS[lid]["lat"], LISTING_COORDS[lid]["lng"])
                       for lid in _opt_lids if lid in LISTING_COORDS]
            if _opt_mc:
                _opt_centroid = (sum(c[0] for c in _opt_mc) / len(_opt_mc),
                                 sum(c[1] for c in _opt_mc) / len(_opt_mc))

        for _, row in geo_for_coords.iterrows():
            if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
                g_lat, g_lng = float(row["latitude"]), float(row["longitude"])
                # Skip if geocoded location is >20 km from municipality listings
                if _opt_centroid and _haversine_km(_opt_centroid[0], _opt_centroid[1], g_lat, g_lng) > 20:
                    continue
                street_coords[str(row["street"]).strip()] = {"lat": g_lat, "lng": g_lng}

    return safe_json({
        "municipalities": munis,
        "locations": sorted(locations),
        "street_coords": street_coords,
    })


@app.get("/search/listings")
def search_listings(
    municipality: Optional[List[str]] = Query(None),
    street:       Optional[List[str]] = Query(None),
    radius_km:    Optional[float]     = Query(None),
    lat:          Optional[float]     = Query(None),
    lng:          Optional[float]     = Query(None),
    unit_type:    Optional[List[str]] = Query(None),
    min_price:    Optional[float]     = Query(None),
    max_price:    Optional[float]     = Query(None),
    min_m2:       Optional[float]     = Query(None),
    max_m2:       Optional[float]     = Query(None),
    esg:          Optional[List[str]] = Query(None),
    house_type:   Optional[List[str]] = Query(None),
):
    def _parse_esg_grade(val):
        """Extract best (lowest) grade letter from 'Consumption: A, Emissions: B' etc."""
        if not val or str(val).lower() in ("nan", "unknown", ""):
            return None
        m = re.findall(r':\s*([A-G])', str(val), re.IGNORECASE)
        if m:
            return sorted(m, key=lambda g: "ABCDEFG".index(g.upper()))[0].upper()
        m2 = re.match(r'^([A-G])$', str(val).strip(), re.IGNORECASE)
        return m2.group(1).upper() if m2 else None

    d = _raw.copy()

    # ESG filter — compare parsed grade
    if esg:
        d = d[d["esg_certificate"].apply(lambda v: _parse_esg_grade(v) in esg)]

    if municipality: d = d[d["municipality"].isin(municipality)]
    if unit_type:    d = d[d["unit_type"].isin(unit_type)]
    if house_type:   d = d[d["house_type"].isin(house_type)]

    # Area/locality/street filter — match against city_area OR use geocoded coords
    geo_centers = []  # populated inside street block, needed later for area_center
    if street:
        # Check if any selected street is in the geocoded CSV — if so, use its coords as center
        if not _GEOCODED_STREETS.empty:
            # Build municipality listing centroid for distance validation
            _muni_centroid = None
            if municipality:
                _lids = d["listing_id"].unique()[:100]  # sample for speed
                _mc = [(LISTING_COORDS[lid]["lat"], LISTING_COORDS[lid]["lng"])
                       for lid in _lids if lid in LISTING_COORDS]
                if _mc:
                    _muni_centroid = (sum(c[0] for c in _mc) / len(_mc),
                                      sum(c[1] for c in _mc) / len(_mc))

            for s in street:
                s_lower = s.strip().lower()
                match = _GEOCODED_STREETS[_GEOCODED_STREETS["street"].str.lower().str.strip() == s_lower]
                if not match.empty:
                    row = match.iloc[0]
                    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
                        g_lat, g_lng = float(row["latitude"]), float(row["longitude"])
                        # Validate: skip if geocoded location is >20 km from municipality listings
                        if _muni_centroid is None or _haversine_km(_muni_centroid[0], _muni_centroid[1], g_lat, g_lng) <= 20:
                            geo_centers.append((g_lat, g_lng))

        def _street_match(city_area_val):
            v = str(city_area_val).lower()
            for s in street:
                s_clean = re.sub(r'\s+nn\s*$', '', s, flags=re.IGNORECASE).strip().lower()
                if s_clean in v:
                    return True
            return False
        mask = d["city_area"].apply(_street_match)
        d_matched = d[mask]

        # If geocoded streets found and radius_km provided, also include nearby listings
        if geo_centers and radius_km:
            geo_listing_ids = set()
            for c_lat, c_lng in geo_centers:
                for lid, c in LISTING_COORDS.items():
                    if _haversine_km(c_lat, c_lng, c["lat"], c["lng"]) <= radius_km:
                        geo_listing_ids.add(lid)
            d_geo = d[d["listing_id"].isin(geo_listing_ids)]
            d = pd.concat([d_matched, d_geo]).drop_duplicates(subset=["listing_id"]) if not d_geo.empty else d_matched
        elif geo_centers and not radius_km:
            # Geocoded street selected but no radius — use 2 km default to find nearby listings
            _default_r = 2.0
            geo_listing_ids = set()
            for c_lat, c_lng in geo_centers:
                for lid, c in LISTING_COORDS.items():
                    if _haversine_km(c_lat, c_lng, c["lat"], c["lng"]) <= _default_r:
                        geo_listing_ids.add(lid)
            d_geo = d[d["listing_id"].isin(geo_listing_ids)]
            d = pd.concat([d_matched, d_geo]).drop_duplicates(subset=["listing_id"]) if not d_geo.empty else d_matched
        else:
            d = d_matched

    # Price filters
    if min_price: d = d[d["price"] >= min_price]
    if max_price: d = d[d["price"] <= max_price]
    if min_m2:    d = d[d["price_per_m2"] >= min_m2]
    if max_m2:    d = d[d["price_per_m2"] <= max_m2]

    if d.empty:
        _early_center = None
        if geo_centers:
            _early_center = {"lat": sum(c[0] for c in geo_centers) / len(geo_centers),
                             "lng": sum(c[1] for c in geo_centers) / len(geo_centers)}
        return safe_json({"listings": [], "total": 0, "area_center": _early_center})

    # Group to listing level
    # Build safe agg dict — only include columns that exist
    _agg = dict(
        property_name  =("property_name","first"),
        municipality   =("municipality","first"),
        province       =("province","first"),
        developer      =("developer","first"),
        units          =("sub_listing_id","nunique"),
        avg_price      =("price","mean"),
        avg_price_m2   =("price_per_m2","mean"),
        min_price      =("price","min"),
        max_price      =("price","max"),
        unit_types     =("unit_type",  lambda x: ", ".join(sorted(x.dropna().unique()))),
        house_types    =("house_type", lambda x: ", ".join(sorted(t for t in x.dropna().unique() if t and t != "Not Mentioned"))),
        city_area      =("city_area","first"),
    )
    if "esg_certificate" in d.columns:
        _agg["esg_certificate"] = ("esg_certificate","first")
    grp = d.groupby("listing_id").agg(**_agg).reset_index()
    grp["avg_price"]    = grp["avg_price"].round(0)
    grp["avg_price_m2"] = grp["avg_price_m2"].round(1)
    grp["min_price"]    = grp["min_price"].round(0)
    grp["max_price"]    = grp["max_price"].round(0)

    # Attach coords and parsed ESG grade
    rows = []
    for _, r in grp.iterrows():
        lid = int(r["listing_id"])
        lat_v, lng_v, _ = _listing_coords(lid, r["municipality"])
        row = {**{k: _clean(v) for k, v in r.items()}, "lat": lat_v, "lng": lng_v}
        row["esg_grade"] = _parse_esg_grade(r.get("esg_certificate", ""))
        rows.append(row)

    # Radius filter — compute centroid from all rows that have real coords
    if radius_km:
        # Use provided lat/lng or compute centroid of current results
        if lat is not None and lng is not None:
            c_lat, c_lng = lat, lng
        else:
            coords = [(r["lat"], r["lng"]) for r in rows if r["lat"] and r["lng"]]
            if coords:
                c_lat = sum(c[0] for c in coords) / len(coords)
                c_lng = sum(c[1] for c in coords) / len(coords)
            else:
                c_lat, c_lng = None, None

        if c_lat and c_lng:
            rows = [r for r in rows if r["lat"] and r["lng"]
                    and _haversine_km(c_lat, c_lng, r["lat"], r["lng"]) <= radius_km]

    # Per-unit-type stats from the apartment-level data (accurate counts + prices per type)
    _ut_order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    _ut_agg = d.groupby("unit_type").agg(
        count     =("sub_listing_id","nunique"),
        min_price =("price","min"),
        avg_price =("price","mean"),
        max_price =("price","max"),
        avg_size  =("size","mean"),
        avg_pm2   =("price_per_m2","mean"),
    ).reset_index()
    for _c in ["min_price","avg_price","max_price"]: _ut_agg[_c] = _ut_agg[_c].round(0)
    _ut_agg["avg_size"] = _ut_agg["avg_size"].round(1)
    _ut_agg["avg_pm2"]  = _ut_agg["avg_pm2"].round(0)
    _ut_agg["_s"] = _ut_agg["unit_type"].apply(lambda x: _ut_order.index(x) if x in _ut_order else 99)
    _ut_agg = _ut_agg.sort_values("_s").drop("_s", axis=1)

    # Determine area center for the frontend map
    area_center = None
    if street and geo_centers:
        # Geocoded street: use its exact coordinates
        area_center = {
            "lat": sum(c[0] for c in geo_centers) / len(geo_centers),
            "lng": sum(c[1] for c in geo_centers) / len(geo_centers),
        }
    elif street and rows:
        # Neighbourhood/area name: use centroid of matched result listings (much tighter than full municipality)
        matched_coords = [(r["lat"], r["lng"]) for r in rows if r.get("lat") and r.get("lng")]
        if matched_coords:
            area_center = {
                "lat": sum(c[0] for c in matched_coords) / len(matched_coords),
                "lng": sum(c[1] for c in matched_coords) / len(matched_coords),
            }
    elif municipality:
        muni_lids = df[df["municipality"].isin(municipality)]["listing_id"].unique()
        muni_coords = [
            (LISTING_COORDS[lid]["lat"], LISTING_COORDS[lid]["lng"])
            for lid in muni_lids
            if lid in LISTING_COORDS
        ]
        if muni_coords:
            area_center = {
                "lat": sum(c[0] for c in muni_coords) / len(muni_coords),
                "lng": sum(c[1] for c in muni_coords) / len(muni_coords),
            }

    return safe_json({"listings": rows, "total": len(rows),
                      "unit_type_stats": _clean(_ut_agg.to_dict(orient="records")),
                      "area_center": area_center})
