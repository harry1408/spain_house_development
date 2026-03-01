from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import re, math, json
from typing import Optional, List

app = FastAPI(title="Valencia Housing Dashboard API")
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
_raw = pd.read_excel("valencia_all_units.xlsx")
# Canonical snapshot label: "Feb 2026"
_raw["period"] = _raw["Month"].astype(str) + " " + _raw["Year"].astype(str)
_raw["period_ord"] = _raw["Month"].map(MONTH_ORDER) + (_raw["Year"].astype(int)-2000)*100

# Full df (with duplicates) used only for snapshot-aware queries; deduplicated per sub+period for unit data
_full = _raw.copy()
df = _raw.drop_duplicates(subset=["sub_listing_id","period"]).copy()

PERIODS_SORTED = sorted(df["period"].unique(), key=lambda p: df[df["period"]==p]["period_ord"].iloc[0])
LATEST_PERIOD  = PERIODS_SORTED[-1]
PREV_PERIOD    = PERIODS_SORTED[-2] if len(PERIODS_SORTED) > 1 else None

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

def _filter(municipality=None, unit_type=None, year=None, esg=None, period=None, df_src=None):
    d = (df_src if df_src is not None else df).copy()
    if municipality: d = d[d["municipality"].isin(municipality)]
    if unit_type:    d = d[d["unit_type"].isin(unit_type)]
    if year:         d = d[d["delivery_year"].isin([int(y) for y in year])]
    if esg:          d = d[d["esg_grade"].isin(esg)]
    if period:       d = d[d["period"].isin(period)]
    return d

# ══════════════════════════════════════════════════════════════════════════
#  META
# ══════════════════════════════════════════════════════════════════════════
@app.get("/filters")
def get_filters():
    return {"municipalities": sorted(df["municipality"].dropna().unique().tolist()),
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
              unit_type:    Optional[List[str]] = Query(None),
              year:         Optional[List[str]] = Query(None),
              esg:          Optional[List[str]] = Query(None),
              period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    p = _filter(municipality, unit_type, year, esg, [PREV_PERIOD]) if PREV_PERIOD else None
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
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None),
                       period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    r = d.groupby("unit_type").agg(avg_price=("price","mean"), count=("price","count"), avg_size=("size","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0); r["avg_size"] = r["avg_size"].round(1)
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    r["_s"] = r["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    return safe_json(r.sort_values("_s").drop("_s",axis=1).to_dict(orient="records"))

@app.get("/charts/delivery-timeline")
def delivery_timeline(municipality: Optional[List[str]] = Query(None),
                      unit_type:    Optional[List[str]] = Query(None),
                      year:         Optional[List[str]] = Query(None),
                      esg:          Optional[List[str]] = Query(None),
                      period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD]).dropna(subset=["delivery_quarter"])
    r = d.groupby("delivery_quarter").agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.sort_values("delivery_quarter").to_dict(orient="records"))

@app.get("/charts/price-distribution")
def price_distribution(municipality: Optional[List[str]] = Query(None),
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None),
                       period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = d.copy(); d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    return safe_json(d2.groupby("bin", observed=True).size().reset_index(name="count").to_dict(orient="records"))

@app.get("/charts/municipality-overview")
def municipality_overview(municipality: Optional[List[str]] = Query(None),
                          unit_type:    Optional[List[str]] = Query(None),
                          year:         Optional[List[str]] = Query(None),
                          esg:          Optional[List[str]] = Query(None),
                          period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    r = d.groupby("municipality").agg(units=("price","count"), listings=("listing_id","nunique"),
                                      avg_price=("price","mean"), avg_price_m2=("price_per_m2","mean")).reset_index()
    r["avg_price"]    = r["avg_price"].round(0)
    r["avg_price_m2"] = r["avg_price_m2"].round(1)
    return safe_json(r.sort_values("units", ascending=False).to_dict(orient="records"))

@app.get("/charts/esg-breakdown")
def esg_breakdown(municipality: Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None),
                  period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    r = d.groupby("esg_grade", dropna=False).agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["esg_grade"] = r["esg_grade"].fillna("Unknown")
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.to_dict(orient="records"))

@app.get("/charts/size-vs-price")
def size_vs_price(municipality: Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None),
                  period:       Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg, period or [LATEST_PERIOD])
    d2 = d[["size","price","unit_type","municipality","property_name"]].dropna()
    if len(d2) > 600: d2 = d2.sample(600, random_state=42)
    return safe_json(d2.to_dict(orient="records"))

# ══════════════════════════════════════════════════════════════════════════
#  TEMPORAL  — market-wide month-over-month
# ══════════════════════════════════════════════════════════════════════════
@app.get("/temporal/market-trend")
def market_trend(municipality: Optional[List[str]] = Query(None),
                 unit_type:    Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type)
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
def unit_type_trend(municipality: Optional[List[str]] = Query(None)):
    d = _filter(municipality)
    r = d.groupby(["period","period_ord","unit_type"]).agg(
        avg_price=("price","mean"), count=("sub_listing_id","nunique")
    ).reset_index().sort_values(["unit_type","period_ord"])
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.drop("period_ord",axis=1).to_dict(orient="records"))

@app.get("/temporal/municipality-trend")
def municipality_trend(municipality: Optional[List[str]] = Query(None)):
    d = _filter(municipality) if municipality else df
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
                    unit_type:    Optional[List[str]] = Query(None)):
    """Units available per period, plus new/removed vs prior period."""
    d = _filter(municipality, unit_type)
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

    # latest period snapshot for listings
    dl = d[d["period"]==LATEST_PERIOD]
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

    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = dl.copy(); d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    price_dist = d2.groupby("bin", observed=True).size().reset_index(name="count")

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
                      "price_dist": price_dist.to_dict(orient="records"),
                      "trend": trend.drop("period_ord",axis=1).to_dict(orient="records")})

# ══════════════════════════════════════════════════════════════════════════
#  DRILL-DOWN — listing (development)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/drilldown/listing/{listing_id}")
def drilldown_listing(listing_id: int):
    d = df[df["listing_id"] == listing_id]
    if d.empty: return safe_json({})

    meta = d[d["period"]==LATEST_PERIOD].iloc[0] if not d[d["period"]==LATEST_PERIOD].empty else d.iloc[0]
    dl   = d[d["period"]==LATEST_PERIOD]

    apt_cols = ["sub_listing_id","unit_type","price","size","price_per_m2",
                "floor","floor_num","bedrooms","bathrooms","floor_area_m2",
                "has_terrace","has_parking","has_pool","has_garden",
                "has_lift","has_ac","has_storage","has_wardrobes","unit_url"]
    apts = dl[apt_cols].copy()
    for col in ["floor_num","bedrooms","bathrooms","floor_area_m2"]:
        apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")
    for col in ["has_terrace","has_parking","has_pool","has_garden","has_lift","has_ac","has_storage","has_wardrobes"]:
        apts[col] = apts[col].fillna(False).astype(bool)
    apts = apts.sort_values(["unit_type","price"])
    apt_records = _clean(apts.to_dict(orient="records"))
    apt_records = [{k:(None if str(v)=="<NA>" else v) for k,v in r.items()} for r in apt_records]

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
