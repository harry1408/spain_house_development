from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import re, math, json
from typing import Optional, List

app = FastAPI(title="Valencia Housing Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── sanitize: replace NaN/Inf floats with None so JSON serialisation never breaks ──
def _clean(obj):
    if isinstance(obj, list):
        return [_clean(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

def safe_json(data):
    return JSONResponse(content=_clean(data))

# ── load & enrich ──────────────────────────────────────────────────────────────
_raw = pd.read_excel("valencia_all_units.xlsx")
df   = _raw.drop_duplicates(subset="sub_listing_id").copy()

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

df["delivery_year"]    = df["delivery_date"].apply(_year)
df["delivery_quarter"] = df["delivery_date"].apply(_quarter)

def _esg(s):
    if pd.isna(s): return None
    m = re.search(r"Consumption: ([A-E])", str(s))
    return m.group(1) if m else None

df["esg_grade"] = df["esg_certificate"].apply(_esg)

def _floor_num(f):
    if pd.isna(f) or str(f).strip() in ["-", "ext."]: return None
    if "ground" in str(f).lower(): return 0
    m = re.search(r"Floor (\d+)", str(f), re.I)
    return int(m.group(1)) if m else None

df["floor_num"] = df["floor"].apply(_floor_num)

def _parse_amenities(s):
    if pd.isna(s): s = ""
    s = str(s)
    m_bed  = re.search(r"(\d+)\s+bedroom", s)
    m_bath = re.search(r"(\d+)\s+bathroom", s)
    m_fa   = re.search(r"(\d+)\s+m.*?floor area", s)
    return {
        "bedrooms":      int(m_bed.group(1))  if m_bed  else (0 if "No bedroom" in s else None),
        "bathrooms":     int(m_bath.group(1)) if m_bath else None,
        "floor_area_m2": int(m_fa.group(1))   if m_fa   else None,
        "has_terrace":   "Terrace" in s,
        "has_parking":   "Parking" in s,
        "has_pool":      "Swimming pool" in s,
        "has_garden":    "Garden" in s,
        "has_lift":      "lift" in s.lower(),
        "has_ac":        "Air conditioning" in s,
        "has_storage":   "Storage room" in s,
        "has_wardrobes": "wardrobe" in s.lower(),
    }

_amenity_df = df["amenities"].apply(_parse_amenities).apply(pd.Series)
for col in _amenity_df.columns:
    df[col] = _amenity_df[col]

# ── filter helper ──────────────────────────────────────────────────────────────
def _filter(municipality=None, unit_type=None, year=None, esg=None):
    d = df.copy()
    if municipality: d = d[d["municipality"].isin(municipality)]
    if unit_type:    d = d[d["unit_type"].isin(unit_type)]
    if year:         d = d[d["delivery_year"].isin([int(y) for y in year])]
    if esg:          d = d[d["esg_grade"].isin(esg)]
    return d

# ══════════════════════════════════════════════════════════════════════════════
#  SUMMARY endpoints
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/filters")
def get_filters():
    return {
        "municipalities":  sorted(df["municipality"].dropna().unique().tolist()),
        "unit_types":      sorted(df["unit_type"].dropna().unique().tolist()),
        "delivery_years":  sorted([int(y) for y in df["delivery_year"].dropna().unique()]),
        "esg_grades":      sorted(df["esg_grade"].dropna().unique().tolist()),
    }

@app.get("/stats")
def get_stats(municipality: Optional[List[str]] = Query(None),
              unit_type:    Optional[List[str]] = Query(None),
              year:         Optional[List[str]] = Query(None),
              esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)
    return {
        "total_units":        len(d),
        "avg_price":          round(float(d["price"].mean()))          if len(d) else 0,
        "avg_price_m2":       round(float(d["price_per_m2"].mean()),1) if len(d) else 0,
        "avg_size":           round(float(d["size"].mean()),1)          if len(d) else 0,
        "total_developments": int(d["listing_id"].nunique()),
    }

@app.get("/charts/price-by-unit-type")
def price_by_unit_type(municipality: Optional[List[str]] = Query(None),
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)
    r = d.groupby("unit_type").agg(avg_price=("price","mean"), count=("price","count"), avg_size=("size","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0); r["avg_size"] = r["avg_size"].round(1)
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    r["_s"] = r["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    return safe_json(r.sort_values("_s").drop("_s",axis=1).to_dict(orient="records"))

@app.get("/charts/delivery-timeline")
def delivery_timeline(municipality: Optional[List[str]] = Query(None),
                      unit_type:    Optional[List[str]] = Query(None),
                      year:         Optional[List[str]] = Query(None),
                      esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg).dropna(subset=["delivery_quarter"])
    r = d.groupby("delivery_quarter").agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.sort_values("delivery_quarter").to_dict(orient="records"))

@app.get("/charts/price-distribution")
def price_distribution(municipality: Optional[List[str]] = Query(None),
                       unit_type:    Optional[List[str]] = Query(None),
                       year:         Optional[List[str]] = Query(None),
                       esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)
    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = d.copy(); d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    return safe_json(d2.groupby("bin", observed=True).size().reset_index(name="count").to_dict(orient="records"))

@app.get("/charts/municipality-overview")
def municipality_overview(municipality: Optional[List[str]] = Query(None),
                          unit_type:    Optional[List[str]] = Query(None),
                          year:         Optional[List[str]] = Query(None),
                          esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)
    r = d.groupby("municipality").agg(
        units=("price","count"),
        listings=("listing_id","nunique"),
        avg_price=("price","mean"),
        avg_price_m2=("price_per_m2","mean")
    ).reset_index()
    r["avg_price"]    = r["avg_price"].round(0)
    r["avg_price_m2"] = r["avg_price_m2"].round(1)
    return safe_json(r.sort_values("units", ascending=False).to_dict(orient="records"))

@app.get("/charts/esg-breakdown")
def esg_breakdown(municipality: Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)
    r = d.groupby("esg_grade", dropna=False).agg(count=("price","count"), avg_price=("price","mean")).reset_index()
    r["esg_grade"] = r["esg_grade"].fillna("Unknown")
    r["avg_price"] = r["avg_price"].round(0)
    return safe_json(r.to_dict(orient="records"))

@app.get("/charts/size-vs-price")
def size_vs_price(municipality: Optional[List[str]] = Query(None),
                  unit_type:    Optional[List[str]] = Query(None),
                  year:         Optional[List[str]] = Query(None),
                  esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg)[["size","price","unit_type","municipality","property_name"]].dropna()
    if len(d) > 600: d = d.sample(600, random_state=42)
    return safe_json(d.to_dict(orient="records"))

@app.get("/charts/amenities")
def amenities_chart(municipality: Optional[List[str]] = Query(None),
                    unit_type:    Optional[List[str]] = Query(None),
                    year:         Optional[List[str]] = Query(None),
                    esg:          Optional[List[str]] = Query(None)):
    d = _filter(municipality, unit_type, year, esg); total = len(d)
    if total == 0: return []
    return [
        {"amenity":"Pool",        "count":int(d["has_pool"].sum()),    "pct":round(float(d["has_pool"].sum()/total*100),1)},
        {"amenity":"Parking",     "count":int(d["has_parking"].sum()), "pct":round(float(d["has_parking"].sum()/total*100),1)},
        {"amenity":"Garden",      "count":int(d["has_garden"].sum()),  "pct":round(float(d["has_garden"].sum()/total*100),1)},
        {"amenity":"Terrace",     "count":int(d["has_terrace"].sum()), "pct":round(float(d["has_terrace"].sum()/total*100),1)},
        {"amenity":"Lift",        "count":int(d["has_lift"].sum()),    "pct":round(float(d["has_lift"].sum()/total*100),1)},
        {"amenity":"A/C",         "count":int(d["has_ac"].sum()),      "pct":round(float(d["has_ac"].sum()/total*100),1)},
        {"amenity":"Storage Room","count":int(d["has_storage"].sum()), "pct":round(float(d["has_storage"].sum()/total*100),1)},
    ]

# ══════════════════════════════════════════════════════════════════════════════
#  DRILL-DOWN endpoints
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/drilldown/municipality/{municipality}")
def drilldown_municipality(municipality: str):
    d = df[df["municipality"] == municipality]
    if d.empty:
        return safe_json({"listings":[],"stats":{},"unit_type_mix":[],"price_dist":[]})

    listings_grp = d.groupby(["listing_id","property_name","developer","delivery_date","esg_grade"], dropna=False).agg(
        units        =("sub_listing_id","nunique"),
        min_price    =("price","min"),
        max_price    =("price","max"),
        avg_price    =("price","mean"),
        avg_price_m2 =("price_per_m2","mean"),
        avg_size     =("size","mean"),
        unit_types   =("unit_type", lambda x: ", ".join(sorted(x.unique().tolist()))),
        has_pool     =("has_pool","max"),
        has_parking  =("has_parking","max"),
        has_terrace  =("has_terrace","max"),
        has_lift     =("has_lift","max"),
    ).reset_index()

    for c in ["avg_price","min_price","max_price"]:
        listings_grp[c] = listings_grp[c].round(0)
    listings_grp["avg_price_m2"] = listings_grp["avg_price_m2"].round(1)
    listings_grp["avg_size"]     = listings_grp["avg_size"].round(1)
    # esg_grade may be NaN
    listings_grp["esg_grade"] = listings_grp["esg_grade"].where(pd.notna(listings_grp["esg_grade"]), None)

    stats = {
        "total_units":    int(d["sub_listing_id"].nunique()),
        "total_listings": int(d["listing_id"].nunique()),
        "avg_price":      round(float(d["price"].mean())),
        "avg_price_m2":   round(float(d["price_per_m2"].mean()),1),
        "price_range":    [int(d["price"].min()), int(d["price"].max())],
    }

    mix = d.groupby("unit_type").size().reset_index(name="count")
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    mix["_s"] = mix["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    mix = mix.sort_values("_s").drop("_s", axis=1)

    bins   = [0,150000,200000,250000,300000,400000,500000,700000,10000000]
    labels = ["<150k","150-200k","200-250k","250-300k","300-400k","400-500k","500-700k",">700k"]
    d2 = d.copy()
    d2["bin"] = pd.cut(d2["price"], bins=bins, labels=labels)
    price_dist = d2.groupby("bin", observed=True).size().reset_index(name="count")

    return safe_json({
        "listings":      listings_grp.to_dict(orient="records"),
        "stats":         stats,
        "unit_type_mix": mix.to_dict(orient="records"),
        "price_dist":    price_dist.to_dict(orient="records"),
    })


@app.get("/drilldown/listing/{listing_id}")
def drilldown_listing(listing_id: int):
    d = df[df["listing_id"] == listing_id]
    if d.empty:
        return safe_json({})

    meta = d.iloc[0]

    apt_cols = ["sub_listing_id","unit_type","price","size","price_per_m2",
                "floor","floor_num","bedrooms","bathrooms","floor_area_m2",
                "has_terrace","has_parking","has_pool","has_garden",
                "has_lift","has_ac","has_storage","has_wardrobes","unit_url"]
    apts = d[apt_cols].copy()

    # ── Robust NaN → None conversion (handles numpy float64 NaN) ──────────────
    # 1) integer-like columns: convert float NaN columns to Int64 (nullable int)
    for col in ["floor_num","bedrooms","bathrooms","floor_area_m2"]:
        apts[col] = pd.to_numeric(apts[col], errors="coerce").astype("Int64")

    # 2) boolean columns: ensure plain Python bool
    for col in ["has_terrace","has_parking","has_pool","has_garden",
                "has_lift","has_ac","has_storage","has_wardrobes"]:
        apts[col] = apts[col].fillna(False).astype(bool)

    # 3) sort
    apts = apts.sort_values(["unit_type","price"])

    # 4) to_dict then sanitize — handles any residual numpy NaN
    apt_records = _clean(apts.to_dict(orient="records"))
    # convert pandas NA to None in int nullable columns
    apt_records = [
        {k: (None if str(v) == "<NA>" else v) for k, v in r.items()}
        for r in apt_records
    ]

    # Floor-price scatter data
    floor_price = d.dropna(subset=["floor_num"])[
        ["floor_num","price","unit_type","size","sub_listing_id"]
    ].copy()
    floor_price["floor_num"] = floor_price["floor_num"].astype(int)

    # Unit type comparison
    unit_comp = d.groupby("unit_type").agg(
        count        =("price","count"),
        avg_price    =("price","mean"),
        min_price    =("price","min"),
        max_price    =("price","max"),
        avg_size     =("size","mean"),
        avg_price_m2 =("price_per_m2","mean"),
    ).reset_index()
    for c in ["avg_price","min_price","max_price"]:
        unit_comp[c] = unit_comp[c].round(0)
    unit_comp["avg_price_m2"] = unit_comp["avg_price_m2"].round(1)
    unit_comp["avg_size"]     = unit_comp["avg_size"].round(1)
    order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]
    unit_comp["_s"] = unit_comp["unit_type"].apply(lambda x: order.index(x) if x in order else 99)
    unit_comp = unit_comp.sort_values("_s").drop("_s", axis=1)

    return safe_json({
        "listing_id":      int(listing_id),
        "property_name":   str(meta["property_name"]),
        "developer":       str(meta["developer"]),
        "municipality":    str(meta["municipality"]),
        "city_area":       str(meta["city_area"]),
        "delivery_date":   str(meta["delivery_date"]),
        "esg_grade":       str(meta["esg_grade"]) if pd.notna(meta["esg_grade"]) else None,
        "total_units":     int(len(d)),
        "apartments":      apt_records,
        "floor_price":     floor_price.to_dict(orient="records"),
        "unit_comparison": unit_comp.to_dict(orient="records"),
    })
