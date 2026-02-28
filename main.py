from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import re
from typing import Optional, List

app = FastAPI(title="Valencia Housing Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load and preprocess data
df = pd.read_excel("valencia_all_units.xlsx")

def parse_year(date_str):
    if pd.isna(date_str) or str(date_str).strip() in ['Delivery ', 'Delivery : at start', 'Delivery : at end']:
        return None
    match = re.search(r'(\d{4})', str(date_str))
    return int(match.group(1)) if match else None

def parse_quarter(date_str):
    if pd.isna(date_str):
        return None
    s = str(date_str).lower()
    year = parse_year(date_str)
    if year is None:
        return None
    if 'immediate' in s:
        return f"2025 Q1"
    for q, months in [('Q1', ['january','february','march','first quarter','first semester']),
                      ('Q2', ['april','may','june','second quarter','second semester']),
                      ('Q3', ['july','august','september','third quarter']),
                      ('Q4', ['october','november','december','fourth quarter'])]:
        if any(m in s for m in months):
            return f"{year} {q}"
    return f"{year} Q2"

df['delivery_year'] = df['delivery_date'].apply(parse_year)
df['delivery_quarter'] = df['delivery_date'].apply(parse_quarter)

# Parse ESG
def parse_esg(s):
    if pd.isna(s): return None
    m = re.search(r'Consumption: ([A-E])', str(s))
    return m.group(1) if m else None

df['esg_grade'] = df['esg_certificate'].apply(parse_esg)

# Parse amenities features
def has_pool(s):
    return 'pool' in str(s).lower() or 'swimming' in str(s).lower()
def has_parking(s):
    return 'parking' in str(s).lower() or 'garage' in str(s).lower()
def has_garden(s):
    return 'garden' in str(s).lower() or 'terrace' in str(s).lower()

df['has_pool'] = df['amenities'].apply(has_pool)
df['has_parking'] = df['amenities'].apply(has_parking)
df['has_garden'] = df['amenities'].apply(has_garden)


def filter_df(municipality=None, unit_type=None, year=None, esg=None):
    d = df.copy()
    if municipality:
        d = d[d['municipality'].isin(municipality)]
    if unit_type:
        d = d[d['unit_type'].isin(unit_type)]
    if year:
        d = d[d['delivery_year'].isin([int(y) for y in year])]
    if esg:
        d = d[d['esg_grade'].isin(esg)]
    return d


@app.get("/filters")
def get_filters():
    return {
        "municipalities": sorted(df['municipality'].dropna().unique().tolist()),
        "unit_types": sorted(df['unit_type'].dropna().unique().tolist()),
        "delivery_years": sorted([int(y) for y in df['delivery_year'].dropna().unique()]),
        "esg_grades": sorted(df['esg_grade'].dropna().unique().tolist()),
    }


@app.get("/stats")
def get_stats(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    return {
        "total_units": len(d),
        "avg_price": round(d['price'].mean()) if len(d) else 0,
        "avg_price_m2": round(d['price_per_m2'].mean(), 1) if len(d) else 0,
        "avg_size": round(d['size'].mean(), 1) if len(d) else 0,
        "total_developments": d['listing_id'].nunique(),
    }


@app.get("/charts/price-by-unit-type")
def price_by_unit_type(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    result = d.groupby('unit_type').agg(
        avg_price=('price', 'mean'),
        count=('price', 'count'),
        avg_size=('size', 'mean')
    ).reset_index()
    result['avg_price'] = result['avg_price'].round(0)
    result['avg_size'] = result['avg_size'].round(1)
    order = ['Studio', '1BR', '2BR', '3BR', '4BR', '5BR', 'Penthouse']
    result['sort'] = result['unit_type'].apply(lambda x: order.index(x) if x in order else 99)
    result = result.sort_values('sort').drop('sort', axis=1)
    return result.to_dict(orient='records')


@app.get("/charts/delivery-timeline")
def delivery_timeline(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    d2 = d.dropna(subset=['delivery_quarter'])
    result = d2.groupby('delivery_quarter').agg(
        count=('price', 'count'),
        avg_price=('price', 'mean')
    ).reset_index()
    result['avg_price'] = result['avg_price'].round(0)
    result = result.sort_values('delivery_quarter')
    return result.to_dict(orient='records')


@app.get("/charts/price-distribution")
def price_distribution(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    bins = [0, 150000, 200000, 250000, 300000, 400000, 500000, 700000, 10000000]
    labels = ['<150k', '150-200k', '200-250k', '250-300k', '300-400k', '400-500k', '500-700k', '>700k']
    d['bin'] = pd.cut(d['price'], bins=bins, labels=labels)
    result = d.groupby('bin', observed=True).size().reset_index(name='count')
    return result.to_dict(orient='records')


@app.get("/charts/municipality-overview")
def municipality_overview(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    result = d.groupby('municipality').agg(
        units=('price', 'count'),
        avg_price=('price', 'mean'),
        avg_price_m2=('price_per_m2', 'mean')
    ).reset_index()
    result['avg_price'] = result['avg_price'].round(0)
    result['avg_price_m2'] = result['avg_price_m2'].round(1)
    result = result.sort_values('units', ascending=False).head(20)
    return result.to_dict(orient='records')


@app.get("/charts/esg-breakdown")
def esg_breakdown(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    result = d.groupby('esg_grade', dropna=False).agg(
        count=('price', 'count'),
        avg_price=('price', 'mean')
    ).reset_index()
    result['esg_grade'] = result['esg_grade'].fillna('Unknown')
    result['avg_price'] = result['avg_price'].round(0)
    return result.to_dict(orient='records')


@app.get("/charts/size-vs-price")
def size_vs_price(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    sample = d[['size', 'price', 'unit_type', 'municipality', 'property_name']].dropna()
    if len(sample) > 500:
        sample = sample.sample(500, random_state=42)
    return sample.to_dict(orient='records')


@app.get("/charts/amenities")
def amenities_chart(
    municipality: Optional[List[str]] = Query(None),
    unit_type: Optional[List[str]] = Query(None),
    year: Optional[List[str]] = Query(None),
    esg: Optional[List[str]] = Query(None),
):
    d = filter_df(municipality, unit_type, year, esg)
    total = len(d)
    return [
        {"amenity": "Pool/Swimming", "count": int(d['has_pool'].sum()), "pct": round(d['has_pool'].sum()/total*100, 1) if total else 0},
        {"amenity": "Parking/Garage", "count": int(d['has_parking'].sum()), "pct": round(d['has_parking'].sum()/total*100, 1) if total else 0},
        {"amenity": "Garden/Terrace", "count": int(d['has_garden'].sum()), "pct": round(d['has_garden'].sum()/total*100, 1) if total else 0},
    ]
