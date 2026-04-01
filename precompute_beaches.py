"""
precompute_beaches.py
=====================
Run once (or whenever your listing data changes) to compute the nearest beach
for every listing in the dataset and save the results to beach_distances.json.

Usage:
    cd backend
    python precompute_beaches.py

Output:
    beach_distances.json  — {listing_id: {nearest_beach_km, nearest_beach_name}}

The file is loaded automatically by main.py at startup.
Delete beach_distances.json and re-run this script to refresh.
"""

import os, json, math, glob
import pandas as pd
import requests

# ── Config ────────────────────────────────────────────────────────────────────
_DIR        = os.path.dirname(__file__)
_DATA_DIR   = os.path.join(_DIR, "data")
_OUT_FILE   = os.path.join(_DIR, "beach_distances.json")
_MAX_KM     = 20          # only record beaches within this distance
MONTH_ORDER = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
               "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}

# ── Load listing data (same logic as main.py) ─────────────────────────────────
_xlsx_files = sorted(glob.glob(os.path.join(_DATA_DIR, "*.xlsx")))
if not _xlsx_files:
    raise SystemExit(f"No .xlsx files found in {_DATA_DIR}")

print(f"Loading {len(_xlsx_files)} file(s)…")
_raw = pd.concat([pd.read_excel(f) for f in _xlsx_files], ignore_index=True)
print(f"  {len(_raw):,} rows loaded")

# Build listing → (lat, lng) map from the data
_lat_col = next((c for c in ["latitude","lat","Latitude","Lat"] if c in _raw.columns), None)
_lng_col = next((c for c in ["longitude","lng","lon","Longitude","Lng","Lon"] if c in _raw.columns), None)

LISTING_COORDS: dict = {}
if _lat_col and _lng_col:
    grp = _raw.dropna(subset=[_lat_col, _lng_col]).groupby("listing_id").first().reset_index()
    for _, row in grp.iterrows():
        lat = pd.to_numeric(row[_lat_col], errors="coerce")
        lng = pd.to_numeric(row[_lng_col], errors="coerce")
        if pd.notna(lat) and pd.notna(lng) and float(lat) != 0 and float(lng) != 0:
            LISTING_COORDS[int(row["listing_id"])] = {"lat": float(lat), "lng": float(lng)}

print(f"  {len(LISTING_COORDS):,} listings have coordinates")

# ── Fetch beach data from OpenStreetMap Overpass API ─────────────────────────
_BEACH_RAW_FILE = os.path.join(_DIR, "beach_raw.json")

def fetch_beaches():
    if os.path.exists(_BEACH_RAW_FILE):
        print("Loading cached raw beach data…")
        with open(_BEACH_RAW_FILE) as f:
            return json.load(f)

    print("Fetching beach data from OpenStreetMap (this may take 30–60s)…")
    query = """
[out:json][timeout:120];
(
  node["natural"="beach"](35.0,-10.0,44.5,5.0);
  way["natural"="beach"](35.0,-10.0,44.5,5.0);
  relation["natural"="beach"](35.0,-10.0,44.5,5.0);
);
out center tags;
"""
    resp = requests.post(
        "https://overpass-api.de/api/interpreter",
        data={"data": query},
        timeout=120,
    )
    resp.raise_for_status()
    elements = resp.json().get("elements", [])

    beaches = []
    for el in elements:
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lng = el.get("lon") or (el.get("center") or {}).get("lon")
        if not lat or not lng:
            continue
        tags = el.get("tags", {})
        name = tags.get("name") or tags.get("name:en") or tags.get("name:es") or ""
        beaches.append({"name": name, "lat": float(lat), "lng": float(lng)})

    with open(_BEACH_RAW_FILE, "w") as f:
        json.dump(beaches, f)
    print(f"  Fetched {len(beaches)} beach features, saved to beach_raw.json")
    return beaches


# ── Haversine distance ────────────────────────────────────────────────────────
def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Compute nearest beach per listing ────────────────────────────────────────
def nearest_beach(lat, lng, beaches, max_km=_MAX_KM):
    best_dist, best_name = float("inf"), None
    for b in beaches:
        d = haversine_km(lat, lng, b["lat"], b["lng"])
        if d < best_dist:
            best_dist, best_name = d, b["name"]
    if best_dist <= max_km:
        return round(best_dist, 1), best_name or "Beach"
    return None, None


def main():
    beaches = fetch_beaches()
    print(f"Computing nearest beach for {len(LISTING_COORDS):,} listings…")

    results = {}
    for i, (lid, coords) in enumerate(LISTING_COORDS.items(), 1):
        km, name = nearest_beach(coords["lat"], coords["lng"], beaches)
        results[lid] = {"nearest_beach_km": km, "nearest_beach_name": name}
        if i % 100 == 0:
            print(f"  {i}/{len(LISTING_COORDS)}", end="\r")

    near_beach = sum(1 for v in results.values() if v["nearest_beach_km"] is not None)
    print(f"\n  {near_beach:,} listings within {_MAX_KM} km of a beach")

    with open(_OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved to {_OUT_FILE}")


if __name__ == "__main__":
    main()
