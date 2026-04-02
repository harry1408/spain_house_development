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
_DIR      = os.path.dirname(__file__)
_DATA_DIR = os.path.join(_DIR, "data")
_OUT_FILE = os.path.join(_DIR, "beach_distances.json")
_MAX_KM   = 20

# ── Municipality fallback coords (mirrors main.py MUNI_COORDS exactly) ────────
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

def _get_muni_coords(municipality):
    if not municipality:
        return None, None
    if municipality in MUNI_COORDS:
        return MUNI_COORDS[municipality]
    muni_lower = str(municipality).lower()
    for k, v in MUNI_COORDS.items():
        if muni_lower in k.lower() or k.lower() in muni_lower:
            return v
    return None, None

# ── Load listing data ─────────────────────────────────────────────────────────
_xlsx_files = sorted(glob.glob(os.path.join(_DATA_DIR, "*.xlsx")))
if not _xlsx_files:
    raise SystemExit(f"No .xlsx files found in {_DATA_DIR}")

print(f"Loading {len(_xlsx_files)} file(s)…")
_raw = pd.concat([pd.read_excel(f) for f in _xlsx_files], ignore_index=True)
print(f"  {len(_raw):,} rows loaded")

# Clean municipality (same as main.py)
def _clean_municipality(s):
    if pd.isna(s): return s
    s = str(s).strip()
    return s.split(",")[-1].strip() if "," in s else s

_raw["municipality"] = _raw["municipality"].apply(_clean_municipality)

# Build listing → (lat, lng) using exact same logic as main.py:
# 1. Use "latitude"/"longitude" columns from Excel if present and non-zero
# 2. Fall back to municipality geocoding
print("Building listing coordinates…")
LISTING_COORDS: dict = {}

# Step 1: exact coords from Excel
for lid, grp in _raw.groupby("listing_id"):
    row = grp.iloc[0]
    lat = row.get("latitude") if "latitude" in grp.columns else None
    lng = row.get("longitude") if "longitude" in grp.columns else None
    if lat is not None and lng is not None:
        try:
            lat_f, lng_f = float(lat), float(lng)
            if pd.notna(lat_f) and pd.notna(lng_f) and lat_f != 0 and lng_f != 0:
                LISTING_COORDS[int(lid)] = {
                    "lat": lat_f, "lng": lng_f, "source": "excel"
                }
        except (ValueError, TypeError):
            pass

print(f"  {len(LISTING_COORDS):,} listings with exact Excel coordinates")

# Step 2: municipality fallback for remaining listings
all_listings = _raw.groupby("listing_id")["municipality"].first().reset_index()
missing = 0
for _, row in all_listings.iterrows():
    lid = int(row["listing_id"])
    if lid in LISTING_COORDS:
        continue
    lat, lng = _get_muni_coords(str(row["municipality"]) if pd.notna(row["municipality"]) else "")
    if lat and lng:
        LISTING_COORDS[lid] = {"lat": lat, "lng": lng, "source": "municipality"}
        missing += 1

print(f"  {missing:,} listings filled via municipality fallback")
print(f"  {len(LISTING_COORDS):,} listings total with coordinates")

# ── Fetch beach data from OpenStreetMap Overpass API ─────────────────────────
_BEACH_RAW_FILE = os.path.join(_DIR, "beach_raw.json")

def fetch_beaches():
    if os.path.exists(_BEACH_RAW_FILE):
        print("Loading cached raw beach data from beach_raw.json…")
        with open(_BEACH_RAW_FILE) as f:
            data = json.load(f)
        print(f"  {len(data)} beaches loaded")
        return data

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


def nearest_beach(lat, lng, beaches, max_km=_MAX_KM):
    best_dist, best_name = float("inf"), None
    for b in beaches:
        d = haversine_km(lat, lng, b["lat"], b["lng"])
        if d < best_dist:
            best_dist, best_name = d, b["name"]
    if best_dist <= max_km:
        return round(best_dist, 1), best_name or "Beach"
    return None, None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    beaches = fetch_beaches()
    total = len(LISTING_COORDS)
    print(f"Computing nearest beach for {total:,} listings…")

    results = {}
    for i, (lid, coords) in enumerate(LISTING_COORDS.items(), 1):
        km, name = nearest_beach(coords["lat"], coords["lng"], beaches)
        results[lid] = {"nearest_beach_km": km, "nearest_beach_name": name}
        if i % 200 == 0:
            print(f"  {i}/{total}", end="\r")

    near_beach = sum(1 for v in results.values() if v["nearest_beach_km"] is not None)
    print(f"\n  {near_beach:,} / {total:,} listings within {_MAX_KM} km of a beach")

    with open(_OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved to {_OUT_FILE}")


if __name__ == "__main__":
    main()
