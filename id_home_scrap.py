import datetime
import calendar
from urllib.parse import urlparse, parse_qs
import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from bs4 import BeautifulSoup
import json
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import glob
import os
import ast

# Set by pipeline via _sc.datadome = "..."
datadome = ""

def api_getter():
    url = "https://www.idealista.com/en/ajax/listing/georeach/valencia-valencia/pagina-3.htm"

    # Create session
    session = requests.Session()

    # Retry strategy (helps avoid temporary blocking)
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[403, 429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    # Headers extracted & cleaned from curl
    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "referer": "https://www.idealista.com/en/venta-viviendas/valencia-valencia/",
        "priority": "u=1, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ IMPORTANT: Replace with fresh values from browser when request fails
    cookies = {
        "SESSION": "0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc",
        "datadome": "8RArcrQ1Tt_9KolnZ77MUTU_HW2XrQg8SSJ_am_ERJnaZkaJVwr7MGTyeKkoJhbikKX08msWLu8xJjmI_mq55AaNA_BnxjfWq2xXcwB8hGwgYMgU1nTbhKyW7CEDYWfh",
        "userUUID": "f943f60d-9600-4449-8313-247b25d74430",
        "lang": "en",
        "PARAGLIDE_LOCALE": "en"
    }

    response = session.get(url, headers=headers, cookies=cookies)

    print("Status Code:", response.status_code)

    if response.status_code == 200:
            data = response.json()

            # pretty print
            # print(json.dumps(data, indent=2))
            for v in data["body"]['ads']:
                print(v)
                datalayerres = requests.get(f'https://www.idealista.com/detail/{v["adId"]}/datalayer')
                print(datalayerres.json())


def get_html_by_state():
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd


    STATE_NAME = "valencia-valencia"
    properties = []
    for i in range(50):

        url = f"https://www.idealista.com/en/venta-viviendas/{STATE_NAME}/pagina-{i+1}.htm"

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
            "priority": "u=0, i",
            "sec-ch-device-memory": "8",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
        }

        # ⚠️ paste FULL cookie string exactly as in curl
        cookies_raw = """userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome=eXJ_89ignO9eBiTcelxT7qOTpvKrvhotrAYthTbA7nZkbd2PH0rZCOVP2ldr~YreePdIJ8XhNUpMMZqDt6kXDyiw24haL~kgHPHY4XBboe0EgJattnFOOxJ9ZKsFzHK~"""

        response = requests.get(
            url,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )

        print("Status:", response.status_code)

        html = response.text

        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".item-info-container")



        for card in cards:

            # 🔹 Agency info
            agency_tag = card.select_one(".logo-branding a")
            agency_name = agency_tag.get("title") if agency_tag else None
            agency_url = "https://www.idealista.com" + agency_tag["href"] if agency_tag else None

            logo_tag = card.select_one(".logo-branding img")
            agency_logo = logo_tag["src"] if logo_tag else None

            # 🔹 Title & link
            title_tag = card.select_one(".item-link")
            title = title_tag.text.strip() if title_tag else None
            property_url = "https://www.idealista.com" + title_tag["href"] if title_tag else None

            # 🔹 Price
            price_tag = card.select_one(".item-price")
            price = price_tag.text.strip() if price_tag else None

            # 🔹 Details (beds, size, extras)
            details = [d.text.strip() for d in card.select(".item-detail")]

            bedrooms = None
            size = None
            extras = []

            for d in details:
                if "bed" in d.lower():
                    bedrooms = d
                elif "m²" in d.lower():
                    size = d
                else:
                    extras.append(d)

            # 🔹 Description
            desc_tag = card.select_one(".item-description p")
            description = desc_tag.text.strip() if desc_tag else None

            # 🔹 Contact options
            has_call_button = bool(card.select_one(".phone-btn"))
            has_email_button = bool(card.select_one(".email-btn"))
            print(title)
            properties.append({
                "state": STATE_NAME,
                "title": title,
                "price": price,
                "bedrooms": bedrooms,
                "size_m2": size,
                "extra_features": ", ".join(extras),
                "description": description,
                "property_url": property_url,
                "agency_name": agency_name,
                "agency_url": agency_url,
                "agency_logo": agency_logo,
                "call_available": has_call_button,
                "email_available": has_email_button
            })
            print("Saved", len(properties), "properties")

        # 🔹 Convert to DataFrame
    df = pd.DataFrame(properties)

    # 🔹 Save CSV
    df.to_csv("valencia_properties.csv", index=False, encoding="utf-8")


        # save HTML

def get_html():


    file = pd.read_csv("spain_regions.csv")

    BASE = "https://www.idealista.com"

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = """userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome=eXJ_89ignO9eBiTcelxT7qOTpvKrvhotrAYthTbA7nZkbd2PH0rZCOVP2ldr~YreePdIJ8XhNUpMMZqDt6kXDyiw24haL~kgHPHY4XBboe0EgJattnFOOxJ9ZKsFzHK~"""

    all_properties = []
    seen_urls = set()
    done_properties=[]

    def extract_cards(html, sub_region):
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".item-info-container")

        extracted = []

        for card in cards:

            title_tag = card.select_one(".item-link")
            if not title_tag:
                continue

            property_url = BASE + title_tag["href"]

            # avoid duplicates
            if property_url in seen_urls:
                continue

            seen_urls.add(property_url)

            # 🔹 Agency info
            agency_tag = card.select_one(".logo-branding a")
            agency_name = agency_tag.get("title") if agency_tag else None
            agency_url = BASE + agency_tag["href"] if agency_tag else None

            logo_tag = card.select_one(".logo-branding img")
            agency_logo = logo_tag["src"] if logo_tag else None

            # 🔹 Title
            title = title_tag.text.strip()

            # 🔹 Price
            price_tag = card.select_one(".item-price")
            price = price_tag.text.strip() if price_tag else None

            # 🔹 Details
            details = [d.text.strip() for d in card.select(".item-detail")]

            bedrooms = None
            size = None
            extras = []

            for d in details:
                if "bed" in d.lower():
                    bedrooms = d
                elif "m²" in d.lower():
                    size = d
                else:
                    extras.append(d)

            # 🔹 Description
            desc_tag = card.select_one(".item-description p")
            description = desc_tag.text.strip() if desc_tag else None

            # 🔹 Contact buttons
            has_call_button = bool(card.select_one(".phone-btn"))
            has_email_button = bool(card.select_one(".email-btn"))

            extracted.append({
                "sub_region": sub_region,
                "title": title,
                "price": price,
                "bedrooms": bedrooms,
                "size_m2": size,
                "extra_features": ", ".join(extras),
                "description": description,
                "property_url": property_url,
                "agency_name": agency_name,
                "agency_url": agency_url,
                "agency_logo": agency_logo,
                "call_available": has_call_button,
                "email_available": has_email_button
            })

        return extracted

    for _, row in file.iterrows():

        sub_region = row["subregion"]
        if sub_region not in ["Alicante","Castellon","Valencia"]:
            continue
        sub_link = row["subregion_link"]

        if sub_region in done_properties:
            continue
        done_properties.append(sub_region)
        expected_count = int(str(row["subregion_count"]).replace(",", "")) if pd.notna(row["subregion_count"]) else None

        print(f"\n====== {sub_region} ======")

        page = 1
        collected = 0

        while True:

            if page == 1:
                url = sub_link
            else:
                url = sub_link.rstrip("/") + f"/pagina-{page}.htm"

            print("Scraping:", url)

            r = requests.get(
            url,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )


            if r.status_code != 200:
                print("Blocked or finished")
                break

            listings = extract_cards(r.text, sub_region)

            if not listings:
                break

            all_properties.extend(listings)
            collected += len(listings)

            print(f"Collected {collected} listings and expected count {expected_count}")

            # stop if reached expected count
            if expected_count and collected >= expected_count:
                print("Reached expected count")
                break

            page += 1
            time.sleep(1)

        # save to CSV
        df = pd.DataFrame(all_properties)
        df.to_csv(f"region/{sub_region}_spain_properties_full.csv", index=False)

        print("\nSaved", len(df), "properties")

def get_links():
    from bs4 import BeautifulSoup
    import pandas as pd

    html = """<nav class="locations-list" aria-labelledby="municipalityTitle">
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/andalucia/" title="Buy homes in Andalusia"><h3 class="region-title">Andalusia</h3></a>
</li>
<li>
<p>9,221</p>
<a href="/en/venta-viviendas/almeria-provincia/municipios" class="subregion" title="Buy homes in Almeria">Almeria</a>
</li>
<li>
<p>10,332</p>
<a href="/en/venta-viviendas/cadiz-provincia/municipios" class="subregion" title="Buy homes in Cadiz">Cadiz</a>
</li>
<li>
<p>7,004</p>
<a href="/en/venta-viviendas/cordoba-provincia/municipios" class="subregion" title="Buy homes in Córdoba">Córdoba</a>
</li>
<li>
<p>13,313</p>
<a href="/en/venta-viviendas/granada-provincia/municipios" class="subregion" title="Buy homes in Granada">Granada</a>
</li>
<li>
<p>3,703</p>
<a href="/en/venta-viviendas/huelva-provincia/municipios" class="subregion" title="Buy homes in Huelva">Huelva</a>
</li>
<li>
<p>4,993</p>
<a href="/en/venta-viviendas/jaen-provincia/municipios" class="subregion" title="Buy homes in Jaén">Jaén</a>
</li>
<li>
<p>40,084</p>
<a href="/en/venta-viviendas/malaga-provincia/municipios" class="subregion" title="Buy homes in Malaga">Malaga</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/malaga-malaga/" class="icon-elbow --indent" title="Buy homes in Málaga">Málaga</a>
</li>
<li>
<a href="/en/venta-viviendas/marbella-malaga/" class="icon-elbow --indent" title="Buy homes in Marbella">Marbella</a>
</li>
</ul>
</li>
<li>
<p>9,929</p>
<a href="/en/venta-viviendas/sevilla-provincia/municipios" class="subregion" title="Buy homes in Seville">Seville</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/sevilla-sevilla/" class="icon-elbow --indent" title="Buy homes in Seville">Seville</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/aragon/" title="Buy homes in Aragon"><h3 class="region-title">Aragon</h3></a>
</li>
<li>
<p>1,750</p>
<a href="/en/venta-viviendas/huesca-provincia/municipios" class="subregion" title="Buy homes in Huesca">Huesca</a>
</li>
<li>
<p>1,332</p>
<a href="/en/venta-viviendas/teruel-provincia/municipios" class="subregion" title="Buy homes in Teruel">Teruel</a>
</li>
<li>
<p>4,476</p>
<a href="/en/venta-viviendas/zaragoza-provincia/municipios" class="subregion" title="Buy homes in Zaragoza">Zaragoza</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/zaragoza-zaragoza/" class="icon-elbow --indent" title="Buy homes in Zaragoza">Zaragoza</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/pais-vasco/" title="Buy homes in Basque Country"><h3 class="region-title">Basque Country</h3></a>
</li>
<li>
<p>1,367</p>
<a href="/en/venta-viviendas/alava/municipios" class="subregion" title="Buy homes in Álava">Álava</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/vitoria-gasteiz-alava/" class="icon-elbow --indent" title="Buy homes in Vitoria-Gasteiz">Vitoria-Gasteiz</a>
</li>
</ul>
</li>
<li>
<p>4,218</p>
<a href="/en/venta-viviendas/vizcaya/municipios" class="subregion" title="Buy homes in Biscay">Biscay</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/bilbao-vizcaya/" class="icon-elbow --indent" title="Buy homes in Bilbao">Bilbao</a>
</li>
</ul>
</li>
<li>
<p>2,281</p>
<a href="/en/venta-viviendas/guipuzcoa/municipios" class="subregion" title="Buy homes in Gipuzkoa">Gipuzkoa</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/donostia-san-sebastian-guipuzcoa/" class="icon-elbow --indent" title="Buy homes in Donostia-San Sebastián">Donostia-San Sebastián</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/islas-canarias/" title="Buy homes in Canary Islands"><h3 class="region-title">Canary Islands</h3></a>
</li>
<li>
<p>6,607</p>
<a href="/en/venta-viviendas/las-palmas/municipios" class="subregion" title="Buy homes in Las Palmas">Las Palmas</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/las-palmas/fuerteventura/" class="icon-elbow --indent" title="Buy homes in Fuerteventura">Fuerteventura</a>
</li>
<li>
<a href="/en/venta-viviendas/las-palmas/gran-canaria/" class="icon-elbow --indent" title="Buy homes in Gran Canaria">Gran Canaria</a>
</li>
<li>
<a href="/en/venta-viviendas/las-palmas/lanzarote/" class="icon-elbow --indent" title="Buy homes in Lanzarote">Lanzarote</a>
</li>
</ul>
</li>
<li>
<p>9,967</p>
<a href="/en/venta-viviendas/santa-cruz-de-tenerife-provincia/municipios" class="subregion" title="Buy homes in Santa Cruz de Tenerife">Santa Cruz de Tenerife</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/santa-cruz-de-tenerife/la-palma/" class="icon-elbow --indent" title="Buy homes in La Palma">La Palma</a>
</li>
<li>
<a href="/en/venta-viviendas/santa-cruz-de-tenerife/tenerife/" class="icon-elbow --indent" title="Buy homes in Tenerife">Tenerife</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/cantabria/" title="Buy homes in Cantabria"><h3 class="region-title">Cantabria</h3></a>
</li>
<li>
<p>6,321</p>
<a href="/en/venta-viviendas/cantabria/municipios" class="subregion" title="Buy homes in Cantabria">Cantabria</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/santander-cantabria/" class="icon-elbow --indent" title="Buy homes in Santander">Santander</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/castilla-la-mancha/" title="Buy homes in Castile-La Mancha"><h3 class="region-title">Castile-La Mancha</h3></a>
</li>
<li>
<p>3,497</p>
<a href="/en/venta-viviendas/albacete-provincia/municipios" class="subregion" title="Buy homes in Albacete">Albacete</a>
</li>
<li>
<p>5,497</p>
<a href="/en/venta-viviendas/ciudad-real-provincia/municipios" class="subregion" title="Buy homes in Ciudad Real">Ciudad Real</a>
</li>
<li>
<p>1,747</p>
<a href="/en/venta-viviendas/cuenca-provincia/municipios" class="subregion" title="Buy homes in Cuenca">Cuenca</a>
</li>
<li>
<p>2,289</p>
<a href="/en/venta-viviendas/guadalajara-provincia/municipios" class="subregion" title="Buy homes in Guadalajara">Guadalajara</a>
</li>
<li>
<p>5,742</p>
<a href="/en/venta-viviendas/toledo-provincia/municipios" class="subregion" title="Buy homes in Toledo">Toledo</a>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/castilla-y-leon/" title="Buy homes in Castile-Leon"><h3 class="region-title">Castile-Leon</h3></a>
</li>
<li>
<p>1,890</p>
<a href="/en/venta-viviendas/avila-provincia/municipios" class="subregion" title="Buy homes in Ávila">Ávila</a>
</li>
<li>
<p>3,761</p>
<a href="/en/venta-viviendas/burgos-provincia/municipios" class="subregion" title="Buy homes in Burgos">Burgos</a>
</li>
<li>
<p>4,784</p>
<a href="/en/venta-viviendas/leon-provincia/municipios" class="subregion" title="Buy homes in León">León</a>
</li>
<li>
<p>1,597</p>
<a href="/en/venta-viviendas/palencia-provincia/municipios" class="subregion" title="Buy homes in Palencia">Palencia</a>
</li>
<li>
<p>3,637</p>
<a href="/en/venta-viviendas/salamanca-provincia/municipios" class="subregion" title="Buy homes in Salamanca">Salamanca</a>
</li>
<li>
<p>1,494</p>
<a href="/en/venta-viviendas/segovia-provincia/municipios" class="subregion" title="Buy homes in Segovia">Segovia</a>
</li>
<li>
<p>712</p>
<a href="/en/venta-viviendas/soria-provincia/municipios" class="subregion" title="Buy homes in Soria">Soria</a>
</li>
<li>
<p>2,729</p>
<a href="/en/venta-viviendas/valladolid-provincia/municipios" class="subregion" title="Buy homes in Valladolid">Valladolid</a>
</li>
<li>
<p>1,336</p>
<a href="/en/venta-viviendas/zamora-provincia/municipios" class="subregion" title="Buy homes in Zamora">Zamora</a>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/cataluna/" title="Buy homes in Catalonia"><h3 class="region-title">Catalonia</h3></a>
</li>
<li>
<p>45,234</p>
<a href="/en/venta-viviendas/barcelona-provincia/municipios" class="subregion" title="Buy homes in Barcelona">Barcelona</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/barcelona-barcelona/" class="icon-elbow --indent" title="Buy homes in Barcelona">Barcelona</a>
</li>
<li>
<a href="/en/venta-viviendas/hospitalet-de-llobregat-barcelona/" class="icon-elbow --indent" title="Buy homes in Hospitalet de Llobregat">Hospitalet de Llobregat</a>
</li>
<li>
<a href="/en/venta-viviendas/terrassa-barcelona/" class="icon-elbow --indent" title="Buy homes in Terrassa">Terrassa</a>
</li>
</ul>
</li>
<li>
<p>19,317</p>
<a href="/en/venta-viviendas/girona-provincia/municipios" class="subregion" title="Buy homes in Gerona">Gerona</a>
</li>
<li>
<p>3,510</p>
<a href="/en/venta-viviendas/lleida-provincia/municipios" class="subregion" title="Buy homes in Lleida">Lleida</a>
</li>
<li>
<p>13,175</p>
<a href="/en/venta-viviendas/tarragona-provincia/municipios" class="subregion" title="Buy homes in Tarragona">Tarragona</a>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/navarra/" title="Buy homes in Comunidad Foral de Navarra"><h3 class="region-title">Comunidad Foral de Navarra</h3></a>
</li>
<li>
<p>2,295</p>
<a href="/en/venta-viviendas/navarra/municipios" class="subregion" title="Buy homes in Navarra">Navarra</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/pamplonairuna-navarra/" class="icon-elbow --indent" title="Buy homes in Pamplona/Iruña">Pamplona/Iruña</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/madrid-provincia/" title="Buy homes in Comunidad de Madrid"><h3 class="region-title">Comunidad de Madrid</h3></a>
</li>
<li>
<p>26,755</p>
<a href="/en/venta-viviendas/madrid-provincia/municipios" class="subregion" title="Buy homes in Madrid">Madrid</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/las-rozas-de-madrid-madrid/" class="icon-elbow --indent" title="Buy homes in Las Rozas de Madrid">Las Rozas de Madrid</a>
</li>
<li>
<a href="/en/venta-viviendas/madrid-madrid/" class="icon-elbow --indent" title="Buy homes in Madrid">Madrid</a>
</li>
<li>
<a href="/en/venta-viviendas/pozuelo-de-alarcon-madrid/" class="icon-elbow --indent" title="Buy homes in Pozuelo de Alarcón">Pozuelo de Alarcón</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/extremadura/" title="Buy homes in Extremadura"><h3 class="region-title">Extremadura</h3></a>
</li>
<li>
<p>3,770</p>
<a href="/en/venta-viviendas/badajoz-provincia/municipios" class="subregion" title="Buy homes in Badajoz">Badajoz</a>
</li>
<li>
<p>4,168</p>
<a href="/en/venta-viviendas/caceres-provincia/municipios" class="subregion" title="Buy homes in Cáceres">Cáceres</a>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/galicia/" title="Buy homes in Galicia"><h3 class="region-title">Galicia</h3></a>
</li>
<li>
<p>7,221</p>
<a href="/en/venta-viviendas/a-coruna-provincia/municipios" class="subregion" title="Buy homes in A Coruña">A Coruña</a>
</li>
<li>
<p>3,313</p>
<a href="/en/venta-viviendas/lugo-provincia/municipios" class="subregion" title="Buy homes in Lugo">Lugo</a>
</li>
<li>
<p>4,270</p>
<a href="/en/venta-viviendas/ourense-provincia/municipios" class="subregion" title="Buy homes in Ourense">Ourense</a>
</li>
<li>
<p>6,554</p>
<a href="/en/venta-viviendas/pontevedra-provincia/municipios" class="subregion" title="Buy homes in Pontevedra">Pontevedra</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/vigo-pontevedra/" class="icon-elbow --indent" title="Buy homes in Vigo">Vigo</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/balears-illes/" title="Buy homes in Islas Baleares"><h3 class="region-title">Islas Baleares</h3></a>
</li>
<li>
<p>22,540</p>
<a href="/en/venta-viviendas/balears-illes/municipios" class="subregion" title="Buy homes in Balearic Islands">Balearic Islands</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/balears-illes/ibiza/" class="icon-elbow --indent" title="Buy homes in Ibiza">Ibiza</a>
</li>
<li>
<a href="/en/venta-viviendas/balears-illes/mallorca/" class="icon-elbow --indent" title="Buy homes in Mallorca">Mallorca</a>
</li>
<li>
<a href="/en/venta-viviendas/balears-illes/menorca/" class="icon-elbow --indent" title="Buy homes in Menorca">Menorca</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/la-rioja/" title="Buy homes in La Rioja"><h3 class="region-title">La Rioja</h3></a>
</li>
<li>
<p>3,572</p>
<a href="/en/venta-viviendas/la-rioja/municipios" class="subregion" title="Buy homes in La Rioja">La Rioja</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/logrono-la-rioja/" class="icon-elbow --indent" title="Buy homes in Logroño">Logroño</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/asturias/" title="Buy homes in Principado de Asturias"><h3 class="region-title">Principado de Asturias</h3></a>
</li>
<li>
<p>7,757</p>
<a href="/en/venta-viviendas/asturias/municipios" class="subregion" title="Buy homes in Asturias">Asturias</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/gijon-asturias/" class="icon-elbow --indent" title="Buy homes in Gijón">Gijón</a>
</li>
<li>
<a href="/en/venta-viviendas/oviedo-asturias/" class="icon-elbow --indent" title="Buy homes in Oviedo">Oviedo</a>
</li>
</ul>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/venta-viviendas/murcia-provincia/" title="Buy homes in Región de Murcia"><h3 class="region-title">Región de Murcia</h3></a>
</li>
<li>
<p>16,140</p>
<a href="/en/venta-viviendas/murcia-provincia/municipios" class="subregion" title="Buy homes in Murcia">Murcia</a>
</li>
</ul>
<ul class="locations-list__links">
<li>
<a href="/en/geo/venta-viviendas/comunidad-valenciana/" title="Buy homes in Valencia region"><h3 class="region-title">Valencia region</h3></a>
</li>
<li>
<p>47,068</p>
<a href="/en/venta-viviendas/alicante/municipios" class="subregion" title="Buy homes in Alicante">Alicante</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/alicante-alacant-alicante/" class="icon-elbow --indent" title="Buy homes in Alicante">Alicante</a>
</li>
<li>
<a href="/en/venta-viviendas/elche-elx-alicante/" class="icon-elbow --indent" title="Buy homes in Elche / Elx">Elche / Elx</a>
</li>
</ul>
</li>
<li>
<p>9,536</p>
<a href="/en/venta-viviendas/castellon/municipios" class="subregion" title="Buy homes in Castellon">Castellon</a>
</li>
<li>
<p>21,569</p>
<a href="/en/venta-viviendas/valencia-provincia/municipios" class="subregion" title="Buy homes in Valencia">Valencia</a>
<ul class="locations-list__municipalities">
<li>
<a href="/en/venta-viviendas/valencia-valencia/" class="icon-elbow --indent" title="Buy homes in Valencia">Valencia</a>
</li>
</ul>
</li>
</ul>
<article class="location-list__special-regions">
<h3 class="outer-region-title">Autonomous cities</h3>
<ul class="locations-list__links">
<li>
<p>88</p>
<a href="/en/venta-viviendas/ceuta-ceuta/" class="subregion" title="Buy homes in Ceuta">Ceuta</a>
</li>
<li>
<p>203</p>
<a href="/en/venta-viviendas/melilla-melilla/" class="subregion" title="Buy homes in Melilla">Melilla</a>
</li>
</ul>
</article>
<article class="location-list__special-regions">
<h3 class="outer-region-title">Other areas</h3>
<ul class="locations-list__links">
<li>
<p>1,596</p>
<a href="/en/venta-viviendas/andorra-provincia/municipios" class="subregion" title="Buy homes in Andorra">Andorra</a>
</li>
<li>
<p>185</p>
<a href="/en/venta-viviendas/cerdanya-francesa/municipios" class="subregion" title="Buy homes in Cerdanya Francesa">Cerdanya Francesa</a>
</li>
<li>
<p>137</p>
<a href="/en/venta-viviendas/pais-vasco-frances/municipios" class="subregion" title="Buy homes in País Vasco Francés">País Vasco Francés</a>
</li>
</ul>
</article>
</nav>"""


    soup = BeautifulSoup(html, "lxml")

    BASE = "https://www.idealista.com"

    data = []

    blocks = soup.select("ul.locations-list__links")

    for block in blocks:

        region_tag = block.select_one(".region-title")
        if not region_tag:
            continue

        region = region_tag.text.strip()
        region_link = BASE + region_tag.find_parent("a")["href"]

        # region count not always present -> set None
        region_count = None

        subregion_links = block.find_all("a", class_="subregion")

        for sub in subregion_links:
            subregion = sub.text.strip()
            subregion_link = BASE + sub["href"]

            li = sub.find_parent("li")

            # subregion count (inside <p>)
            count_tag = li.find("p")
            subregion_count = count_tag.text.strip().replace(",", "") if count_tag else None

            municipalities = li.select(".locations-list__municipalities a")

            if municipalities:
                for m in municipalities:
                    municipality = m.text.strip()
                    municipality_link = BASE + m["href"]

                    data.append({
                        "region": region,
                        "region_count": region_count,
                        "subregion": subregion,
                        "subregion_count": subregion_count,
                        "municipality": municipality,
                        "municipality_count": None,
                        "region_link": region_link,
                        "subregion_link": subregion_link.replace('municipios',''),
                        "municipality_link": municipality_link
                    })
            else:
                data.append({
                    "region": region,
                    "region_count": region_count,
                    "subregion": subregion,
                    "subregion_count": subregion_count,
                    "municipality": None,
                    "municipality_count": None,
                    "region_link": region_link,
                    "subregion_link": subregion_link.replace('municipios',''),
                    "municipality_link": None
                })

    df = pd.DataFrame(data)

    print(df.head())
    print("\nTotal rows:", len(df))

    df.to_csv("spain_regions.csv", index=False)


def get_indivdual_listing(province,type,month_name):
    os.makedirs(f"{province}_chunks", exist_ok=True)
    os.makedirs("region", exist_ok=True)

    # folder containing your csv files
    folder_path = "region/"  # change if needed
    file_iter=1
    all_links = []

    # find files containing "valencia" in name
    # files = glob.glob(folder_path + "*valencia*.csv")
    files = glob.glob(folder_path + "*.csv")

    files = [f for f in files
             if f"{province}" in os.path.basename(f).lower()
             and "new" in os.path.basename(f).lower()
             and month_name in os.path.basename(f).lower() ]

    print("Files found:", len(files))
    all_rows = []
    main_links = []
    for file in files:
        area = file.split('region\\')[1].split(f'_{province}_properties_full')[0]
        print("Reading:", file, "Region",area)


        df = pd.read_csv(file)

        # change column name if needed
        if "property_url" in df.columns:
            for lnk in df["property_url"]:
                if lnk in all_links:
                    continue
                all_links.append(lnk)
                # if len(all_links) < 21000:
                #     continue
                main_links.append({'url': lnk, 'type': 'main', 'main': 'NA',"area":area})


        elif "url" in df.columns:
            for lnk in df["url"]:
                if lnk in all_links:
                    continue
                all_links.append(lnk)
                # if len(all_links) < 21000:
                #     continue
                main_links.append({'url': lnk, 'type': 'main', 'main': 'NA', "area": area})
    print("Total Links",len(all_links))
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = f"""userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome={datadome}"""


    for lnk in main_links:
        URL=lnk['url']

        BASE = "https://www.idealista.com"
        print(URL)

        response = requests.get(
            URL,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )

        soup = BeautifulSoup(response.text, "lxml")

        data = {}
        data['type'] = lnk['type']
        data['main'] = lnk['main']
        data['municipal'] = lnk['area']

        # =========================
        # BASIC INFO
        # =========================

        data["url"] = URL
        data["listing_id"] = URL.rstrip("/").split("/")[-1]

        title_tag = soup.select_one("span.main-info__title-main")
        data["title"] = title_tag.text.strip() if title_tag else None

        price_tag = soup.select_one(".info-data-price")
        data["price"] = price_tag.text.strip() if price_tag else None

        price_m2 = soup.select_one(".info-data-price span")
        data["price_per_m2"] = price_m2.text.strip() if price_m2 else None
        price = None
        price_m2 = None

        # --- NEW layout ---
        price_section = soup.select_one(".price-features__container")

        if price_section:

            rows = price_section.select(".flex-feature")

            for row in rows:
                text = row.get_text(" ", strip=True).lower()

                if "property price" in text:
                    strong = row.select_one("strong")
                    if strong:
                        price = strong.text.strip()

                elif "price per m²" in text or "price per m2" in text:
                    spans = row.select("span")
                    if len(spans) > 1:
                        price_m2 = spans[1].text.strip()

        # --- FALLBACK: old layout ---
        if not price:
            price_tag = soup.select_one(".info-data-price")
            price = price_tag.text.strip() if price_tag else None

        if not price_m2:
            price_m2_tag = soup.select_one(".info-data-price span")
            price_m2 = price_m2_tag.text.strip() if price_m2_tag else None

        data["price_per_m2"] = price_m2

        area_m2 = None

        features_block = soup.select_one(".info-features")

        if features_block:
            spans = features_block.select("span")

            for sp in spans:
                text = sp.get_text(strip=True)

                if "m²" in text.lower():
                    area_m2 = text
                    break

        data["area"] = area_m2

        # =========================
        # LOCATION TEXT (VISIBLE)
        # =========================

        minor = soup.select_one(".main-info__title-minor")
        data["location_text"] = minor.text.strip() if minor else None

        # =========================
        # DESCRIPTION
        # =========================

        desc = soup.select_one(".comment")
        data["description"] = desc.text.strip() if desc else None

        # =========================
        # JSON-LD STRUCTURED DATA
        # =========================

        def extract_json_ld(soup):
            script = soup.find("script", type="application/ld+json")
            if not script:
                return {}
            try:
                return json.loads(script.string)
            except:
                return {}

        json_ld = extract_json_ld(soup)

        location = {}

        data["full_address"] = ''

        if json_ld:
            address = json_ld.get("address", {})
            # location["street"] = address.get("streetAddress")
            # location["postal_code"] = address.get("postalCode")
            # location["city"] = address.get("addressLocality")
            # location["province"] = address.get("addressRegion")
            # location["country"] = address.get("addressCountry")
            full_address = ", ".join(
                filter(None, [
                    address.get("streetAddress"),
                    address.get("postalCode"),
                    address.get("addressLocality"),
                    address.get("addressRegion"),
                    address.get("addressCountry"),
                ])
            )

            data["full_address"] = full_address

            geo = json_ld.get("geo", {})
            location["latitude"] = geo.get("latitude")
            location["longitude"] = geo.get("longitude")

        # =========================
        # FEATURES
        # =========================

        features = {}

        items = soup.select(
            ".details-property-feature-one li, "
            ".details-property-feature-two li"
        )

        for item in items:
            label = item.select_one("span")
            value = item.select_one("strong")

            if label and value:
                features[label.text.strip()] = value.text.strip()
            else:
                text = item.get_text(" ", strip=True)
                if text:
                    features[text] = True
        feature_list = []

        for key, value in features.items():
            if value is True:
                feature_list.append(key)  # standalone feature
            else:
                feature_list.append(f"{key}: {value}")

        data["features"] = ", ".join(feature_list)

        # =========================
        # ENERGY CERTIFICATE
        # =========================

        energy = soup.select_one(".energy-certificate")
        data["other_energy_certificate"] = energy.text.strip() if energy else None

        energy_data = {}

        # find all feature-two blocks
        blocks = soup.select(".details-property-feature-two")

        for block in blocks:
            headings = [h.get_text(strip=True) for h in block.select("h2")]
            heading = ", ".join(headings) if headings else None

            # check if this block is energy certificate section
            if heading and "certificate" in heading.lower():

                rows = block.select(".details-property_features li")

                for row in rows:
                    spans = row.select("span")

                    if len(spans) < 2:
                        continue

                    label = spans[0].text.strip().replace(":", "")

                    rating = None
                    for cls in spans[-1].get("class", []):
                        if "icon-energy" in cls:
                            rating = cls.split("-")[-1].upper()

                    if label and rating:
                        energy_data[label] = rating

        # convert to comma-separated string
        data["energy_certificate"] = ", ".join(
            f"{k}: {v}" for k, v in energy_data.items()
        ) if energy_data else None


        # =========================
        # PROPERTY STATISTICS
        # =========================

        stats = soup.select(".stats-text")

        for s in stats:
            text = s.text.lower()

            if "updated" in text:
                data["last_updated"] = s.text.strip()

            if "views" in text:
                data["views"] = s.text.strip()

            if "saved" in text:
                data["saved"] = s.text.strip()

        # =========================
        # AGENCY INFO
        # =========================

        agency = soup.select_one(".professional-name")
        data["agency_name"] = agency.text.strip() if agency else None
        data["agency_link"] = ''
        agency_link = soup.select_one(".professional-name a")
        if agency_link:
            try:
                data["agency_link"] = BASE + agency_link["href"] if agency_link else None
            except:
                pass

        # =========================
        # IMAGE URLS
        # =========================

        images = []

        for img in soup.select(".gallery-image img"):
            src = img.get("src") or img.get("data-src")
            if src:
                images.append(src)

        data["images"] = images

        images_dic = []

        for img in soup.select(".gallery-image img"):
            src = img.get("src") or img.get("data-src")

            if src:
                title = (
                        img.get("alt") or
                        img.get("title") or
                        src.split("/")[-1]  # fallback: filename
                )

                images_dic.append({
                    "url": src,
                    "title": title
                })

        data["images_dic"] = images_dic

        # =========================
        # FALLBACK COORDINATES (script search)
        # =========================

        script_text = soup.get_text()

        coords = re.search(r'"latitude":([0-9\.\-]+),"longitude":([0-9\.\-]+)', script_text)
        data["latitude"] = ''
        data["longitude"] = ''
        if coords:
            data["latitude"] = coords.group(1)
            data["longitude"] = coords.group(2)

        # =========================
        # SAVE JSON
        # # =========================
        #
        # with open("property_details.json", "w", encoding="utf-8") as f:
        #     json.dump(data, f, indent=4, ensure_ascii=False)

        data["location_hierarchy"] = extract_location_hierarchy(soup)

        # =========================
        # NEW DEVELOPMENT TYPOLOGIES (if present)
        # =========================

        new_dev_units = []
        data["new_development_units"] = ''
        data["new_development_units_number"] = ''

        dev_title_tag = soup.select_one(".table__tittle")
        dev_title = dev_title_tag.text.strip() if dev_title_tag else None

        rows = soup.select(".table__row")

        for row in rows:
            link = row.get("href")
            full_link = BASE + link if link else None

            cells = row.select(".table__cell")

            if not cells:
                continue

            # Property type & price
            main_cell = row.select_one(".table__go-to-property")
            property_type = None
            price = None

            if main_cell:
                property_type = main_cell.contents[0].strip()
                strong = main_cell.select_one("strong")
                price = strong.text.strip() if strong else None

            # Extract other details
            beds = size = floor = extras = None

            if len(cells) > 1:
                beds = cells[1].text.strip()

            if len(cells) > 2:
                size = cells[2].text.strip()

            if len(cells) > 3:
                floor = cells[3].text.strip()

            if len(cells) > 4:
                extras = cells[4].text.strip()

            new_dev_units.append({
                "development_title": dev_title,
                "property_type": property_type,
                "price": price,
                "bedrooms": beds,
                "size": size,
                "floor": floor,
                "extras": extras,
                "url": full_link
            })
            if full_link not in ['',None,"null"]:
                main_links.append({'url':full_link,'type':'sub-link','main':URL,'area':lnk['area']})

        if new_dev_units:
            data["new_development_units"] = new_dev_units
            data["new_development_units_number"] = len(new_dev_units)

        all_rows.append(data)
        if len(all_rows)==500:
            df = pd.DataFrame(all_rows)
            df.to_csv(f"{province}_chunks/{province}_all_data_{file_iter}_{type}_{month_name}.csv", index=False, encoding="utf-8")
            all_rows=[]
            file_iter+=1


        print(json.dumps(data, indent=2, ensure_ascii=False))
    df = pd.DataFrame(all_rows)
    df.to_csv(f"{province}_chunks/{province}_all_data_last_{file_iter}_{type}_{month_name}.csv", index=False, encoding="utf-8")

def extract_location_hierarchy(soup):
    print("address")
    location_data = {
        "street": None,
        "subdistrict": None,
        "district": None,
        "city": None,
        "province": None
    }

    # find heading containing "Location"
    heading = soup.find(
        lambda tag: tag.name in ["h2", "h3", "h4"]
        and "location" in tag.get_text(strip=True).lower()
    )

    if not heading:
        return None

    print(heading)

    container = heading.find_next()
    # print(container)

    lines = []
    for item in container.find_all(["p", "li", "span"], recursive=True):
        text = item.get_text(strip=True)
        if text:
            lines.append(text)

    # for line in lines:
    #     low = line.lower()
    #     print(low)
    #     if low.startswith("subdistrict"):
    #         location_data["subdistrict"] = line.replace("Subdistrict", "").strip()
    #
    #     elif low.startswith("district"):
    #         location_data["district"] = line.replace("District", "").strip()
    #
    #     elif "," in line:
    #         location_data["province"] = line
    #
    #     elif location_data["street"] is None:
    #         location_data["street"] = line
    #
    #     else:
    #         location_data["city"] = line

    # 🔹 convert to single string
    location_string = ", ".join(
        filter(None, lines)
    )

    return location_string

def get_individual_localtion_links():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = """userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome=If_XoFNzpFBqvlfAVE0~6U1HjW3CnItXQpCU6eu7jKHH71jD3yC65yPHkOwVvSeUsjMd3xhL3RwxCAnA~dneWm6bkB7cztb5K3f8~W5LDgcP3sHn23sSCXPSHjlQeeMW"""

    url = "https://www.idealista.com/en/venta-viviendas/alicante/municipios"
    base_url = "https://www.idealista.com"
    response = requests.get(
        url,
        headers=headers,
        cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
    )



    soup = BeautifulSoup(response.text, "lxml")

    data = []

    # locate main list
    location_list = soup.select_one("#location_list")

    for letter_block in location_list.find_all("li", recursive=False):

        letter_tag = letter_block.select_one(".location_letter")
        if not letter_tag:
            continue

        letter = letter_tag.text.strip()

        municipalities = letter_block.select("ul li")

        for m in municipalities:
            name_tag = m.find("a")
            count_tag = m.find("span")

            if not name_tag:
                continue

            name = name_tag.text.strip()
            link = base_url + name_tag["href"]

            count = None
            if count_tag:
                count = int(count_tag.text.replace(",", "").strip())

            data.append({
                "letter": letter,
                "municipality": name,
                "properties": count,
                "link": link
            })

    print(f"Total municipalities: {len(data)}")

    df = pd.DataFrame(data)
    df.to_csv("alicante_municipalities.csv", index=False, encoding="utf-8")

    print("Saved alicante_municipalities.csv")

def get_html(province,type,month_name):
    os.makedirs("region", exist_ok=True)

    file = pd.read_csv(f"{province}_municipalities_{type}_{month_name}.csv")

    BASE = "https://www.idealista.com"

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = f"""userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome={datadome}"""

    all_properties = []
    seen_urls = set()
    done_properties=[]

    def extract_cards(html, sub_region):
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".item-info-container")

        extracted = []

        for card in cards:

            title_tag = card.select_one(".item-link")
            if not title_tag:
                continue

            property_url = BASE + title_tag["href"]

            # avoid duplicates
            if property_url in seen_urls:
                continue

            seen_urls.add(property_url)

            # 🔹 Agency info
            agency_tag = card.select_one(".logo-branding a")
            agency_name = agency_tag.get("title") if agency_tag else None
            agency_url = BASE + agency_tag["href"] if agency_tag else None

            logo_tag = card.select_one(".logo-branding img")
            agency_logo = logo_tag["src"] if logo_tag else None

            # 🔹 Title
            title = title_tag.text.strip()

            # 🔹 Price
            price_tag = card.select_one(".item-price")
            price = price_tag.text.strip() if price_tag else None

            # 🔹 Details
            details = [d.text.strip() for d in card.select(".item-detail")]

            bedrooms = None
            size = None
            extras = []

            for d in details:
                if "bed" in d.lower():
                    bedrooms = d
                elif "m²" in d.lower():
                    size = d
                else:
                    extras.append(d)

            # 🔹 Description
            desc_tag = card.select_one(".item-description p")
            description = desc_tag.text.strip() if desc_tag else None

            # 🔹 Contact buttons
            has_call_button = bool(card.select_one(".phone-btn"))
            has_email_button = bool(card.select_one(".email-btn"))

            extracted.append({
                "sub_region": sub_region,
                "title": title,
                "price": price,
                "bedrooms": bedrooms,
                "size_m2": size,
                "extra_features": ", ".join(extras),
                "description": description,
                "property_url": property_url,
                "agency_name": agency_name,
                "agency_url": agency_url,
                "agency_logo": agency_logo,
                "call_available": has_call_button,
                "email_available": has_email_button
            })

        return extracted


    for _, row in file.iterrows():

        sub_region = row["municipality"]


        sub_region = sub_region.replace('/','-')

        sub_link = row["link"]

        if sub_region in done_properties:
            continue
        done_properties.append(sub_region)
        expected_count = int(str(row["properties"]).replace(",", "")) if pd.notna(row["properties"]) else None

        print(f"\n====== {sub_region} ======")

        page = 1
        collected = 0

        while True:

            if page == 1:
                url = sub_link
            else:
                url = sub_link.rstrip("/") + f"/pagina-{page}.htm"

            print("Scraping:", url)

            r = requests.get(
            url,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )


            if r.status_code != 200:
                print("Blocked or finished")
                break

            listings = extract_cards(r.text, sub_region)

            if not listings:
                break

            all_properties.extend(listings)
            collected += len(listings)

            print(f"Collected {collected} listings and expected count {expected_count}")

            # stop if reached expected count
            if expected_count and collected >= expected_count:
                print("Reached expected count")
                break

            page += 1
            time.sleep(1)

        # save to CSV
        df = pd.DataFrame(all_properties)
        df.to_csv(f"region/{sub_region}_{province}_properties_full_{type}_{month_name}.csv", index=False)

        print("\nSaved all",f" for {province} ", len(df), "properties")

def get_indivdual_last_listing(province,month):
    print("Getting individual listing for ",province)
    os.makedirs(f"{province}_chunks", exist_ok=True)

    # folder containing your csv files
    folder_path = f"{province}_chunks/"  # change if needed
    file_iter= 1
    all_links = []

    # find files containing "province" in name
    #files = glob.glob(folder_path + "*province*.csv")
    files = glob.glob(folder_path + "*.csv")

    files = [f for f in files
             if f"{province}" in os.path.basename(f).lower()
             and "new" in os.path.basename(f).lower()
             and month in os.path.basename(f).lower()]
    all_rows = []
    main_links = []
    print(files)
    for file in files:
        print(file)
        try:
            xls = pd.read_csv(file)
        except:
            print("error in ",file)


        df = xls[
            xls['new_development_units_number'].notna() &
            (xls['new_development_units_number'] != '')
            ]

        print(len(df))


        # change column name if needed
        for col in df['new_development_units']:
            data = col.replace("'",'"').replace("None", "null")
            json_data = json.loads(data)
            for obj in json_data:
                print(obj)
                lnk = obj["url"]
                print(lnk)
                if lnk is None:
                    continue

                if lnk in all_links:
                    continue
                ids = re.findall(r'/(\d+)/', lnk)

                property_id = ids[0]

                all_links.append(lnk)
                # if len(all_links) < 7500:
                #     continue
                main_links.append(
                    {'url': lnk, 'type': 'sub flat', 'main': property_id,
                     "area": f"reference from {property_id}"})

    print("Total Links",len(all_links))

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = f"""userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome={datadome}"""


    for lnk in main_links:
        URL=lnk['url']
        print(lnk)
        BASE = "https://www.idealista.com"

        response = requests.get(
            URL,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )
        html = response.text

        soup = BeautifulSoup(response.text, "lxml")

        data = {}
        data['type'] = lnk['type']
        data['main'] = lnk['main']
        data['municipal'] = lnk['area']

        # =========================
        # BASIC INFO
        # =========================

        data["url"] = URL
        data["listing_id"] = URL.rstrip("/").split("/")[-1]

        title_tag = soup.select_one("span.main-info__title-main")
        data["title"] = title_tag.text.strip() if title_tag else None

        price_tag = soup.select_one(".info-data-price")
        data["price"] = price_tag.text.strip() if price_tag else None

        price_m2 = soup.select_one(".info-data-price span")
        data["price_per_m2"] = price_m2.text.strip() if price_m2 else None
        price = None
        price_m2 = None

        # --- NEW layout ---
        price_section = soup.select_one(".price-features__container")

        if price_section:

            rows = price_section.select(".flex-feature")

            for row in rows:
                text = row.get_text(" ", strip=True).lower()

                if "property price" in text:
                    strong = row.select_one("strong")
                    if strong:
                        price = strong.text.strip()

                elif "price per m²" in text or "price per m2" in text:
                    spans = row.select("span")
                    if len(spans) > 1:
                        price_m2 = spans[1].text.strip()

        # --- FALLBACK: old layout ---
        if not price:
            price_tag = soup.select_one(".info-data-price")
            price = price_tag.text.strip() if price_tag else None

        if not price_m2:
            price_m2_tag = soup.select_one(".info-data-price span")
            price_m2 = price_m2_tag.text.strip() if price_m2_tag else None

        data["price_per_m2"] = price_m2

        area_m2 = None

        features_block = soup.select_one(".info-features")

        if features_block:
            spans = features_block.select("span")

            for sp in spans:
                text = sp.get_text(strip=True)

                if "m²" in text.lower():
                    area_m2 = text
                    break

        data["area"] = area_m2

        # =========================
        # LOCATION TEXT (VISIBLE)
        # =========================

        minor = soup.select_one(".main-info__title-minor")
        data["location_text"] = minor.text.strip() if minor else None

        # =========================
        # DESCRIPTION
        # =========================

        desc = soup.select_one(".comment")
        data["description"] = desc.text.strip() if desc else None

        # =========================
        # JSON-LD STRUCTURED DATA
        # =========================

        def extract_json_ld(soup):
            script = soup.find("script", type="application/ld+json")
            if not script:
                return {}
            try:
                return json.loads(script.string)
            except:
                return {}

        json_ld = extract_json_ld(soup)

        location = {}

        data["full_address"] = ''

        if json_ld:
            address = json_ld.get("address", {})
            # location["street"] = address.get("streetAddress")
            # location["postal_code"] = address.get("postalCode")
            # location["city"] = address.get("addressLocality")
            # location["province"] = address.get("addressRegion")
            # location["country"] = address.get("addressCountry")
            full_address = ", ".join(
                filter(None, [
                    address.get("streetAddress"),
                    address.get("postalCode"),
                    address.get("addressLocality"),
                    address.get("addressRegion"),
                    address.get("addressCountry"),
                ])
            )

            data["full_address"] = full_address

            geo = json_ld.get("geo", {})
            location["latitude"] = geo.get("latitude")
            location["longitude"] = geo.get("longitude")

        # =========================
        # FEATURES
        # =========================

        features = {}

        items = soup.select(
            ".details-property-feature-one li, "
            ".details-property-feature-two li"
        )

        for item in items:
            label = item.select_one("span")
            value = item.select_one("strong")

            if label and value:
                features[label.text.strip()] = value.text.strip()
            else:
                text = item.get_text(" ", strip=True)
                if text:
                    features[text] = True
        feature_list = []

        for key, value in features.items():
            if value is True:
                feature_list.append(key)  # standalone feature
            else:
                feature_list.append(f"{key}: {value}")

        data["features"] = ", ".join(feature_list)

        # =========================
        # ENERGY CERTIFICATE
        # =========================

        energy = soup.select_one(".energy-certificate")
        data["other_energy_certificate"] = energy.text.strip() if energy else None

        energy_data = {}

        # find all feature-two blocks
        blocks = soup.select(".details-property-feature-two")

        for block in blocks:
            print(block.select("h2"))
            headings = [h.get_text(strip=True) for h in block.select("h2")]
            heading = ", ".join(headings) if headings else None
            print(heading)

            # check if this block is energy certificate section
            if heading and "certificate" in heading.lower():

                rows = block.select(".details-property_features li")

                for row in rows:
                    spans = row.select("span")

                    if len(spans) < 2:
                        continue

                    label = spans[0].text.strip().replace(":", "")

                    rating = None
                    for cls in spans[-1].get("class", []):
                        if "icon-energy" in cls:
                            rating = cls.split("-")[-1].upper()

                    if label and rating:
                        energy_data[label] = rating

        # convert to comma-separated string
        data["energy_certificate"] = ", ".join(
            f"{k}: {v}" for k, v in energy_data.items()
        ) if energy_data else None


        # =========================
        # PROPERTY STATISTICS
        # =========================

        stats = soup.select(".stats-text")

        for s in stats:
            text = s.text.lower()

            if "updated" in text:
                data["last_updated"] = s.text.strip()

            if "views" in text:
                data["views"] = s.text.strip()

            if "saved" in text:
                data["saved"] = s.text.strip()

        # =========================
        # AGENCY INFO
        # =========================

        agency = soup.select_one(".professional-name")
        data["agency_name"] = agency.text.strip() if agency else None
        data["agency_link"] = ''
        agency_link = soup.select_one(".professional-name a")
        if agency_link:
            try:
                data["agency_link"] = BASE + agency_link["href"] if agency_link else None
            except:
                pass

        # =========================
        # IMAGE URLS
        # =========================

        def extract_images():
            BASE = "https://www.idealista.com/en/obra-nueva/109585063/inmueble/110731848/foto/{}"

            images = []

            for i in range(1, 11):

                url = BASE.format(i)
                print("Fetching:", url)

                r= requests.get(
                    url,
                    headers=headers,
                    cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
                )
                soup = BeautifulSoup(r.text, "lxml")

                # extract jpg source
                for src in soup.select('main.rs-gallery-container source[type="image/jpeg"]'):
                    img = src.get("srcset")
                    if img:
                        img = img.replace("blur/", "")  # optional: remove blur
                        images.append(img)

            # remove duplicates
            images = list(dict.fromkeys(images))

            print(images)


        images = []
        for img in soup.select("#main-multimedia"):
            print(img)
            src = img.get("data-service") or img.get("src")

            if src and src.endswith(".jpg"):
                images.append(src)

        images_dic = []

        # Select actual img tags inside the container
        for img in soup.select("#main-multimedia img"):
            src = img.get("src") or img.get("data-src") or img.get("data-service")

            if src and src.endswith(".jpg"):
                title = (
                        img.get("alt") or
                        img.get("title") or
                        src.split("/")[-1]
                )

                images_dic.append({
                    "url": src,
                    "title": title
                })

        second_level_scrape = scrape_idealista(URL, html)
        data["images"] = second_level_scrape["images"]

        if "images_dic" in second_level_scrape:
            images_dic.extend(second_level_scrape["images_dic"])

        data["images_dic"] = images_dic

        # =========================
        # FALLBACK COORDINATES (script search)
        # =========================

        # regex fallback on raw HTML for lat/lon (sub-flat pages lack JSON-LD geo)
        coords = re.search(r'"latitude"\s*:\s*"?([0-9\.\-]+)"?.*?"longitude"\s*:\s*"?([0-9\.\-]+)"?', html)
        if coords:
            location["latitude"]  = coords.group(1)
            location["longitude"] = coords.group(2)

        data["latitude"]  = second_level_scrape.get("lat") or location.get("latitude") or ""
        data["longitude"] = second_level_scrape.get("lon") or location.get("longitude") or ""
        data["map"]       = second_level_scrape.get("map_link", "")

        # =========================
        # SAVE JSON
        # # =========================
        #
        # with open("property_details.json", "w", encoding="utf-8") as f:
        #     json.dump(data, f, indent=4, ensure_ascii=False)

        data["location_hierarchy"] = extract_location_hierarchy(soup)

        # =========================
        # NEW DEVELOPMENT TYPOLOGIES (if present)
        # =========================

        new_dev_units = []
        data["new_development_units"] = ''
        data["new_development_units_number"] = ''

        dev_title_tag = soup.select_one(".table__tittle")
        dev_title = dev_title_tag.text.strip() if dev_title_tag else None

        rows = soup.select(".table__row")

        for row in rows:
            link = row.get("href")
            full_link = BASE + link if link else None

            cells = row.select(".table__cell")

            if not cells:
                continue

            # Property type & price
            main_cell = row.select_one(".table__go-to-property")
            property_type = None
            price = None

            if main_cell:
                property_type = main_cell.contents[0].strip()
                strong = main_cell.select_one("strong")
                price = strong.text.strip() if strong else None

            # Extract other details
            beds = size = floor = extras = None

            if len(cells) > 1:
                beds = cells[1].text.strip()

            if len(cells) > 2:
                size = cells[2].text.strip()

            if len(cells) > 3:
                floor = cells[3].text.strip()

            if len(cells) > 4:
                extras = cells[4].text.strip()

            new_dev_units.append({
                "development_title": dev_title,
                "property_type": property_type,
                "price": price,
                "bedrooms": beds,
                "size": size,
                "floor": floor,
                "extras": extras,
                "url": full_link
            })
            if full_link not in ['',None,"null"]:
                main_links.append({'url':full_link,'type':'sub-link','main':URL,'area':lnk['area']})

        if new_dev_units:
            data["new_development_units"] = new_dev_units
            data["new_development_units_number"] = len(new_dev_units)

        all_rows.append(data)
        if len(all_rows)==500:
            df = pd.DataFrame(all_rows)
            df.to_csv(f"{province}_chunks/{province}_all_data_sub_flats_{month}_{file_iter}.csv", index=False, encoding="utf-8")
            all_rows=[]
            file_iter+=1


        print(json.dumps(data, indent=2, ensure_ascii=False))
    df = pd.DataFrame(all_rows)
    df.to_csv(f"{province}_chunks/{province}_all_data_last_sub_flats_{month}_{file_iter}.csv", index=False, encoding="utf-8")

def get_indivdual_golden_listing():


    # folder containing your csv files
    folder_path = "valencia_chunks/"  # change if needed
    file_iter= 52
    all_links = []

    # find files containing "valencia" in name
    files = glob.glob(folder_path + "*valencia*.csv")
    all_rows = []
    main_links = []
    for file in files:
        print(file)
        df = pd.read_csv(file)

        # ensure columns are strings (prevents errors)
        df['title'] = df['title'].astype(str)
        df['description'] = df['description'].astype(str)

        # ✅ create unit_type column
        df['unit_type'] = df['title'].str.split('for').str[0].str.strip()

        # ✅ create delivery_date column
        df['features'] = df['features'].astype(str)
        df['delivery_date'] = df['features'].apply(
            lambda x: 'delivery '+ x.lower().split('delivery')[-1].split(',')[0].strip()
            if 'delivery' in x.lower()
            else None
        )

        all_rows.append(df)

    # ✅ combine all files
    final_df = pd.concat(all_rows, ignore_index=True)

    # ✅ save to single file
    final_df.to_csv("valencia_combined.csv", index=False)

    print("✅ Combined file created: valencia_combined.csv")

def get_individual_localtion_new_home_links(province,month):
    os.makedirs(f"{province}_chunks", exist_ok=True)
    os.makedirs("region", exist_ok=True)

    municipal_subs = {"valencia":"valencia-provincia","alicante":"alicante"
                      ,"castellon":"castellon","murcia":"murcia-provincia"}
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = f"""userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome={datadome}"""

    url = f"https://www.idealista.com/en/venta-obranueva/{municipal_subs[province]}/municipios"
    print(url)
    base_url = "https://www.idealista.com"
    response = requests.get(
        url,
        headers=headers,
        cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
    )

    soup = BeautifulSoup(response.text, "lxml")

    _pt = response.text[:3000].lower()
    if (response.status_code in (403, 429)
            or ("datadome" in _pt and "blocked" in _pt)
            or "are you a robot" in _pt
            or ("enable javascript" in _pt and "#location_list" not in response.text)):
        raise RuntimeError(
            f"DataDome blocked the request (HTTP {response.status_code}). "
            "The datadome cookie is invalid or expired — click 'Auto-detect from Browser' "
            "to refresh it, then restart the pipeline."
        )

    data = []

    # locate main list
    location_list = soup.select_one("#location_list")

    if location_list is None:
        raise RuntimeError(
            f"Could not find #location_list on page (HTTP {response.status_code}). "
            "DataDome may be blocking — refresh the datadome cookie and retry."
        )

    for letter_block in location_list.find_all("li", recursive=False):

        letter_tag = letter_block.select_one(".location_letter")
        if not letter_tag:
            continue

        letter = letter_tag.text.strip()

        municipalities = letter_block.select("ul li")

        for m in municipalities:
            name_tag = m.find("a")
            count_tag = m.find("span")

            if not name_tag:
                continue

            name = name_tag.text.strip()
            link = base_url + name_tag["href"]

            count = None
            if count_tag:
                count = int(count_tag.text.replace(",", "").strip())

            data.append({
                "letter": letter,
                "municipality": name,
                "properties": count,
                "link": link
            })

    print(f"Total municipalities: {len(data)}")

    df = pd.DataFrame(data)
    df.to_csv(f"{province}_municipalities_new_{month}.csv", index=False, encoding="utf-8")

    print(f"Saved {province} municipalities.csv")

def final_sheet():
    import pandas as pd
    import ast
    import re
    import numpy as np

    # =========================
    # FILE PATH
    # =========================
    file_path = "valencia_chunks/valencia_all_data_74_new.csv"

    # =========================
    # LOAD DATA
    # =========================
    df = pd.read_csv(file_path, low_memory=False)

    # =========================
    # FILTER ONLY DEVELOPMENTS
    # =========================
    df = df[
        df["new_development_units"].notna() &
        (df["new_development_units"].astype(str).str.strip() != "") &
        (df["new_development_units"].astype(str).str.strip() != "[]")
        ].copy()

    print("Developments found:", len(df))

    # =========================
    # PARSE UNIT DATA
    # =========================
    def parse_units(x):
        try:
            return ast.literal_eval(x)
        except:
            return None

    df["new_development_units"] = df["new_development_units"].apply(parse_units)

    # explode rows
    units_df = df.explode("new_development_units").reset_index(drop=True)

    # flatten dictionaries
    unit_details = pd.json_normalize(units_df["new_development_units"])

    units_df = pd.concat(
        [units_df.drop(columns=["new_development_units"]), unit_details],
        axis=1
    )

    # remove duplicate columns if any
    units_df = units_df.loc[:, ~units_df.columns.duplicated()]

    # =========================
    # AUTO-DETECT PRICE & AREA
    # =========================
    price_col = None
    area_col = None

    for col in units_df.columns:
        c = col.lower()
        if "price" in c:
            price_col = col
        if "size" in c or "area" in c or "m2" in c:
            area_col = col

    if price_col is None or area_col is None:
        raise Exception("Price or area column not found")

    print("Using:", price_col, "and", area_col)

    # =========================
    # CLEAN NUMERIC VALUES
    # =========================
    def clean_numeric(x):
        if x is None or (isinstance(x, float) and np.isnan(x)) or x=='':
            return ''
        print(x)
        return float(re.sub(r"[^\d]", "", str(x)))

    units_df["price"] = units_df[price_col].apply(clean_numeric)
    units_df["size"] = units_df[area_col].apply(clean_numeric)

    units_df["price"] = pd.to_numeric(units_df["price"])
    units_df["size"] = pd.to_numeric(units_df["size"])

    # units_df = units_df.dropna(subset=["price"])

    # =========================
    # DETECT UNIT TYPE
    # =========================
    def detect_unit_type(text):
        text = str(text).lower()

        if "studio" in text:
            return "Studio"
        if "penthouse" in text or "ático" in text:
            return "Penthouse"
        if "1" in text:
            return "1BR"
        if "2" in text:
            return "2BR"
        if "3" in text:
            return "3BR"
        if "4" in text:
            return "4BR"
        if "5" in text:
            return "5BR"
        return "Other"

    if "property_type" in units_df.columns:
        units_df["unit_type"] = units_df["property_type"].apply(detect_unit_type)
    else:
        units_df["unit_type"] = "Other"

    # =========================
    # KEEP HIGHEST PRICE PER TYPE
    # =========================
    units_filtered = (
        units_df
        .sort_values("price", ascending=False)
        .drop_duplicates(subset=["listing_id", "unit_type"])
    )

    # =========================
    # MATRIX STRUCTURE
    # =========================
    bedroom_order = ["Studio", "1BR", "2BR", "3BR", "4BR", "5BR", "Penthouse"]

    # price matrix
    price_matrix = units_filtered.pivot(
        index="listing_id",
        columns="unit_type",
        values="price"
    ).reindex(columns=bedroom_order)

    # area matrix
    area_matrix = units_filtered.pivot(
        index="listing_id",
        columns="unit_type",
        values="size"
    ).reindex(columns=bedroom_order)

    # price per m²
    price_m2_matrix = price_matrix.divide(area_matrix)

    # rename columns
    price_matrix.columns = [f"{c} Price €" for c in price_matrix.columns]
    area_matrix.columns = [f"{c} Area m²" for c in area_matrix.columns]
    price_m2_matrix.columns = [f"{c} €/m²" for c in price_m2_matrix.columns]

    # =========================
    # BASE INFO (ONE ROW PER DEVELOPMENT)
    # =========================
    base_cols = ["listing_id"]

    for col in ["location", "property_type", "url"]:
        if col in df.columns:
            base_cols.append(col)

    base_info = df[base_cols]

    # =========================
    # MERGE FINAL SHEET
    # =========================
    final_df = (
        base_info
        .merge(price_matrix, on="listing_id", how="left")
        .merge(area_matrix, on="listing_id", how="left")
        .merge(price_m2_matrix, on="listing_id", how="left")
    )

    final_df = final_df.sort_values("listing_id")

    # =========================
    # EXPORT
    # =========================
    output_file = "valencia_final_sheet.xlsx"
    final_df.to_excel(output_file, index=False)

    print("\n✅ SUCCESS — Final sheet created")
    print("Saved as:", output_file)

def final_sheet_1():

    import pandas as pd
    import ast
    import re
    import numpy as np

    file_path = "valencia_chunks/valencia_all_data_74_new.csv"

    # =========================
    # LOAD DATA
    # =========================
    df = pd.read_csv(file_path, low_memory=False)
    df.columns = df.columns.str.strip()

    # =========================
    # FILTER DEVELOPMENTS
    # =========================
    df = df[
        df["new_development_units"].notna() &
        (df["new_development_units"].astype(str).str.strip() != "") &
        (df["new_development_units"].astype(str).str.strip() != "[]")
    ].copy()

    print("Developments found:", len(df))

    # =========================
    # SAFE PARSER
    # =========================
    def parse_units_safe(text):
        if not isinstance(text, str):
            return None

        text = re.sub(r'"extras":\s*,', '"extras": null,', text)

        try:
            return ast.literal_eval(text)
        except:
            return None

    # =========================
    # CLEAN PRICE
    # =========================
    def clean_price(value):
        if not isinstance(value, str):
            return None

        if "ask" in value.lower() or "request" in value.lower():
            return None

        match = re.search(r'[\d.,]+', value)
        if match:
            num = match.group(0)
            num = num.replace('.', '').replace(',', '')
            return float(num)

        return None

    # =========================
    # CLEAN AREA
    # =========================
    def clean_area(value):
        if not isinstance(value, str):
            return None

        match = re.search(r'\d+', value)
        return float(match.group()) if match else None

    # =========================
    # UNIT TYPE DETECTION
    # =========================
    def detect_unit_type(unit):
        text = unit['property_type'].lower() +  unit['bedrooms'].lower() if unit['bedrooms'] else ''
        if "studio" in text:
            return "Studio"

        if "penthouse" in text or "ático" in text:
            return "Penthouse"

        match = re.search(r'(\d+)', text)
        if match:
            n = int(match.group(1))
            if 1 <= n <= 5:
                return f"{n}BR"

        return None

    # =========================
    # EXTRACT UNIT DATA
    # =========================
    rows = []
    unit_counts = {}

    for _, row in df.iterrows():

        units = parse_units_safe(row["new_development_units"])
        if not units:
            continue

        # count total units
        unit_counts[row["listing_id"]] = len(units)

        for u in units:
            print(row['title'],u)
            unit_type = detect_unit_type(u)
            print(unit_type)
            if not unit_type:
                print("error")
                continue

            price = clean_price(u.get("price"))
            size  = clean_area(u.get("size"))

            if not price:
                continue

            rows.append({
                "listing_id": row["listing_id"],
                "unit_type": unit_type,
                "price": price,
                "size": size
            })

    units_df = pd.DataFrame(rows)

    print("Extracted unit rows:", len(units_df))

    # =========================
    # CREATE MATRICES
    # =========================
    bedroom_order = ["Studio","1BR","2BR","3BR","4BR","5BR","Penthouse"]

    price_matrix = (
        units_df.pivot_table(
            index="listing_id",
            columns="unit_type",
            values="price",
            aggfunc="max"
        ).reindex(columns=bedroom_order)
    )

    area_matrix = (
        units_df.pivot_table(
            index="listing_id",
            columns="unit_type",
            values="size",
            aggfunc="max"
        ).reindex(columns=bedroom_order)
    )

    price_m2_matrix = price_matrix.divide(area_matrix)

    price_matrix.columns = [f"{c} Price €" for c in price_matrix.columns]
    area_matrix.columns = [f"{c} Area m²" for c in area_matrix.columns]
    price_m2_matrix.columns = [f"{c} €/m²" for c in price_m2_matrix.columns]

    # =========================
    # PROPERTY TYPE PER DEVELOPMENT
    # =========================
    property_type_map = units_df.groupby("listing_id")["unit_type"].apply(list)

    def classify_property(types):

        result = []

        if "Studio" in types:
            result.append("Studio")

        if any(t.endswith("BR") for t in types):
            result.append("Flat / Apartment")

        if "Penthouse" in types:
            result.append("Penthouse")

        return ", ".join(result)

    property_type_df = property_type_map.apply(classify_property).reset_index()
    property_type_df.columns = ["listing_id", "property_type"]

    # =========================
    # UNIT COUNT TABLE
    # =========================
    unit_count_df = pd.DataFrame(
        list(unit_counts.items()),
        columns=["listing_id", "new_development_units_numbers"]
    )

    # =========================
    # PROPERTY INFO
    # =========================
    df["property_name"] = df["title"].apply(
        lambda x: str(x).split("New home development:")[-1].split("by")[0].strip()
    )

    df["developer"] = df["title"].apply(
        lambda x: str(x).split("by")[-1].strip()
    )

    df["city_area"] = df["location_text"]
    df["province"] = "Valencia"

    info_df = df[[
        "listing_id",
        "property_name",
        "developer",
        "city_area",
        "province"
    ]].drop_duplicates()

    # =========================
    # MERGE FINAL SHEET
    # =========================
    final_df = (
        info_df
        .merge(property_type_df, on="listing_id", how="left")
        .merge(unit_count_df, on="listing_id", how="left")   # NEW COLUMN
        .merge(price_matrix, on="listing_id", how="left")
        .merge(area_matrix, on="listing_id", how="left")
        .merge(price_m2_matrix, on="listing_id", how="left")
    )

    final_df = final_df.sort_values("listing_id")

    # =========================
    # EXPORT
    # =========================
    final_df.to_excel("valencia_final_sheet.xlsx", index=False)

    print("\n✅ FINAL SHEET CREATED")

def final_sheet_all_units(province,month):
    os.makedirs(f"{province}_chunks", exist_ok=True)

    import pandas as pd
    import ast
    import re
    import numpy as np

    features_df = pd.read_excel(f"{province}_all_sub_flats_{month}_units.xlsx")

    print(features_df.columns)

    features_df = features_df[
        ["listing_id", "images","images_dic", "map", "latitude", "longitude", "features"]
    ]

    features_df = features_df.rename(columns={"listing_id":"sub_listing_id"})


    folder_path = f"{province}_chunks/"

    files = glob.glob(folder_path + "*.csv")

    files = [
        f for f in files
        if f"{province}" in os.path.basename(f).lower()
           and "new" in os.path.basename(f).lower()
           and month in os.path.basename(f).lower()
    ]

    print("Files found:", len(files))

    # Combine all files
    df_list = []

    for file in files:
        print("Reading:", file)
        temp_df = pd.read_csv(file, low_memory=False)
        df_list.append(temp_df)

    if not df_list:
        print("No files found.")
    else:
        combined_df = pd.concat(df_list, ignore_index=True)
        print("Total rows combined:", len(combined_df))


    # =========================
    # LOAD DATA
    # =========================
    # df = pd.read_excel(file_path, low_memory=False)
    df = combined_df
    df.columns = df.columns.str.strip()

    # =========================
    # FILTER DEVELOPMENTS
    # =========================
    df = df[
        df["new_development_units"].notna() &
        (df["new_development_units"].astype(str).str.strip() != "") &
        (df["new_development_units"].astype(str).str.strip() != "[]")
    ].copy()

    print("Developments found:", len(df))

    # =========================
    # SAFE PARSER
    # =========================
    def parse_units_safe(text):
        if not isinstance(text, str):
            return None
        text = re.sub(r'"extras":\s*,', '"extras": null,', text)
        try:
            return ast.literal_eval(text)
        except:
            return None

    # =========================
    # CLEAN PRICE
    # =========================
    def clean_price(value):
        if not isinstance(value, str):
            return None

        if "ask" in value.lower() or "request" in value.lower():
            return None

        match = re.search(r'[\d.,]+', value)
        if match:
            num = match.group(0)
            num = num.replace('.', '').replace(',', '')
            return float(num)
        return None

    # =========================
    # CLEAN AREA
    # =========================
    def clean_area(value):
        if not isinstance(value, str):
            return None
        match = re.search(r'\d+', value)
        return float(match.group()) if match else None

    # =========================
    # UNIT TYPE DETECTION
    # =========================
    def detect_unit_type(unit):

        ptype = str(unit.get("property_type", "")).lower()
        bedrooms = str(unit.get("bedrooms", "")).lower()

        text = ptype + " " + bedrooms

        if "studio" in text:
            return "Studio"

        if "penthouse" in text or "ático" in text:
            return "Penthouse"

        match = re.search(r'(\d+)', bedrooms)
        if match:
            n = int(match.group(1))
            if 1 <= n <= 5:
                return f"{n}BR"

        return None

    # =========================
    # EXTRACT UNIT DATA
    # =========================
    rows = []
    unit_counts = {}

    for _, row in df.iterrows():

        units = parse_units_safe(row["new_development_units"])
        if not units:
            continue

        # count total units
        unit_counts[row["listing_id"]] = len(units)

        for u in units:

            unit_type = detect_unit_type(u)
            if not unit_type:
                continue

            price = clean_price(u.get("price"))
            size  = clean_area(u.get("size"))

            if not price:
                continue

            rows.append({
                "listing_id": row["listing_id"],
                "unit_type": unit_type,
                "price": price,
                "size": size,
                "unit_url": u.get("url"),
                "floor": u.get("floor")
            })

    units_df = pd.DataFrame(rows)
    units_df["sub_listing_id"] = (
        units_df["unit_url"]
        .str.split("?").str[0]
        .str.rstrip("/")
        .str.split("/").str[-1]
    )

    print("Units extracted:", len(units_df))

    # =========================
    # CALCULATE €/m²
    # =========================
    units_df["price_per_m2"] = units_df["price"] / units_df["size"]

    # =========================
    # PROPERTY TYPE PER DEVELOPMENT
    # =========================
    property_type_map = units_df.groupby("listing_id")["unit_type"].apply(list)

    def classify_property(types):
        result = []

        if "Studio" in types:
            result.append("Studio")

        if any(t.endswith("BR") for t in types):
            result.append("Flat / Apartment")

        if "Penthouse" in types:
            result.append("Penthouse")

        return ", ".join(result)

    property_type_df = property_type_map.apply(classify_property).reset_index()
    property_type_df.columns = ["listing_id", "property_type"]

    # =========================
    # UNIT COUNT TABLE
    # =========================
    unit_count_df = pd.DataFrame(
        list(unit_counts.items()),
        columns=["listing_id", "new_development_units_numbers"]
    )

    # =========================
    # PROPERTY INFO
    # =========================
    df["property_name"] = df["title"].apply(
        lambda x: str(x).split("New home development:")[-1].split("by")[0].strip()
    )

    df["developer"] = df["title"].apply(
        lambda x: str(x).split("by")[-1].strip()
    )

    df["city_area"] = df["location_hierarchy"]
    df["province"] = province

    def extract_delivery_date(text):
        if not isinstance(text, str):
            return None

        text_low = text.lower()

        # English delivery
        if "delivery" in text_low:
            part = text_low.split("delivery")[-1]
            return "Delivery " + part.split(",")[0].strip()

        # Spanish entrega
        if "entrega" in text_low:
            part = text_low.split("entrega")[-1]
            return "Delivery " + part.split(",")[0].strip()

        return None

    def extract_amenities(text):
        if not isinstance(text, str):
            return None

        txt = text.lower()

        # remove delivery info
        if "delivery" in txt:
            txt = txt.split("delivery")[0]
        if "entrega" in txt:
            txt = txt.split("entrega")[0]

        return txt.strip().title()

    def extract_sub_id(text):

        return text.split("/")[-1].replace("/","")

    # =========================
    # EXTRACT UNITS
    # =========================
    df["delivery_date"] = df["features"].apply(extract_delivery_date)
    df["municipality"] = df["location_text"]
    df["description"] = df["description"]
    df["amenities"] = df["features"].apply(extract_amenities)
    df["esg_certificate"] = (
        df["energy_certificate"]
        .fillna(df["other_energy_certificate"])
        .astype(str)
        .str.strip()
        .replace("", None)
    )


    info_df = df[[
        "listing_id",
        "description",
        "property_name",
        "developer",
        "city_area",
        "municipality",
        "province",
        "delivery_date",
        "amenities",
        "esg_certificate",
        "last_updated"
    ]].drop_duplicates()

    # =========================
    # MERGE FINAL UNIT SHEET
    # =========================
    final_units = (
        units_df
        .merge(info_df, on="listing_id", how="left")
        .merge(property_type_df, on="listing_id", how="left")
        .merge(unit_count_df, on="listing_id", how="left")
    )

    # reorder columns
    final_units = final_units[[
        "listing_id",
        "sub_listing_id",
        "description",
        "property_name",
        "developer",
        "city_area",
        "municipality",
        "province",
        "delivery_date",
        "amenities",
        "esg_certificate",
        "last_updated",
        "property_type",
        "new_development_units_numbers",
        "unit_type",
        "price",
        "size",
        "price_per_m2",
        "floor",
        "unit_url"
    ]]

    # =========================
    # EXPORT
    # =========================
    final_units["sub_listing_id"] = final_units["sub_listing_id"].astype(str)
    features_df["sub_listing_id"] = features_df["sub_listing_id"].astype(str)

    final_units = final_units.merge(
        features_df,
        on="sub_listing_id",
        how="left"
    )

    final_units["amenities"] = (
        final_units["features"]
        .astype(str)
        .str.split(r"(?i)(Consumption|Emissions|In process)", n=1, regex=True)
        .str[0]
        .str.rstrip(", ")
        .str.strip()
    )


    final_units = final_units.drop("features",axis=1)

    final_units = final_units.drop_duplicates()

    final_units.to_excel(f"{province}_all_{month}_units.xlsx", index=False)

    print("\n✅ ALL UNITS SHEET CREATED")

def final_sheet_formatted():

    import pandas as pd
    import ast
    import re

    file_path = "valencia_chunks/valencia_all_data_74_new.csv"

    # =========================
    # LOAD DATA
    # =========================
    df = pd.read_csv(file_path, low_memory=False)
    df.columns = df.columns.str.strip()

    # =========================
    # FILTER DEVELOPMENTS
    # =========================
    df = df[
        df["new_development_units"].notna() &
        (df["new_development_units"].astype(str).str.strip() != "") &
        (df["new_development_units"].astype(str).str.strip() != "[]")
    ].copy()



    print("Developments:", len(df))

    # =========================
    # SAFE PARSER
    # =========================
    def parse_units_safe(text):
        if not isinstance(text, str):
            return None
        text = re.sub(r'"extras":\s*,', '"extras": null,', text)
        try:
            return ast.literal_eval(text)
        except:
            return None

    # =========================
    # CLEANERS
    # =========================
    def clean_price(val):
        if not isinstance(val, str):
            return None
        if "ask" in val.lower() or "request" in val.lower():
            return None
        m = re.search(r'[\d.,]+', val)
        if m:
            return float(m.group().replace('.', '').replace(',', ''))
        return None

    def clean_area(val):
        if not isinstance(val, str):
            return None
        m = re.search(r'\d+', val)
        return float(m.group()) if m else None

    # =========================
    # UNIT TYPE DETECTION
    # =========================
    def detect_unit_type(u):
        ptype = str(u.get("property_type", "")).lower()
        bedrooms = str(u.get("bedrooms", "")).lower()
        text = ptype + " " + bedrooms

        if "studio" in text:
            return "Studio"
        if "penthouse" in text or "ático" in text:
            return "Penthouse"

        m = re.search(r'(\d+)', bedrooms)
        if m:
            return f"{m.group(1)}BR"

        return None

    def classify_property(types):

        result = []

        if "Studio" in types:
            result.append("Studio")

        if any(t.endswith("BR") for t in types):
            result.append("Flat / Apartment")

        if "Penthouse" in types:
            result.append("Penthouse")

        return ", ".join(result)

    def extract_delivery_date(text):
        if not isinstance(text, str):
            return None

        text_low = text.lower()

        # English delivery
        if "delivery" in text_low:
            part = text_low.split("delivery")[-1]
            return "Delivery " + part.split(",")[0].strip()

        # Spanish entrega
        if "entrega" in text_low:
            part = text_low.split("entrega")[-1]
            return "Delivery " + part.split(",")[0].strip()

        return None

    def extract_amenities(text):
        if not isinstance(text, str):
            return None

        txt = text.lower()

        # remove delivery info
        if "delivery" in txt:
            txt = txt.split("delivery")[0]
        if "entrega" in txt:
            txt = txt.split("entrega")[0]

        return txt.strip().title()

    # =========================
    # EXTRACT UNITS
    # =========================
    df["delivery_date"] = df["features"].apply(extract_delivery_date)
    df["municipality"] = df["municipal"]
    df["amenities"] = df["features"].apply(extract_amenities)
    df["esg_certificate"] = (
        df["energy_certificate"]
        .fillna(df["other_energy_certificate"])
        .astype(str)
        .str.strip()
        .replace("", None)
    )
    rows = []
    unit_counts = {}

    for _, row in df.iterrows():

        units = parse_units_safe(row["new_development_units"])
        if not units:
            continue

        unit_counts[row["listing_id"]] = len(units)

        for u in units:
            unit_type = detect_unit_type(u)
            if not unit_type:
                continue

            price = clean_price(u.get("price"))
            size  = clean_area(u.get("size"))

            if not price:
                continue

            rows.append({
                "listing_id": row["listing_id"],
                "unit_type": unit_type,
                "price": price,
                "size": size,
                "price_m2": price / size if size else None
            })



    units_df = pd.DataFrame(rows)

    print("Units extracted:", len(units_df))

    # =========================
    # NUMBER UNITS PER TYPE
    # =========================
    units_df["type_seq"] = (
        units_df.groupby(["listing_id","unit_type"])
        .cumcount() + 1
    )

    # =========================
    # CREATE WIDE TABLES
    # =========================
    price_wide = units_df.pivot(
        index="listing_id",
        columns=["unit_type","type_seq"],
        values="price"
    )

    area_wide = units_df.pivot(
        index="listing_id",
        columns=["unit_type","type_seq"],
        values="size"
    )

    price_m2_wide = units_df.pivot(
        index="listing_id",
        columns=["unit_type","type_seq"],
        values="price_m2"
    )

    property_type_map = (
        units_df.groupby("listing_id")["unit_type"]
        .apply(list)
    )

    # =========================
    # FORMAT COLUMN NAMES
    # =========================
    def format_cols(cols, suffix):
        return [
            f"{ut} {suffix}" if seq == 1 else f"{ut} {suffix} {seq}"
            for ut, seq in cols
        ]

    price_wide.columns = format_cols(price_wide.columns, "Price €")
    area_wide.columns  = format_cols(area_wide.columns, "Area m²")
    price_m2_wide.columns = format_cols(price_m2_wide.columns, "€/m²")

    property_type_df = property_type_map.apply(classify_property).reset_index()
    property_type_df.columns = ["listing_id", "property_type"]

    # =========================
    # PROPERTY INFO
    # =========================
    df["property_name"] = df["title"].apply(
        lambda x: str(x).split("New home development:")[-1].split("by")[0].strip()
    )

    df["developer"] = df["title"].apply(
        lambda x: str(x).split("by")[-1].strip()
    )

    df["city_area"] = df["location_text"]
    df["province"] = "Valencia"

    info_df = df[[
        "listing_id",
        "property_name",
        "developer",
        "city_area",
        "municipality",
        "province",
        "delivery_date",
        "amenities",  # 👈 NEW
        "esg_certificate"  # 👈 NEW
    ]].drop_duplicates()

    # =========================
    # UNIT COUNT TABLE
    # =========================
    unit_count_df = pd.DataFrame(
        list(unit_counts.items()),
        columns=["listing_id","new_development_units_numbers"]
    )

    # =========================
    # MERGE FINAL SHEET
    # =========================
    final_df = (
        info_df
        .merge(property_type_df, on="listing_id", how="left")
        .merge(unit_count_df, on="listing_id", how="left")
        .merge(price_wide, on="listing_id", how="left")
        .merge(area_wide, on="listing_id", how="left")
        .merge(price_m2_wide, on="listing_id", how="left")
    )

    info_cols = [
        "listing_id",
        "property_name",
        "developer",
        "city_area",
        "municipality",
        "province",
        "delivery_date",
        "amenities",
        "esg_certificate",
        "property_type",
        "new_development_units_numbers"  # ✅ ADD THIS
    ]

    price_cols = [c for c in final_df.columns if "Price €" in c]
    area_cols = [c for c in final_df.columns if "Area m²" in c]
    m2_cols = [c for c in final_df.columns if "€/m²" in c]

    new_columns = []

    for col in final_df.columns:
        if col in info_cols:
            new_columns.append(("", col))

        elif col in price_cols:
            new_columns.append(("Sales Price (EUR)", col))

        elif col in area_cols:
            new_columns.append(("Area (M²)", col))

        elif col in m2_cols:
            new_columns.append(("Sales Price (€/m²)", col))

    final_df.columns = pd.MultiIndex.from_tuples(new_columns)

    # =========================
    # EXPORT
    # =========================
    final_df.to_excel("valencia_final_sheet_formatted.xlsx", index=True,
    merge_cells=True )

    print("\n✅ FINAL SHEET CREATED")

def final_sheet_subflats(province,month):
    os.makedirs(f"{province}_chunks", exist_ok=True)

    import pandas as pd
    import ast
    import re
    import numpy as np

    folder_path = f"{province}_chunks/"

    files = glob.glob(folder_path + "*.csv")

    files = [
        f for f in files
        if f"{province}" in os.path.basename(f).lower()
           and "sub_flats" in os.path.basename(f).lower()
           and month in os.path.basename(f).lower()
    ]

    print("Files found:", len(files))

    # Combine all files
    df_list = []

    for file in files:
        print("Reading:", file)
        temp_df = pd.read_csv(file, low_memory=False)
        df_list.append(temp_df)

    if not df_list:
        print("No files found.")
    else:
        combined_df = pd.concat(df_list, ignore_index=True)
        print("Total rows combined:", len(combined_df))

    # file_path = f"{province}_chunks/{province}_all_new_units_combined.xlsx"

    # =========================
    # LOAD DATA
    # =========================
    # df = pd.read_excel(file_path, low_memory=False)
    df = combined_df

    # =========================
    # EXPORT
    # =========================
    combined_df.to_excel(f"{province}_all_sub_flats_{month}_units.xlsx", index=False)


def extract_js_variable(html, var_name):
    print(f"Extracting JS variable '{var_name}'...")
    print(html[:500])  # print first 500 chars for debugging

    start = html.find(f"var {var_name}")
    if start == -1:
        return None

    start = html.find("{", start)

    brace_count = 0
    end = start

    for i in range(start, len(html)):
        if html[i] == "{":
            brace_count += 1
        elif html[i] == "}":
            brace_count -= 1

        if brace_count == 0:
            end = i + 1
            break

    js_object = html[start:end]

    # try plain JSON first (faster, no bytecode issues)
    try:
        return json.loads(js_object)
    except Exception:
        pass

    # try normalising JS → JSON: quote bare keys, replace JS-only tokens
    try:
        fixed = re.sub(r'(?<=[{,])\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:', r'"\1":', js_object)
        fixed = fixed.replace("'", '"').replace('undefined', 'null').replace('Infinity', '0').replace('NaN', '0')
        return json.loads(fixed)
    except Exception:
        pass

    # last resort: js2py (may fail with bytecode error on some environments)
    try:
        import js2py
        ctx = js2py.EvalJs()
        ctx.execute(f"var obj = {js_object};")
        return ctx.obj.to_dict()
    except Exception as e:
        print("JS parsing failed:", e)
        return None

def scrape_idealista(url,html):
    print("Scraping level 2 for maps:", url)
    config = extract_js_variable(html, "config")#
    media = extract_js_variable(html, "adMultimediasInfo")
    detail = extract_js_variable(html, "adDetail")

    result = {}

    # BASIC INFO
    if config:
        result["property_id"] = config.get("propertyId")
        result["location_id"] = config.get("idForm", {}).get("locationId")
        result["agency"] = config.get("adCommercialName")
        result["contact_name"] = config.get("adFirstName")
        result["multimediaCarrousel"] = config.get("multimediaCarrousel")

    if detail:
        result["title"] = detail.get("headerTitle")

    if media:
        result["price"] = media.get("price")

    # FEATURES
    features = []
    if media and "features" in media:
        for f in media["features"]:
            if f:
                if f.get("subText"):
                    features.append(f["subText"])

    result["features"] = features

    # IMAGES
    images = []
    if media and "fullScreenGalleryPics" in media:
        for img in media["fullScreenGalleryPics"]:
            images.append(img.get("imageDataService"))

    result["images"] = images

    images_dic = []

    if media and "fullScreenGalleryPics" in media:
        for idx, img in enumerate(media["fullScreenGalleryPics"], start=1):
            url = img.get("imageDataService")

            if url:
                title = (
                        img.get("tag") or  # sometimes Idealista provides tags
                        img.get("title") or
                        f"image_{idx}"  # fallback
                )

                images_dic.append({
                    "url": url,
                    "title": title
                })

    result["images_dic"] = images_dic

    # FLOOR PLANS
    plans = []
    if media and "plans" in media:
        for p in media["plans"]:
            if p:
                plans.append(p.get("imageDataService"))

    result["floor_plans"] = plans

    # VIDEOS
    videos = []
    if media and "videos" in media:
        for v in media["videos"]:
            if v:
                videos.append(v.get("src"))

    result["videos"] = videos

    # PRIMARY: regex on raw HTML — works even when JS parsing fails
    lat_m = re.search(r'"latitude"\s*:\s*"?(-?[0-9]+\.[0-9]+)"?', html)
    lon_m = re.search(r'"longitude"\s*:\s*"?(-?[0-9]+\.[0-9]+)"?', html)
    if lat_m and lon_m:
        result["lat"] = lat_m.group(1)
        result["lon"] = lon_m.group(1)

    # PRIMARY: Google Maps static URL in page source
    map_m = re.search(r'(https://maps\.googleapis\.com/maps/api/staticmap[^"\'<>\s]+)', html)
    if map_m:
        try:
            map_src = map_m.group(1).replace("&amp;", "&")
            result["map_link"] = map_src
            if not result.get("lat"):
                query = parse_qs(urlparse(map_src).query)
                if "center" in query:
                    lat, lon = query["center"][0].split(",")
                    result["lat"] = lat
                    result["lon"] = lon
        except Exception:
            pass

    # fallback: multimediaCarrousel from parsed config JS
    if not result.get("lat") and result.get("multimediaCarrousel"):
        try:
            map_src = result["multimediaCarrousel"]["map"].get("src", "")
            if not result.get("map_link"):
                result["map_link"] = map_src
            query = parse_qs(urlparse(map_src).query)
            lat, lon = query["center"][0].split(",")
            result["lat"] = lat
            result["lon"] = lon
        except Exception:
            pass

    # fallback: adDetail parsed JS
    if not result.get("lat") and detail:
        try:
            result["lat"] = str(detail.get("latitude") or "")
            result["lon"] = str(detail.get("longitude") or "")
        except Exception:
            pass

    # fallback images: og:image meta tags (used on individual unit pages)
    if not result.get("images"):
        soup_fb = BeautifulSoup(html, "lxml")
        og_imgs = [t.get("content", "") for t in soup_fb.find_all("meta", property="og:image") if t.get("content")]
        result["images"]     = og_imgs
        result["images_dic"] = [{"url": u, "title": f"image_{i+1}"} for i, u in enumerate(og_imgs)]

    return result


def spain_expired_listings(type,month_name):



    df = pd.read_excel("sold_out_properties.xlsx")


    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,en-IN;q=0.7",
        "priority": "u=0, i",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not:A-Brand";v="99.0.0.0", "Microsoft Edge";v="145.0.3800.58", "Chromium";v="145.0.7632.76"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    }

    # ⚠️ paste FULL cookie string exactly as in curl
    cookies_raw = f"""userUUID=f943f60d-9600-4449-8313-247b25d74430; SESSION=0a6f08ce8f40790a~34576419-c444-47bd-8a13-b908f7faebcc; utag_main__sn=1; utag_main_ses_id=1771670467015%3Bexp-session; utag_main__prevTsUrl=https%3A%2F%2Fwww.idealista.com%2Fen%2F%3Bexp-session; utag_main__prevTsReferrer=https://www.bing.com/%3Bexp-session; utag_main__prevTsSource=Search engines%3Bexp-session; utag_main__prevTsCampaign=organicTrafficByTm%3Bexp-session; utag_main__prevTsProvider=%3Bexp-session; utag_main__prevTsNotificationId=%3Bexp-session; utag_main__prevTsProviderClickId=%3Bexp-session; utag_main__ss=0%3Bexp-session; PARAGLIDE_LOCALE=en; lang=en; datadome={datadome}"""

    results = []

    for _,lnk in df.iterrows():
        url=lnk['Link']
        sub_listing = lnk['Unit ID']

        BASE = "https://www.idealista.com"
        print(url)

        response = requests.get(
            url,
            headers=headers,
            cookies={c.split("=")[0]: "=".join(c.split("=")[1:]) for c in cookies_raw.split("; ")}
        )

        soup = BeautifulSoup(response.text, "lxml")

        # ---- Extract Date ----
        date_tag = soup.find("p", class_="deactivated-detail_date")
        removed_date = None

        if date_tag:
            text = date_tag.get_text(strip=True)

            # Extract date using regex
            match = re.search(r'\d{2}/\d{2}/\d{4}', text)
            if match:
                removed_date = match.group(0)

        # Example (modify based on actual HTML)
        sub_tag = soup.find(text=re.compile("Sub listing"))
        if sub_tag:
            sub_listing = sub_tag.strip()

        # Append result
        results.append({
            "url": url,
            "sub_listing": sub_listing,
            "removed_date": removed_date
        })

        print(f"Processed: {url}")

    output_df = pd.DataFrame(results)
    output_df.to_excel(f"expired_listing_{month_name}.xlsx", index=False)


def listing_search():
    folder_path = r"C:\Users\HarinderSingh\PycharmProjects\insights_backend\valencia_chunks"

    result = []

    # Loop through all files in folder
    for file in os.listdir(folder_path):
        if file.endswith(".xls") or file.endswith(".xlsx") or file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)

            try:
                # Read Excel file
                if 'xls' in file:
                    df = pd.read_excel(file_path)
                else:
                    df = pd.read_csv(file_path)


                # Check if 'sub_listing' column exists
                if 'listing_id' in df.columns:
                    # Convert to string for safe comparison
                    if df['listing_id'].astype(str).str.contains("110611134").any():
                       print(file)

            except Exception as e:
                print(f"Error reading {file}: {e}")


if __name__ == "__main__":
    month = datetime.datetime.now().month
    month_name = calendar.month_name[month]

    provices=['castellon','alicante','valencia','murcia']
    province = provices[3]
    type = "new"

    datadome = "VDqZ1CKJ86KTXRs6OC81b7BjGYv0DFELq1IrWHT_SACW_uTvp6SJ0wVwMDeA~HOHNiV2QE30AbPklQFB3Aa9VkJ~TdC68vcEPZ9MZY73d~hjPXD_QVW7XlJnvnFvP5p9"
    ###for castellon till 33

    #get_individual_localtion_new_home_links(province,month_name)  #1st extraction of individual municipalities for provice
    #get_html(province,type,month_name)
    #get_indivdual_listing(province,type,month_name.lower())
    get_indivdual_last_listing(province,month_name.lower()) #alicante is here
    #final_sheet_subflats(province,month_name.lower())
    #final_sheet_all_units(province,month_name.lower()) #valencia is here
    #spain_expired_listings(type,month_name)



