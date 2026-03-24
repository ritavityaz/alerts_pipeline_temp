import json
import os
import csv
import asyncio

import aiohttp

from .config import (
    OREF_ALERTS_HISTORY_URL,
    MAX_CONCURRENT_CITY_FETCHES,
    S3_BUCKET,
    s3_client,
)

_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://www.oref.org.il/",
    "X-Requested-With": "XMLHttpRequest",
}


async def fetch_api(session, params):
    """Fetch alerts from the Pikud HaOref API."""
    async with session.get(OREF_ALERTS_HISTORY_URL, params=params, headers=_API_HEADERS) as resp:
        if resp.status != 200:
            print(f"  API returned status {resp.status} for {params}")
            return []
        text = (await resp.text()).strip()
        if not text:
            return []
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            print(f"  Failed to parse JSON for {params}: {text[:200]}")
            return []


async def fetch_latest():
    """Fetch last 24 hours of alerts (single request)."""
    async with aiohttp.ClientSession() as session:
        data = await fetch_api(session, {"lang": "he", "mode": "1"})
    print(f"Fetched {len(data)} alerts (last 24h)")
    return data


async def fetch_all_cities(cities):
    """Fetch last month per city (initialization). Returns deduplicated alerts."""
    all_alerts = {}
    sem = asyncio.Semaphore(MAX_CONCURRENT_CITY_FETCHES)

    async def fetch_city(session, city, idx):
        async with sem:
            data = await fetch_api(session, {
                "lang": "he",
                "mode": "3",
                "city_0": city,
            })
            for a in data:
                rid = a.get("rid")
                if rid:
                    all_alerts[rid] = a
            if idx % 100 == 0:
                print(f"  Fetched {idx}/{len(cities)} cities, {len(all_alerts)} unique alerts so far")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, i) for i, city in enumerate(cities)]
        await asyncio.gather(*tasks)

    print(f"Initialization complete: {len(all_alerts)} unique alerts from {len(cities)} cities")
    return list(all_alerts.values())


def load_cities():
    """Load city list from bundled cities.csv."""
    cities = []
    csv_path = os.path.join(os.path.dirname(__file__), "cities.csv")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row:
                cities.append(row[0])
    return cities


def load_geo_maps():
    """Load city→zone_en and city→name_en mappings from zones.geojson on S3."""
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key="optimized/zones.geojson")
    gj = json.loads(resp["Body"].read())
    zone_map = {}
    name_en_map = {}
    for feat in gj["features"]:
        p = feat["properties"]
        zone_map[p["name_he"]] = p.get("zone_en", "")
        name_en_map[p["name_he"]] = p.get("name_en", "")
    return zone_map, name_en_map
