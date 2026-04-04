#!/usr/bin/env python3
"""Stage 1: Fetch data from Square APIs + Open-Meteo weather.

Caches all responses to data/ as JSON files.
Re-run safely — checks file age before re-fetching (use --force to bypass).

Usage:
    python3 fetch_data.py           # Fetch all (skip if cached < 1hr)
    python3 fetch_data.py --force   # Force re-fetch everything
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────
ENV_PATH = "/Users/ross/projects/station/menu-manager/.env.local"
BASE_URL = "https://connect.squareup.com/v2"
LOCATION_ID = "67H0ZPV2M9JPV"
STATION_MENU_ID = "7SZUNIBCQEUANKT7GB3R6JXW"
OPEN_DATE = "2026-02-22"
DATA_DIR = Path(__file__).parent / "data"
CACHE_MAX_AGE = 3600  # 1 hour

# Tuxedo Park, NY
TUXEDO_LAT = 41.1965
TUXEDO_LNG = -74.1968


def get_token():
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("SQUARE_ACCESS_TOKEN="):
                return line.strip().split("=", 1)[1]
    raise RuntimeError("SQUARE_ACCESS_TOKEN not found in .env.local")


def api_request(method, path, token, body=None):
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def is_fresh(filepath):
    if not filepath.exists():
        return False
    age = time.time() - filepath.stat().st_mtime
    return age < CACHE_MAX_AGE


def save_json(filepath, data):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, default=str, indent=2)
    print(f"  Saved {filepath.name} ({filepath.stat().st_size:,} bytes)")


# ── Square Orders ───────────────────────────────────────────────────────
def fetch_orders(token):
    """Fetch all completed orders since opening day."""
    out = DATA_DIR / "orders.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  orders.json is fresh, skipping (use --force)")
        return

    print("  Fetching orders from Square...")
    all_orders = []
    cursor = None
    page = 0

    while True:
        page += 1
        body = {
            "location_ids": [LOCATION_ID],
            "query": {
                "filter": {
                    "date_time_filter": {
                        "created_at": {
                            "start_at": f"{OPEN_DATE}T00:00:00Z",
                        }
                    },
                    "state_filter": {"states": ["COMPLETED"]},
                },
                "sort": {"sort_field": "CREATED_AT", "sort_order": "ASC"},
            },
            "limit": 500,
        }
        if cursor:
            body["cursor"] = cursor

        data = api_request("POST", "/orders/search", token, body)
        orders = data.get("orders", [])
        all_orders.extend(orders)
        print(f"    Page {page}: {len(orders)} orders (total: {len(all_orders)})")

        cursor = data.get("cursor")
        if not cursor:
            break

    save_json(out, {"orders": all_orders, "fetched_at": datetime.now(timezone.utc).isoformat()})


# ── Square Payments ─────────────────────────────────────────────────────
def fetch_payments(token):
    """Fetch all payments (for tip data)."""
    out = DATA_DIR / "payments.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  payments.json is fresh, skipping")
        return

    print("  Fetching payments from Square...")
    all_payments = []
    cursor = None
    page = 0

    while True:
        page += 1
        params = {
            "location_id": LOCATION_ID,
            "begin_time": f"{OPEN_DATE}T00:00:00Z",
            "sort_order": "ASC",
            "limit": "100",
        }
        if cursor:
            params["cursor"] = cursor

        path = "/payments?" + urllib.parse.urlencode(params)
        data = api_request("GET", path, token)
        payments = data.get("payments", [])
        all_payments.extend(payments)
        print(f"    Page {page}: {len(payments)} payments (total: {len(all_payments)})")

        cursor = data.get("cursor")
        if not cursor:
            break

    save_json(out, {"payments": all_payments, "fetched_at": datetime.now(timezone.utc).isoformat()})


# ── Square Catalog ──────────────────────────────────────────────────────
def fetch_catalog(token):
    """Fetch catalog for item name/category mapping."""
    out = DATA_DIR / "catalog.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  catalog.json is fresh, skipping")
        return

    print("  Fetching catalog from Square...")
    all_objects = []
    cursor = None

    while True:
        path = "/catalog/list?types=ITEM,CATEGORY,MODIFIER_LIST"
        if cursor:
            path += f"&cursor={cursor}"
        data = api_request("GET", path, token)
        all_objects.extend(data.get("objects", []))
        cursor = data.get("cursor")
        if not cursor:
            break

    print(f"    {len(all_objects)} catalog objects")
    save_json(out, {"objects": all_objects, "fetched_at": datetime.now(timezone.utc).isoformat()})


# ── Square Team Members ─────────────────────────────────────────────────
def fetch_team_members(token):
    """Fetch team members for staff name mapping."""
    out = DATA_DIR / "team_members.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  team_members.json is fresh, skipping")
        return

    print("  Fetching team members from Square...")
    body = {
        "query": {
            "filter": {
                "location_ids": [LOCATION_ID],
            }
        }
    }
    data = api_request("POST", "/team-members/search", token, body)
    members = data.get("team_members", [])
    print(f"    {len(members)} team members")
    save_json(out, {"team_members": members, "fetched_at": datetime.now(timezone.utc).isoformat()})


# ── Square Labor Shifts ─────────────────────────────────────────────────
def fetch_shifts(token):
    """Fetch labor shifts for staff scheduling data."""
    out = DATA_DIR / "shifts.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  shifts.json is fresh, skipping")
        return

    print("  Fetching labor shifts from Square...")
    all_shifts = []
    cursor = None
    page = 0

    while True:
        page += 1
        body = {
            "query": {
                "filter": {
                    "location_ids": [LOCATION_ID],
                    "start": {
                        "start_at": f"{OPEN_DATE}T00:00:00Z",
                    },
                },
                "sort": {"field": "START_AT", "order": "ASC"},
            },
            "limit": 200,
        }
        if cursor:
            body["cursor"] = cursor

        data = api_request("POST", "/labor/shifts/search", token, body)
        shifts = data.get("shifts", [])
        all_shifts.extend(shifts)
        print(f"    Page {page}: {len(shifts)} shifts (total: {len(all_shifts)})")

        cursor = data.get("cursor")
        if not cursor:
            break

    save_json(out, {"shifts": all_shifts, "fetched_at": datetime.now(timezone.utc).isoformat()})


# ── Weather (Open-Meteo) ───────────────────────────────────────────────
def fetch_weather():
    """Fetch historical weather for Tuxedo Park, NY."""
    out = DATA_DIR / "weather.json"
    if is_fresh(out) and "--force" not in sys.argv:
        print(f"  weather.json is fresh, skipping")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"  Fetching weather {OPEN_DATE} to {today}...")

    # Try archive API first for bulk historical data
    params = {
        "latitude": TUXEDO_LAT,
        "longitude": TUXEDO_LNG,
        "start_date": OPEN_DATE,
        "end_date": today,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,snowfall_sum,weathercode,windspeed_10m_max",
        "hourly": "temperature_2m,precipitation,weathercode",
        "timezone": "America/New_York",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        print(f"    {len(data.get('daily', {}).get('time', []))} days of weather data")
        save_json(out, data)
    except Exception as e:
        print(f"    Archive API error: {e}")
        # Fallback: try forecast API for recent days
        params_forecast = {
            "latitude": TUXEDO_LAT,
            "longitude": TUXEDO_LNG,
            "past_days": 42,
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,snowfall_sum,weathercode,windspeed_10m_max",
            "hourly": "temperature_2m,precipitation,weathercode",
            "timezone": "America/New_York",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
        }
        url2 = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params_forecast)
        req2 = urllib.request.Request(url2)
        with urllib.request.urlopen(req2) as resp2:
            data2 = json.loads(resp2.read())
        print(f"    Forecast API fallback: {len(data2.get('daily', {}).get('time', []))} days")
        save_json(out, data2)


# ── Main ────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Station Sales Analytics — Data Fetch")
    print("=" * 60)

    token = get_token()

    print("\n[1/4] Orders")
    fetch_orders(token)

    print("\n[2/4] Payments")
    fetch_payments(token)

    print("\n[3/6] Catalog")
    fetch_catalog(token)

    print("\n[4/6] Team Members")
    fetch_team_members(token)

    print("\n[5/6] Labor Shifts")
    fetch_shifts(token)

    print("\n[6/6] Weather")
    fetch_weather()

    print("\nDone! Data cached in data/")


if __name__ == "__main__":
    main()
