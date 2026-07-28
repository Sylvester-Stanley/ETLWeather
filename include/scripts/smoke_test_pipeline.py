"""Standalone smoke test for the ETLWeather pipeline logic — no Airflow, no Docker.

This reproduces exactly what the three DAG tasks do, using plain `requests` and
`psycopg2`, so you can confirm three things independently before blaming Airflow:

    1. the Open-Meteo endpoint the DAG builds is reachable and well-formed
    2. the transform produces the row shape the table expects
    3. the target Postgres is reachable and accepts the INSERT

Run it from the project root (see docs/IMPLEMENTATION_GUIDE.md, Step 0):

    python include/scripts/smoke_test_pipeline.py                # extract + transform only
    python include/scripts/smoke_test_pipeline.py --load         # also write a row
    python include/scripts/smoke_test_pipeline.py --load --dsn postgresql://postgres:postgres@localhost:5434/weather

Exit code is 0 on success, 1 on failure, so it doubles as a CI health check.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

# Kept identical to dags/etlweather.py so the test exercises the real contract.
LATITUDE = "51.5074"
LONGITUDE = "-0.1278"
BASE_URL = "https://api.open-meteo.com"
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5434/weather"

WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog", 51: "Light drizzle",
    53: "Moderate drizzle", 55: "Dense drizzle", 61: "Slight rain",
    63: "Moderate rain", 65: "Heavy rain", 71: "Slight snow fall",
    73: "Moderate snow fall", 75: "Heavy snow fall", 80: "Slight rain showers",
    81: "Moderate rain showers", 82: "Violent rain showers", 95: "Thunderstorm",
}


def ok(msg: str) -> None:
    print(f"  [PASS] {msg}")


def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")


def extract() -> dict[str, Any]:
    """Mirror of extract_weather_data(): build the endpoint and GET it."""
    import requests

    endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
    url = BASE_URL + endpoint
    print(f"\n[1/3] EXTRACT  GET {url}")

    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch weather data: {response.status_code}")

    payload = response.json()
    ok(f"HTTP 200, {len(response.content)} bytes")

    if "current_weather" not in payload:
        raise RuntimeError("response JSON has no 'current_weather' key")
    ok("'current_weather' key present")
    print("  raw payload:")
    print("    " + json.dumps(payload, indent=2)[:600].replace("\n", "\n    "))
    return payload


def transform(weather_data: dict[str, Any]) -> dict[str, Any]:
    """Mirror of transform_weather_data(): flatten to the warehouse row shape."""
    print("\n[2/3] TRANSFORM")
    current_weather = weather_data["current_weather"]
    transformed = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "temperature": current_weather["temperature"],
        "windspeed": current_weather["windspeed"],
        "winddirection": current_weather["winddirection"],
        "weathercode": current_weather["weathercode"],
    }

    expected = {"latitude", "longitude", "temperature", "windspeed",
                "winddirection", "weathercode"}
    if set(transformed) != expected:
        raise RuntimeError(f"unexpected keys: {set(transformed) ^ expected}")
    ok(f"row has the expected {len(expected)} columns")

    code = transformed["weathercode"]
    print(f"  temperature   : {transformed['temperature']} C")
    print(f"  windspeed     : {transformed['windspeed']} km/h")
    print(f"  winddirection : {transformed['winddirection']} deg")
    print(f"  weathercode   : {code} -> {WMO_CODES.get(code, 'unmapped')}")

    # The same sanity rules encoded in include/sql/verify_weather_data.sql.
    if not -90 <= float(transformed["temperature"]) <= 60:
        raise RuntimeError(f"temperature implausible: {transformed['temperature']}")
    if float(transformed["windspeed"]) < 0:
        raise RuntimeError("negative windspeed")
    if not 0 <= float(transformed["winddirection"]) <= 360:
        raise RuntimeError("winddirection outside 0-360")
    ok("all range checks passed")

    # Documents a real quirk: these two are strings, not floats.
    print(f"  note: latitude is {type(transformed['latitude']).__name__!r} "
          f"({transformed['latitude']!r}) — Postgres coerces it to FLOAT on insert")
    return transformed


def load(row: dict[str, Any], dsn: str) -> None:
    """Mirror of load_weather_data(): create the table if needed, then insert."""
    import psycopg2

    print(f"\n[3/3] LOAD  {dsn.rsplit('@', 1)[-1]}")
    conn = psycopg2.connect(dsn)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        latitude FLOAT,
        longitude FLOAT,
        temperature FLOAT,
        windspeed FLOAT,
        winddirection FLOAT,
        weathercode INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    ok("table weather_data present")

    cursor.execute("""
    INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (row["latitude"], row["longitude"], row["temperature"],
          row["windspeed"], row["winddirection"], row["weathercode"]))
    conn.commit()
    ok("1 row inserted and committed")

    cursor.execute("SELECT COUNT(*) FROM weather_data;")
    total = cursor.fetchone()[0]
    cursor.execute("""
        SELECT timestamp, temperature, windspeed, weathercode
        FROM weather_data ORDER BY timestamp DESC LIMIT 1;
    """)
    latest = cursor.fetchone()
    print(f"  total rows now : {total}")
    print(f"  newest row     : {latest}")

    cursor.close()
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--load", action="store_true",
                        help="also insert into Postgres (needs the weather_db container running)")
    parser.add_argument("--dsn", default=DEFAULT_DSN,
                        help=f"Postgres DSN for --load (default: {DEFAULT_DSN})")
    args = parser.parse_args()

    print("=" * 68)
    print("ETLWeather pipeline smoke test")
    print("=" * 68)

    try:
        payload = extract()
        row = transform(payload)
        if args.load:
            load(row, args.dsn)
        else:
            print("\n[3/3] LOAD  skipped (pass --load to write to Postgres)")
    except Exception as exc:  # noqa: BLE001 - top-level reporter
        print(f"\n{type(exc).__name__}: {exc}")
        fail("smoke test failed")
        return 1

    print("\n" + "=" * 68)
    print("RESULT: all stages passed")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    sys.exit(main())
