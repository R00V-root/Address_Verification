#!/usr/bin/env python3
"""
state_capitol_verifier.py

- Reads  us_state_capitals.json  (produced earlier)
- Verifies JSON syntax
- Queries the U.S. Census Geocoder API for each address
- Writes  us_state_capitals_verified.json  with CORRECT addresses + new lat/long
-  Re-checks new file and ensures every coordinate pair is unique
"""

from __future__ import annotations
import argparse, json, pathlib, time, urllib.parse, requests

# ───── API constants ───────────────────────────────────────────────────────────=
CENSUS_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    "?address={addr}&benchmark=2020&format=json"
)
DEFAULT_PAUSE = 0.4        # seconds between requests

# ───── helpers ────────────────────────────────────────────────────────────────
def load_json(path: pathlib.Path) -> list[dict]:
    """Parse file & ensure it’s a JSON array of objects."""
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Root element must be a JSON array")
    return data

def geocode(address: str) -> tuple[str, float, float] | None:
    """Return (standardized address, lat, lon) or None if no match."""
    url = CENSUS_URL.format(addr=urllib.parse.quote_plus(address))
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    matches = r.json().get("result", {}).get("addressMatches", [])
    if not matches:
        return None
    best = matches[0]
    coords = best["coordinates"]
    std_addr = best["matchedAddress"]
    return std_addr, float(coords["y"]), float(coords["x"])   # (lat, lon)

def unique_latlon(records: list[dict]) -> bool:
    return len({(r["latitude"], r["longitude"]) for r in records}) == len(records)

# ───── main pipeline ──────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input",  default="us_state_capitals.json",
                    help="original JSON file (default: us_state_capitals.json)")
    ap.add_argument("-o", "--output", default="us_state_capitals_verified.json",
                    help="verified JSON file (default: us_state_capitals_verified.json)")
    ap.add_argument("--pause", type=float, default=DEFAULT_PAUSE,
                    help="seconds to wait between API calls (default: 0.4)")
    args = ap.parse_args()

    inp  = pathlib.Path(args.input).expanduser()
    outp = pathlib.Path(args.output).expanduser()

    # Step 1 & 2: load + syntax check
    records = load_json(inp)
    print(f"✓ Parsed {len(records)} records – JSON syntax OK")

    # Step 3: geocode & correct addresses
    failures: list[str] = []
    for rec in records:
        result = geocode(rec["address"])
        if result is None:
            failures.append(f'{rec["state"]} | {rec["address"]}')
        else:
            std_addr, lat, lon = result
            rec["address"]   = std_addr
            rec["latitude"]  = round(lat, 6)
            rec["longitude"] = round(lon, 6)
        time.sleep(args.pause)

    # Step 4: write verified file
    outp.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"✓ Verified data written ➜ {outp}")

    # Step 5: re-parse & uniqueness check
    _ = load_json(outp)   # parse again; will raise if corrupt
    if unique_latlon(records):
        print("✓ All latitude/longitude pairs are unique")
    else:
        print("⚠️  Duplicate coordinates detected!")

    # Report failures
    if failures:
        print("\nAddresses NOT found by Census Geocoder:")
        for bad in failures:
            print(" •", bad)
    else:
        print("\n✓ Every address matched the Census Geocoder API")

if __name__ == "__main__":
    main()
