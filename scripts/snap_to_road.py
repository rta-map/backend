"""One-time script to snap street-precision accidents to nearest road via OSRM."""

import argparse
import math
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import SessionLocal, engine
from models import Accident

OSRM_URL = "http://router.project-osrm.org/nearest/v1/driving/{lon},{lat}?number=1"
MAX_SNAP_DISTANCE_M = 100
COMMIT_EVERY = 50
PRINT_EVERY = 100
SLEEP_S = 0.25


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def ensure_column(conn):
    try:
        conn.execute("ALTER TABLE accidents ADD COLUMN road_snapped BOOLEAN DEFAULT 0")
        conn.commit()
        print("Added road_snapped column.")
    except Exception:
        pass  # Column already exists


def fetch_snapped(lat, lon):
    url = OSRM_URL.format(lat=lat, lon=lon)
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    snapped_lon, snapped_lat = data["waypoints"][0]["location"]
    return snapped_lat, snapped_lon


def run(dry_run: bool, limit: int | None):
    raw_conn = engine.raw_connection()
    ensure_column(raw_conn)
    raw_conn.close()

    db = SessionLocal()
    try:
        query = (
            db.query(Accident)
            .filter(Accident.geo_precision == "street")
            .filter((Accident.road_snapped == False) | (Accident.road_snapped == None))
            .order_by(Accident.id)
        )
        if limit:
            query = query.limit(limit)

        rows = query.all()
        total = len(rows)
        print(f"Found {total:,} rows to process.")

        updated = skipped = errors = 0

        for i, accident in enumerate(rows, 1):
            try:
                snapped_lat, snapped_lon = fetch_snapped(accident.lat, accident.lon)
                dist = haversine_m(accident.lat, accident.lon, snapped_lat, snapped_lon)

                if dist <= MAX_SNAP_DISTANCE_M:
                    if dry_run:
                        print(
                            f"  [DRY] id={accident.id} ({accident.lat:.6f},{accident.lon:.6f})"
                            f" -> ({snapped_lat:.6f},{snapped_lon:.6f}) dist={dist:.1f}m"
                        )
                    else:
                        accident.lat = snapped_lat
                        accident.lon = snapped_lon
                    updated += 1
                else:
                    if dry_run:
                        print(
                            f"  [DRY] id={accident.id} snap too far ({dist:.1f}m), keeping original"
                        )
                    skipped += 1

                if not dry_run:
                    accident.road_snapped = True

            except Exception as e:
                print(f"  ERROR id={accident.id}: {e}")
                if not dry_run:
                    accident.road_snapped = True
                errors += 1

            if not dry_run and i % COMMIT_EVERY == 0:
                db.commit()

            if i % PRINT_EVERY == 0:
                print(f"  Progress: {i}/{total} | updated={updated} skipped={skipped} errors={errors}")

            time.sleep(SLEEP_S)

        if not dry_run:
            db.commit()

        print(
            f"\nDone. total={total} updated={updated} skipped(too far)={skipped} errors={errors}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snap street-precision accidents to nearest road.")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    args = parser.parse_args()

    run(dry_run=args.dry_run, limit=args.limit)
