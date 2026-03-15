"""One-time script to parse highway km from 'place' column into highway_km."""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import SessionLocal
from models import Accident

# Matches patterns like:
#   61-ՐԴ ԿՄ-ԻՆ   19.5-ՐԴ ԿՄ   7-րդ կм-ին   1,5-րդ կм-ին
#   10․2-րդ կм-ին  11-13-րդ կм-ին  104-րդ կ/մ-ին  Մ-16 12-ՐԴ ԿՄ-ԻՆ
# Decimal separator may be '.', ',' or Armenian full stop '․' (U+0589).
KM_RE = re.compile(
    r"(\d+(?:[.,\u0589]\d+)?)"   # number with optional decimal
    r"(?:-\d+)?"                  # optional range end (e.g. 11-13), ignored
    r"\s*-?\s*"                   # optional dash
    r"(?:ՐԴ|րդ|ԻՆ|ին)?\s*"        # optional ordinal/locative suffix
    r"(?:ԿՄ|կм|կ/մ)",             # km marker (uppercase or lowercase Armenian)
    re.IGNORECASE | re.UNICODE,
)

# Մ-16/8 → km 8  (highway code / km number)
SLASH_KM_RE = re.compile(r"[Մ]-\d+/\s*(\d+(?:[.,\u0589]\d+)?)", re.UNICODE)


def parse_km(place: str) -> float | None:
    m = KM_RE.search(place) or SLASH_KM_RE.search(place)
    if not m:
        return None
    raw = m.group(1).replace(",", ".").replace("\u0589", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def fix_existing_precision(db, dry_run: bool):
    """Set geo_precision = 'highway_km' for already-backfilled rows still marked 'highway'."""
    rows = (
        db.query(Accident)
        .filter(Accident.is_highway == True)
        .filter(Accident.highway_km != None)
        .filter(Accident.geo_precision == "highway")
        .all()
    )
    if not rows:
        print("No previously backfilled rows need geo_precision fix.")
        return
    print(f"Fixing geo_precision for {len(rows):,} already-backfilled rows...")
    if not dry_run:
        for acc in rows:
            acc.geo_precision = "highway_km"
        db.commit()
    print(f"  {'[DRY] Would fix' if dry_run else 'Fixed'} {len(rows):,} rows.")


def run(dry_run: bool, limit: int | None):
    db = SessionLocal()
    try:
        fix_existing_precision(db, dry_run)

        query = (
            db.query(Accident)
            .filter(Accident.is_highway == True)
            .filter(Accident.highway_km == None)
            .filter(Accident.place != None)
            .filter(Accident.place != "")
            .order_by(Accident.id)
        )
        if limit:
            query = query.limit(limit)

        rows = query.all()
        total = len(rows)
        print(f"Found {total:,} rows to process.")

        updated = skipped = 0

        for i, accident in enumerate(rows, 1):
            km = parse_km(accident.place)
            if km is not None:
                if dry_run:
                    print(f"  [DRY] id={accident.id} place={accident.place!r} -> highway_km={km}")
                else:
                    accident.highway_km = km
                    accident.geo_precision = "highway_km"
                updated += 1
            else:
                skipped += 1
                if dry_run:
                    print(f"  [SKIP] id={accident.id} place={accident.place!r} (no km found)")

            if not dry_run and i % 200 == 0:
                db.commit()
                print(f"  {i}/{total} committed")

        if not dry_run:
            db.commit()

        print(f"\nDone. total={total} updated={updated} skipped(no km)={skipped}")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill highway_km from place column.")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    args = parser.parse_args()

    run(dry_run=args.dry_run, limit=args.limit)
