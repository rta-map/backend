"""One-time script to import dtp_clean.csv into rta.db."""

import sys
from pathlib import Path

import pandas as pd

# Allow imports from backend root
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import SessionLocal, engine
from models import Accident, Base

CSV_PATH = Path(__file__).parent.parent / "dtp_clean.csv"

COLUMN_MAP = {
    "date": "date",
    "year": "year",
    "month": "month",
    "hour": "hour",
    "accident_type": "accident_type",
    "dead": "dead",
    "injured": "injured",
    "city": "city",
    "district": "district",
    "street": "street",
    "place": "place",
    "is_highway": "is_highway",
    "highway_code": "highway_code",
    "highway_km": "highway_km",
    "geo_precision": "geo_precision",
    "lat": "lat",
    "lon": "lon",
}


def run():
    print("Creating tables...")
    Base.metadata.create_all(bind=engine)

    print(f"Reading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)

    # Keep only relevant columns that exist
    cols = [c for c in COLUMN_MAP if c in df.columns]
    df = df[cols].copy()

    # Drop rows without coordinates
    df = df.dropna(subset=["lat", "lon"])

    # Normalise boolean
    if "is_highway" in df.columns:
        df["is_highway"] = df["is_highway"].map(
            lambda v: True if str(v).strip().lower() in ("true", "1", "yes") else False
        )

    # Cast numeric columns
    for col in ("dead", "injured", "year", "month"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    print(f"Importing {len(df):,} rows...")

    BATCH = 500
    db = SessionLocal()
    try:
        for i in range(0, len(df), BATCH):
            batch = df.iloc[i : i + BATCH]
            db.bulk_insert_mappings(Accident, batch.rename(columns=COLUMN_MAP).to_dict(orient="records"))
            db.commit()
            if (i // BATCH) % 20 == 0:
                print(f"  {i + len(batch):,} / {len(df):,}")
    finally:
        db.close()

    print("Done.")


if __name__ == "__main__":
    run()
