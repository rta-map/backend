from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from schemas import AccidentProperties, Feature, FeatureCollection, Point


def _filters(date_from, date_to, accident_type, only_dead) -> tuple[str, dict]:
    clauses = ["geo_precision NOT IN ('city', 'highway')", "place IS NOT NULL", "place != ''", "geo_informal IS NOT 1"]
    params = {}
    if date_from is not None:
        clauses.append("date >= :date_from")
        params["date_from"] = date_from
    if date_to is not None:
        clauses.append("date <= :date_to")
        params["date_to"] = date_to
    if accident_type is not None:
        clauses.append("accident_type = :accident_type")
        params["accident_type"] = accident_type
    if only_dead:
        clauses.append("dead > 0")
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def query_all(
    db: Session,
    date_from: Optional[str],
    date_to: Optional[str],
    accident_type: Optional[str],
    only_dead: bool = False,
) -> FeatureCollection:
    extra_sql, params = _filters(date_from, date_to, accident_type, only_dead)

    sql = text(
        f"""
        SELECT
            lat, lon, date, accident_type,
            COALESCE(dead, 0) AS dead,
            COALESCE(injured, 0) AS injured,
            city, district, street, place, is_highway
        FROM accidents
        WHERE 1=1
          {extra_sql}
        """
    )

    rows = db.execute(sql, params).fetchall()
    features: List[Feature] = []
    for row in rows:
        features.append(
            Feature(
                geometry=Point(coordinates=[row.lon, row.lat]),
                properties=AccidentProperties(
                    date=row.date,
                    accident_type=row.accident_type,
                    dead=row.dead,
                    injured=row.injured,
                    city=row.city,
                    district=row.district,
                    street=row.street,
                    place=row.place,
                    is_highway=bool(row.is_highway) if row.is_highway is not None else None,
                ),
            )
        )
    return FeatureCollection(features=features)
