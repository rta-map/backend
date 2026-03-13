from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from schemas import AccidentProperties, ClusterProperties, Feature, FeatureCollection, Point


def get_cell_size(zoom: int) -> Optional[float]:
    if zoom >= 14:
        return None
    elif zoom >= 12:
        return 0.01
    elif zoom >= 10:
        return 0.05
    elif zoom >= 8:
        return 0.2
    elif zoom >= 6:
        return 1.0
    else:
        return 5.0


def _filters(date_from, date_to, accident_type) -> tuple[str, dict]:
    clauses = []
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
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def query_clustered(
    db: Session,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    cell_size: float,
    date_from: Optional[str],
    date_to: Optional[str],
    accident_type: Optional[str],
) -> FeatureCollection:
    extra_sql, params = _filters(date_from, date_to, accident_type)
    params.update(
        {
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
            "cell": cell_size,
        }
    )

    sql = text(
        f"""
        SELECT
            ROUND(lat / :cell) * :cell AS cell_lat,
            ROUND(lon / :cell) * :cell AS cell_lon,
            AVG(lat) AS avg_lat,
            AVG(lon) AS avg_lon,
            COUNT(*) AS cnt,
            SUM(COALESCE(dead, 0)) AS total_dead,
            SUM(COALESCE(injured, 0)) AS total_injured
        FROM accidents
        WHERE lat BETWEEN :min_lat AND :max_lat
          AND lon BETWEEN :min_lon AND :max_lon
          {extra_sql}
        GROUP BY cell_lat, cell_lon
        """
    )

    rows = db.execute(sql, params).fetchall()
    features: List[Feature] = []
    for row in rows:
        features.append(
            Feature(
                geometry=Point(coordinates=[row.avg_lon, row.avg_lat]),
                properties=ClusterProperties(
                    count=row.cnt,
                    dead=row.total_dead,
                    injured=row.total_injured,
                ),
            )
        )
    return FeatureCollection(features=features)


def query_individual(
    db: Session,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    date_from: Optional[str],
    date_to: Optional[str],
    accident_type: Optional[str],
) -> FeatureCollection:
    extra_sql, params = _filters(date_from, date_to, accident_type)
    params.update(
        {
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        }
    )

    sql = text(
        f"""
        SELECT
            lat, lon, date, accident_type,
            COALESCE(dead, 0) AS dead,
            COALESCE(injured, 0) AS injured,
            city, district, street, is_highway
        FROM accidents
        WHERE lat BETWEEN :min_lat AND :max_lat
          AND lon BETWEEN :min_lon AND :max_lon
          {extra_sql}
        LIMIT 2000
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
                    is_highway=bool(row.is_highway) if row.is_highway is not None else None,
                ),
            )
        )
    return FeatureCollection(features=features)
