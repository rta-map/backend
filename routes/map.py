from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_db
from schemas import FeatureCollection
from services.clustering import get_cell_size, query_clustered, query_individual

router = APIRouter()


@router.get("/accidents", response_model=FeatureCollection)
def get_accidents(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: int,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    accident_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    cell_size = get_cell_size(zoom)
    if cell_size is None:
        return query_individual(db, min_lat, max_lat, min_lon, max_lon, date_from, date_to, accident_type)
    return query_clustered(db, min_lat, max_lat, min_lon, max_lon, cell_size, date_from, date_to, accident_type)
