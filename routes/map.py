from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_db
from schemas import FeatureCollection
from services.clustering import query_all

router = APIRouter()


@router.get("/accidents", response_model=FeatureCollection)
def get_accidents(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    accident_type: Optional[str] = None,
    only_dead: bool = False,
    db: Session = Depends(get_db),
):
    return query_all(db, date_from, date_to, accident_type, only_dead)
