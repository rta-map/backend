from typing import List, Literal, Optional

from pydantic import BaseModel


class Point(BaseModel):
    type: Literal["Point"] = "Point"
    coordinates: List[float]  # [lon, lat]


class AccidentProperties(BaseModel):
    date: Optional[str]
    accident_type: Optional[str]
    dead: int
    injured: int
    city: Optional[str]
    district: Optional[str]
    street: Optional[str]
    place: Optional[str]
    is_highway: Optional[bool]


class Feature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Point
    properties: AccidentProperties


class FeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[Feature]
