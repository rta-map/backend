from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel


class Point(BaseModel):
    type: Literal["Point"] = "Point"
    coordinates: List[float]  # [lon, lat]


class ClusterProperties(BaseModel):
    is_cluster: Literal[True] = True
    count: int
    dead: int
    injured: int


class AccidentProperties(BaseModel):
    is_cluster: Literal[False] = False
    count: Literal[1] = 1
    date: Optional[str]
    accident_type: Optional[str]
    dead: int
    injured: int
    city: Optional[str]
    district: Optional[str]
    street: Optional[str]
    is_highway: Optional[bool]


class Feature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Point
    properties: Union[ClusterProperties, AccidentProperties]


class FeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[Feature]
