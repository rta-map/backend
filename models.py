from sqlalchemy import Boolean, Column, Float, Index, Integer, Text

from database import Base


class Accident(Base):
    __tablename__ = "accidents"

    id = Column(Integer, primary_key=True)
    date = Column(Text)
    year = Column(Integer)
    month = Column(Integer)
    hour = Column(Float)
    accident_type = Column(Text)
    dead = Column(Integer)
    injured = Column(Integer)
    city = Column(Text)
    district = Column(Text)
    street = Column(Text)
    place = Column(Text)
    is_highway = Column(Boolean)
    highway_code = Column(Text)
    highway_km = Column(Float)
    geo_precision = Column(Text)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)

    __table_args__ = (
        Index("ix_accidents_lat_lon", "lat", "lon"),
        Index("ix_accidents_year", "year"),
        Index("ix_accidents_accident_type", "accident_type"),
    )
