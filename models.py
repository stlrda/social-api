from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Define Models
class LatestSocial(BaseModel):
    type: str
    last_update: date

class CovidSocial(BaseModel):
    report_date: date
    state_nm: str
    county_nm: str
    geo_id: str
    cases: int
    case_rate: float
    new_cases: int
    case_avg: float
    death: int
    mortality_rate: float
    new_deaths: int
    death_avg: float
    case_fatality_rate: Optional[float]

class CensusSocial(BaseModel):
    geo_id: int
    county: str
    est_pop_age_65pl: Optional[int]
    est_pop_wth_dsablty: Optional[int]