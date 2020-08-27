from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Define Models
class LatestSocial(BaseModel):
    type: str
    last_update: date

class CovidCounty(BaseModel):
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

class UnemploymentDataCounty(BaseModel):
    geo_id: str
    month_last_date: date
    state_nm: str
    county_nm: str
    labor_force: int
    employed: int
    unemployed: int
    unemployed_rate: Optional[float]

class UnemploymentDataZip(BaseModel):
    zip_cd: str
    month_last_date: date
    labor_force: float
    employed: float
    unemployed: float
    unemployed_rate: Optional[float]

class UnemploymentClaimsCounty(BaseModel):
    period_end_date: date
    geo_id: str
    state_nm: str
    county_nm: str
    claims_cnt: int

class UnemploymentClaimsZip(BaseModel):
    zip_cd: str
    period_end_date: date
    claims_cnt: int