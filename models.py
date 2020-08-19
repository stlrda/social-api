from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Define Models
class LatestSocial(BaseModel):
    type: str
    last_update: date