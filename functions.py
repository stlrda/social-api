import sqlalchemy
import databases
import json

from datetime import date
from fastapi import HTTPException

# DB connection information in local gitignored file
from config import DATABASE_URL

## Load Database Configuration ##
database = databases.Database(DATABASE_URL)

engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

async def get_date_range(qryMin, qryMax):
    await database.connect()
    dateMin = await database.fetch_one(query=qryMin) 
    dateMax = await database.fetch_one(query=qryMax)
    await database.disconnect()
    return (dateMin['min'], dateMax['max'])

# This function validates the dates passed in as parameters.  Enforces formatting, valid dates, and valid date ranges depending on route
async def validate_dates(dateInput, qryType = 'covid'):

    # enforce date falls within date range for covid data
    if qryType == 'covid':
        
        qryMin = '''SELECT MIN(report_date) FROM cre_vu_covid_county;'''
        qryMax = '''SELECT MAX(report_date) FROM cre_vu_covid_county;'''
        
        date_range = await get_date_range(qryMin, qryMax)

        if dateInput < date_range[0] or dateInput > date_range[1]:
            raise HTTPException(status_code=400, detail=f"Sorry, no data for {dateInput} found.  Records are added daily starting from {date_range[0]} to {date_range[1]}.")

    # enforce date falls within date range for unemployment data
    elif qryType == 'unemploymentMonthly':
        
        qryMin = '''SELECT MIN(month_last_date) FROM cre_vu_bls_unemployment_data;'''
        qryMax = '''SELECT MAX(month_last_date) FROM cre_vu_bls_unemployment_data;'''
        
        date_range = await get_date_range(qryMin, qryMax)

        if dateInput < date_range[0] or dateInput > date_range[1]:
            raise HTTPException(status_code=400, detail=f"Sorry, no data for {dateInput} found.  Records are added monthly starting from {date_range[0]} to {date_range[1]}.")

    elif qryType == 'claimsCounty' or qryType == 'claimsZip':
        
        if qryType == 'claimsCounty':
            qryMin = '''SELECT MIN(period_end_date) FROM cre_vu_unemployment_clms;'''
            qryMax = '''SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms;'''
        else:
            qryMin = '''SELECT MIN(period_end_date) FROM cre_vu_unemployment_clms_map_2_zip;'''
            qryMax = '''SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms_map_2_zip;'''

        date_range = await get_date_range(qryMin, qryMax)

        if dateInput < date_range[0] or dateInput > date_range[1]:
            raise HTTPException(status_code=400, detail=f"Sorry, no data for {dateInput} found.  Records are added weekly starting from {date_range[0]} to {date_range[1]}.")

    else:
        raise HTTPException(status_code=400, detail=f"Unknown Query Type")