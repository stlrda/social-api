import sqlalchemy
import databases
import json

from datetime import date, datetime as dt
from fastapi import FastAPI, Request, Depends, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.responses import RedirectResponse

# DB connection information in local gitignored file
from config import DATABASE_URL

## Load Database Configuration ##
database = databases.Database(DATABASE_URL)

engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

# This function validates the dates passed in as parameters.  Enforces formatting, valid dates, and valid date ranges depending on route
async def validate_dates(dateInput, qryType = 'covid'):

    # enforce date falls within date range for covid data
    if qryType == 'covid':
        
        valid_dates = []
        date_query = '''SELECT DISTINCT report_date FROM cre_vu_covid_county;'''
        
        await database.connect()
        date_objs = await database.fetch_all(query=date_query) 
        await database.disconnect()

        for obj in date_objs:
            valid_dates.append(obj['report_date'])
        
        if dateInput not in valid_dates:
            raise HTTPException(status_code=400, detail=f"No records for {dateInput} exist.  Records start on 2020-01-24 and new data is usually available within 2 days of the present date.")

    # enforce date falls within date range for unemployment data
    elif qryType == 'unemploymentMonthly':
        
        date_query = '''SELECT MAX(month_last_date) FROM cre_vu_bls_unemployment_data;'''
        
        await database.connect()
        latest_date = await database.fetch_one(query=date_query)
        await database.disconnect()

        if  dateInput <  dt.strptime('2019-04-01', '%Y-%m-%d').date() or dateInput > latest_date['max']:
            raise HTTPException(status_code=400, detail=f"No records for {dateInput} exist.  Records start on '2019-04-01' and new data is added at the end of each month.")

    elif qryType == 'unemploymentClaims':
        
        date_query = '''SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms;;'''
        
        await database.connect()
        latest_date = await database.fetch_one(query=date_query) 
        await database.disconnect()

        if dateInput < dt.strptime('2018-01-06', '%Y-%m-%d').date() or dateInput > latest_date['max']:
            raise HTTPException(status_code=400, detail=f"No records for {dateInput} exist.  Records start on '2018-01-06' and new data is added weekly.")

    else:
        raise HTTPException(status_code=400, detail=f"Unknown Query Type")