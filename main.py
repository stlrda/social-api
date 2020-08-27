# API for Regional Social Database
import sqlalchemy
import databases
import json

from datetime import date
from pydantic import BaseModel
from fastapi import FastAPI, Request, Depends, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.responses import RedirectResponse

# DB connection information in local gitignored file
from config import DATABASE_URL

# import the base models
from models import *

# import validate_dates
from functions import validate_dates

## Load Database Configuration ##
database = databases.Database(DATABASE_URL)

engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

# Allow All CORS
app = FastAPI(title='Social API', docs_url="/social/docs", redoc_url="/social/redoc", openapi_url="/social/openapi.json")
app.add_middleware(CORSMiddleware, allow_origins=['*'])

## Connect to DB on Startup and disconnect on Shutdown ##
@app.on_event('startup')
async def startup():
    await database.connect()
@app.on_event('shutdown')
async def shutdown():
    await database.disconnect()

# Redirect Root to Docs 
@app.get('/social')
async def get_api_docs():
    response = RedirectResponse(url='/social/redoc')
    return response

## Social ENDPOINTS!! ##
@app.get('/social/latest', response_model=List[LatestSocial])
async def get_latest():
    """This route will return the most recent dates that data has been added to the database."""
    
    query = "SELECT run_cd AS type, lst_success_dt AS last_update FROM cre_last_success_run_dt;"
    return await database.fetch_all(query=query)

@app.get('/social/covid', response_model=List[CovidSocial])
async def get_covid_data(date: Optional[date] = None):
    """This route will return all the covid data gathered for all counties for a given date.
    If no date is provided it will return the data from the most recent date in the database."""
    
    values = {'date': date}

    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_covid_county 
                    WHERE report_date = (SELECT MAX(report_date) FROM cre_vu_covid_county);'''
        return await database.fetch_all(query=query)
    else:
        await validate_dates(date)
        query = '''SELECT * 
                    FROM cre_vu_covid_county 
                    WHERE report_date = :date;'''
        return await database.fetch_all(query=query, values=values)

@app.get('/social/covid/{county}', response_model=List[CovidSocial])
async def get_covid_data_time_series(county: str, startdate: Optional[date] = None, enddate: Optional[date] = None):
    """This route will return all the covid information for a specific county (referenced by geo_id) that was gathered between a start and end date.
    If dates are not provided it will return all data present for the specified county that was gathered in the month previous to the most recent data in the database."""

    default_values = {'county': county}
    date_range_values = {'county': county, 'startdate': startdate, 'enddate': enddate}
    
    if startdate == None or enddate == None :
        query = '''SELECT * FROM cre_vu_covid_county
                    WHERE date_part('year', report_date) = (SELECT date_part('year', (lst_success_dt - INTERVAL '1 month')) FROM cre_last_success_run_dt WHERE run_cd = 'DLY_ALL')
                    AND date_part('month', report_date) = (SELECT date_part('month', (lst_success_dt - INTERVAL '1 month')) FROM cre_last_success_run_dt WHERE run_cd = 'DLY_ALL')
                    AND geo_id = :county
                    ORDER BY report_date;'''
        
        return await database.fetch_all(query=query, values=default_values)

    else:
        await validate_dates(startdate)
        await validate_dates(enddate)
        
        if  (startdate) > (enddate):
            raise HTTPException(status_code=400, detail=f"Your start date needs to be before your end date.")

        query = '''SELECT * FROM cre_vu_covid_county
                    WHERE report_date BETWEEN :startdate AND :enddate
                    AND geo_id = :county
                    ORDER BY report_date;'''
        
        return await database.fetch_all(query=query, values=date_range_values)

@app.get('/social/unemployment/data', response_model=List[UnemploymentSocial])
async def get_monthly_unemployment_data(date: Optional[date] = None):

    values = {'dateYr': date.year, 'dateMon': date.month}

    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_data 
                    WHERE month_last_date = (SELECT MAX(month_last_date) FROM cre_vu_bls_unemployment_data);'''

        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'unemploymentMonthly')

        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_data
                    WHERE date_part('year', month_last_date) = :dateYr
                    AND date_part('month', month_last_date) =  :dateMon;'''

        return await database.fetch_all(query=query, values=values)

@app.get('/social/unemployment/claims', response_model=List[UnemploymentClaimsSocial])
async def get_weekly_unemployment_claims(date: Optional[date] = None):

    values = {'date': date}

    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms
                    WHERE period_end_date = (SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms);'''
        
        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'unemploymentClaims')

        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms
                    WHERE period_end_date BETWEEN :date AND (:date + INTERVAL '6 day');'''
        
        return await database.fetch_all(query=query, values=values)

## Modify API Docs ##
def api_docs():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title='Social Database',
        version='0.1.0',
        description='Automatically Updated, Demographic, Economic and Covid data from Saint Louis, provided by the St. Louis Regional Data Alliance .<br><br>If you\'d prefer to interact with queries in browser, see the <a href=\'/social/docs\'>Swagger UI</a>',
        routes=app.routes,#[2:],
    )
    openapi_schema['info']['x-logo'] = {
        'url' : 'https://api.stldata.org/rda-favicon.png' # Need a more permanent source
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = api_docs
