# API for Regional Social Database
import sqlalchemy
import databases
import json
import re

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
    query = "SELECT run_cd AS type, lst_success_dt AS last_update FROM cre_last_success_run_dt;"
    return await database.fetch_all(query=query)

@app.get('/social/covid', response_model=List[CovidSocial])
async def get_covid(date: Optional[str] = None):
    
    if date == None:
        date = 'Null'
    else:
        if bool(re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date)) == False: 
            raise HTTPException(status_code=400, detail="Please enter your date in 'YYYY-MM-DD' Format")

        date_query = '''SELECT DISTINCT report_date 
                        FROM cre_vu_covid_county'''
        date_objs = await database.fetch_all(query=date_query) 

        valid_dates = [] 
        for value in date_objs:
            valid_dates.append(str(value['report_date']))

        if date not in valid_dates:
            raise HTTPException(status_code=400, detail="No records for that date exist")

        date = "'"+date+"'"

    query = f'''SELECT * 
                FROM cre_vu_covid_county 
                WHERE report_date = COALESCE({date},(SELECT MAX(report_date) FROM cre_vu_covid_county))'''
    return await database.fetch_all(query=query)

# @app.get('/social/census/{CensusInput}', response_model=List[CensusSocial])
# async def get_census(CensusInput: str):

#     query = f'''SELECT census.geo_id, CONCAT(county_nm, ', ', state_nm) as County, {CensusInput}
#                 FROM cre_census_data_by_county_yr2018 AS census
#                 JOIN lkup_areas_of_intr_geo_scope as lkup
#                 ON census.geo_id = lkup.geo_id;'''
#     return await database.fetch_all(query=query)

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
