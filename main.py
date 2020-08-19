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
@app.get('/social/latest', response_model=LatestSocial)
async def get_latest():
    query = "SELECT run_cd AS type, lst_success_dt AS last_update FROM cre_last_success_run_dt;"
    return await database.fetch_all(query=query)

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