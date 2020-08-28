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
    """This route will return the most recent dates that data has been added to the database.\n
    Example: https://api.stldata.org/social/latest"""
    
    query = "SELECT run_cd AS type, lst_success_dt AS last_update FROM cre_last_success_run_dt;"
    return await database.fetch_all(query=query)

## Covid data for all counties by date ##
@app.get('/social/covid', response_model=List[CovidCounty])
async def get_covid_data(date: Optional[date] = None):
    """This route will return all the covid data gathered for all counties for a given date.
    If no date is provided it will return the data from the most recent date in the database.\n
    Example: https://api.stldata.org/social/covid?date=2020-01-25"""
    
    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_covid_county 
                    WHERE report_date = (SELECT MAX(report_date) FROM cre_vu_covid_county);'''
        return await database.fetch_all(query=query)
    else:
        await validate_dates(date)
        
        values = {'date': date}
        query = '''SELECT * 
                    FROM cre_vu_covid_county 
                    WHERE report_date = :date;'''
        return await database.fetch_all(query=query, values=values)

## Covid data for a single county by date range ##
@app.get('/social/covid/{county}', response_model=List[CovidCounty])
async def get_covid_data_time_series(county: str, startdate: Optional[date] = None, enddate: Optional[date] = None):
    """This route will return all the covid information for a specific county (referenced by FIPS code) that was gathered between a startdate and enddate (dates are inclusive and both are required though they can be the same).
    If dates are not provided it will return all data present for the specified county that was gathered in the month previous to the most recent data in the database (DLY_ALL from the /latest endpoint).\n
    FIPS codes for all counties and states can be found here: https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt\n
    Example: https://api.stldata.org/social/covid/29189?startdate=2020-07-01&enddate=2020-07-04"""

    if startdate == None or enddate == None :
        
        default_values = {'county': county}
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
            raise HTTPException(status_code=400, detail=f"Your startdate needs to be before or the same as enddate.")
        
        date_range_values = {'county': county, 'startdate': startdate, 'enddate': enddate}
        query = '''SELECT * FROM cre_vu_covid_county
                    WHERE report_date BETWEEN :startdate AND :enddate
                    AND geo_id = :county
                    ORDER BY report_date;'''
        
        return await database.fetch_all(query=query, values=date_range_values)

## Unemployment Data from BLS by county ##
@app.get('/social/unemployment/data/county', response_model=List[UnemploymentDataCounty])
async def get_unemployment_data_county(date: Optional[date] = None):
    '''This route will return all unemployment data gathered from the BLS for all MO & IL counties for a given month.
    When a date is provided it will return the data for the month that date falls in.  If no date it given it will return the most recent data.\n
    Example: https://api.stldata.org/social/unemployment/data/county?date=2020-03-28'''
    
    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_data 
                    WHERE month_last_date = (SELECT MAX(month_last_date) FROM cre_vu_bls_unemployment_data);'''

        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'unemploymentCounty')
        values = {'dateYr': date.year, 'dateMon': date.month}
        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_data
                    WHERE date_part('year', month_last_date) = :dateYr
                    AND date_part('month', month_last_date) =  :dateMon;'''

        return await database.fetch_all(query=query, values=values)

## Unemployment Data from BLS by zip ##
@app.get('/social/unemployment/data/zip', response_model=List[UnemploymentDataZip])
async def get_unemployment_data_zip(date: Optional[date] = None):
    '''This route will return all unemployment data gathered from the BLS for all MO & IL zip codes for a given month.
    When a date is provided it will return the data for the month that date falls in.  If no date it given it will return the most recent data.\n
    Example: https://api.stldata.org/social/unemployment/data/zip'''
    
    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_map_2_zip
                    WHERE month_last_date = (SELECT MAX(month_last_date) FROM cre_vu_bls_unemployment_map_2_zip);'''

        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'unemploymentZip')
        values = {'dateYr': date.year, 'dateMon': date.month}
        query = '''SELECT * 
                    FROM cre_vu_bls_unemployment_map_2_zip
                    WHERE date_part('year', month_last_date) = :dateYr
                    AND date_part('month', month_last_date) =  :dateMon;'''

        return await database.fetch_all(query=query, values=values)

## Unemployment claims by county ##
@app.get('/social/unemployment/claims/county', response_model=List[UnemploymentClaimsCounty])
async def get_weekly_claims_county(date: Optional[date] = None):
    '''This route will return all the unemployment claims from all counties in MO & IL for a given week.
    When a date is provided it will return the data that was gathered during the 7 day period starting on that date.  If no date it given it will return the most recent data.\n
    Example: https://api.stldata.org/social/unemployment/claims/county?date=2020-03-28'''

    values = {'date': date}

    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms
                    WHERE period_end_date = (SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms);'''
        
        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'claimsCounty')

        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms
                    WHERE period_end_date BETWEEN :date AND (:date + INTERVAL '6 day');'''
        
        return await database.fetch_all(query=query, values=values)

## Unemployment claims by zip ##
@app.get('/social/unemployment/claims/zip', response_model=List[UnemploymentClaimsZip])
async def get_weekly_claims_zip(date: Optional[date] = None):
    '''This route will return all the unemployment claims from all zip codes in MO & IL for a given week.
    When a date is provided it will return the data that was gathered during the 7 day period starting on that date.  If no date it given it will return the most recent data.\n
    Example: https://api.stldata.org/social/unemployment/claims/zip'''

    if date == None:
        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms_map_2_zip
                    WHERE period_end_date = (SELECT MAX(period_end_date) FROM cre_vu_unemployment_clms_map_2_zip);'''
        
        return await database.fetch_all(query=query)
    
    else:
        await validate_dates(date, qryType = 'claimsZip')
        values = {'date': date}
        query = '''SELECT * 
                    FROM cre_vu_unemployment_clms_map_2_zip
                    WHERE period_end_date BETWEEN :date AND (:date + INTERVAL '6 day');'''
        
        return await database.fetch_all(query=query, values=values)

## Unemployment claims by zip ##
@app.get('/social/census', response_model=List[CensusCategories])
async def get_census_categories(category:str):
    '''When provided with a category variable this endpoint will return the value for that category from the most recent census data for all geo_ids within MO & IL.  If a category name is imporperly provided a list of all available variables will be returned\n
    Example: https://api.stldata.org/social/census?category=age_65pl\n
    Example: https://api.stldata.org/social/census?category=total_households\n'''
    
    variables = {
        'age_65pl': 'est_pop_age_65pl',
        'disability': 'est_pop_wth_dsablty',
        'age_25pl': 'est_pop_age_25pl',
        'age_25pl_hgh_schl_orls': 'est_pop_age_25pl_hgh_schl_orls',
        'age_16pl': 'est_pop_age_16pl',
        'age_16pl_laborforce': 'est_pop_age_16pl_in_lbr_frce_prop',
        'age_16pl_employed': 'est_pop_age_16pl_empld_prop',
        'age_16pl_unemployed': 'est_pop_age_16pl_unempl_rt',
        'no_insurance': 'est_pop_wthout_hlth_insr',
        'not_hispanic_latino': 'est_pop_not_hisp_latino',
        'hispanic_latino': 'est_pop_hisp_latino',
        'total_households': 'est_tot_hh',
        'households_own': 'est_tot_hh_own_res',
        'households_rent': 'est_tot_hh_rent_res',
        'est_gini_ndx': 'est_gini_ndx',
        'no_internet': 'est_pop_no_internet_access',
        'commute_to_work': 'est_pop_commute_2_wrk',
        'public_trans_to_work': 'est_pop_publ_trans_2_wrk',
        'median_household_income_2018adj': 'est_mdn_hh_ncome_ttm_2018nfl_adj',
        'median_per_capita_income_2018adj': 'est_mdn_percap_ncome_ttm_2018nfl_adj',
        'est_pop_wth_knwn_pvrty_stts': 'est_pop_wth_knwn_pvrty_stts',
        'est_pop_undr_pvrty_wth_knwn_pvrty_stts': 'est_pop_undr_pvrty_wth_knwn_pvrty_stts',
        'white': 'est_pop_white',
        'black': 'est_pop_blk',
        'native_american': 'est_pop_am_ind',
        'asian': 'est_pop_asian',
        'hawaiian': 'est_pop_hwaiian',
        'other': 'est_pop_othr',
        '2pl_race': 'est_pop_2pl_race',
        'total_pop': 'est_tot_pop',
        'imu_score': 'imu_score',
        'rpl_themes_svi_ndx': 'rpl_themes_svi_ndx',
        'area': 'area_sq_mi'
        }

    varlist = list(variables.keys())
    if category not in varlist:
        raise HTTPException(status_code=400, detail=f'Sorry {category} is not an acceptable variable.  Please choose from the following list {varlist}')

    query = f'''SELECT geo_id, {variables[category]} as category FROM cre_vu_census_data_by_county_curr;'''
    
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
