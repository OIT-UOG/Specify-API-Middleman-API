from fastapi import FastAPI, Depends, Query, HTTPException, __version__ as fversion
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import aiohttp
from threading import Thread
import os
import json
import uvicorn
from typing import List, Dict

from .specify import CombinedApi as Api
from .specify import ColumnModel, CombinedSettingsModel
from pydantic import Field, BaseModel

API_URL = '/'.join(s.strip('/') for s in [os.getenv('API_URL'), 'specify-solr'])

app_url = os.getenv('APP_URL')

if not app_url.startswith('http'):
    app_url = 'https://' + app_url

origins = [
    app_url
]

tags = [
    {
        "name": "setup",
        "description": "These endpoints include information you'll want to retrieve when you first load your client app." \
            "This includes the fields available and the image base urls to use per collection"
    },
    {
        "name": "search",
        "description": "endpoints for searching specimen records"
    },
    {
        "name": "misc",
        "description": "miscellaneous endpoints"
    }
]

api = Api(API_URL)
dump_api = Api(API_URL)


def start_loop(loop, api, dump_api):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(api.start(), dump_api.start(None)))

def prestart():
    loop = asyncio.new_event_loop()
    t = Thread(target=start_loop, args=(loop, api, dump_api))
    t.start()
    t.join()

prestart()

DocItemModel = api.DocItemModel


class SearchResponseModel(BaseModel):
    docs: List[DocItemModel] = []
    facet_counts: Dict[str, int]
    total: int
    last_page: int


app = FastAPI(
    title="Specify Middleman API",
    description="provides a simplified, unified view into the Specify SOLR collections",
    version="0.0.1",
    openapi_tags=tags,
)


print('='*20 + f'\n= Allowing origins: {origins}\n' + ('='*20))

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_headers=['*']
)

async def shared_api():
    if not api.ready:
        await api.start()
        return api
    return api


async def shared_dump_api():
    if not dump_api.ready:
        await dump_api.start(None)
        return dump_api
    return dump_api

@app.get("/settings", tags=['setup'], response_model=CombinedSettingsModel, response_model_exclude_unset=True)
async def settings(api: Api = Depends(shared_api)):
    """returns query syntax to use in the /search endpoint  
    also returns settings used to configure the individual collections, which includes their respective image store urls"""
    return await api.settings()

@app.get("/model", tags=['setup'], response_model=List[ColumnModel], response_model_exclude_unset=True)
async def model(api: Api = Depends(shared_api)):
    """returns the header meta-information for each attribute returned from item from the /search endpoint"""
    return await api.model()

@app.get("/search", tags=["search"], response_model=SearchResponseModel)
async def query(api: Api = Depends(shared_api), 
                q: str=Query("[\"*\"]",
                    description="lisp-y \"json\" string following the rules described in this endpoint's description",
                    example='[2,"david",[1,"anae","rus*"],["2_latitude1",12,14],["10_startDate",2018]]'),
                colls: str = Query('', 
                    regex=r"((^|,)(\w*?))*$", 
                    example="fish,coral",
                    description="Comma-separated list of collections to include in search. All collections if not used"),
                # geo: bool = Query(False, description="whether or not to also return list of geocoordinates for full search result space"),
                sort: str = Query(None, description="column name (or solrname) to sort by", example="1_catalogNumber"),
                asc: bool = Query(False, description="if sort is given, this defines the order to sort by. Default descending"),
                page: int = 0):
    """
    Query specimen data from all or some of the collections as if from a single collection.
    
    ---

    **queryTerm**:

    This parameter must be a string that is a lisp-y "json" list of arbitrary depth (with limitations).
    
    The valid terms are as follows:

    ```
    1 |  '["single search string with or without asterisk wildcards*"]'`
    2 |  '[OR,  "term1", "term2", ...]' *
    3 |  '[AND, "term1", "term2", ...]' *
    4 |  '["colname or solrname", "search term specifically in this field"]'  # ["cn",["sd", 2020]] would obviously not do anything
    5 |  '["colname or solrname", from_value, to_value]'  # the last 2 terms must be of term type 1 (as in no additional depth)
    ```

    \* The **OR** and **AND** values should be the ones provided by the `/settings` endpoint


    *Example*:
    ```
    '[
        AND, 
        "david", 
        [
            OR,
            "sch*",
            "fer*"
        ],
        [
            "2_latitude1", 
            -180, 
            5
        ],
        [
            "10_startDate", 
            2020
        ]
    ]'
    ```
    """
    qt = json.loads(q)
    if not colls:
        c = [*api._collections]
    else:
        c = [api.short_names.get(k, k) for k in colls.split(',')]
        for k in c:
            if k not in api._collections:
                raise HTTPException(status_code=422, detail=f'{k} is not a collection')
        
    return await api.query(qt, c, sort, asc, page)


@app.get("/searchdump", tags=["search"], response_model=SearchResponseModel)
async def querydump(api: Api = Depends(shared_dump_api),
                q: str = Query("[\"*\"]"),
                colls: str = Query('', regex=r"((^|,)(\w*?))*$"),
                sort: str = Query(None),
                asc: bool = Query(False),
                ):
    """
    same as search endpoint but returns all results for faster csv building.

    TODO: possibly just use solr's csv output, but how would we clean it up/combine it?.. don't?
    """
    qt = json.loads(q)
    if not colls:
        c = [*api._collections]
    else:
        c = [api.short_names.get(k, k) for k in colls.split(',')]
        for k in c:
            if k not in api._collections:
                raise HTTPException(
                    status_code=422, detail=f'{k} is not a collection')

    return await api.query(qt, c, sort, asc, 0)


@app.get("/fastapi_version", tags=['misc'], include_in_schema=False)
async def version():
    return fversion
