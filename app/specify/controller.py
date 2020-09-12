import asyncio
import aiohttp
import re
from fastapi import HTTPException
from cachetools import TTLCache
from .api import SpecifyApi, Api, Column, SearchSyntax, QueryCache, FieldModel, ApiValidationError, deephash
from pydantic import BaseModel, HttpUrl, Field, create_model
from typing import Optional, List, Dict
from pathlib import Path
import random
from functools import reduce
from operator import iconcat
import logging

COLLECTION_PATTERN = re.compile(r'<a href="(.*?)"')


class Settings(BaseModel):
    shortName: str
    solrURL: Optional[str] = ""
    imageBaseUrl: HttpUrl
    bottomHeight: Optional[int] = None
    bannerURL: Optional[Path] = None
    defMapType: Optional[str] = None
    imageInfoFlds: str
    bannerHeight: Optional[int] = None
    topBranding: Optional[str] = None
    collectionName: str
    solrCore: Optional[str] = None
    solrPort: Optional[str] = None
    solrPageSize: Optional[int] = 50
    bottomWidth: Optional[int] = None
    bannerWidth: Optional[int] = None
    defInitialView: Optional[str] = None
    topWidth: Optional[int] = None
    imagePreviewSize: Optional[int] = 200
    topMarginLeft: Optional[int] = None
    portalInstance: Optional[str] = None
    maxSolrPageSize: Optional[int] = 5000
    bottomBranding: Optional[str] = None
    bannerTitle: Optional[str] = None
    topHeight: Optional[int] = None
    bottomMarginRight: Optional[int] = None
    imageViewSize: Optional[int] = 600
    bottomMarginLeft: Optional[int] = None
    topMarginRight: Optional[int] = None
    backgroundURL: Optional[Path] = None


class CombinedSettingsModel(BaseModel):
    search_syntax: SearchSyntax
    collections: Dict[str, Settings]


class ImageResponseModel(BaseModel):
    id: int
    name: str
    title: str
    coll: str


class CombinedApi():
    COLLECTION_SOLRNAME = FieldModel.COLLECTION_SOLRNAME
    DEFAULT_QUERY_ROWS = 50

    def __init__(self, base_url):
        self.base_url = base_url
        self.ready = False
        self._model = None
        self._new_cache()
    
    def _new_cache(self):
        # not too efficient cache
        self.cache = QueryCache()

    async def start(self):
        collections = await self._list_collections()
        _apis = {c: SpecifyApi(self.base_url, c, query_rows=self.DEFAULT_QUERY_ROWS) for c in collections}
        apis = list(_apis.values())
        for api in apis:
            await api.start()
        self.apis = apis 
        self._api_map = _apis

        await self.model(poke=False)

        # need to assign these atomically... does gunicorn disregard all this?
        self._collections = collections
        self.short_names = {c.replace('vouchers', ''): c for c in collections}
        
        item_model = {
            c.get('solrname'): (c.SOLRTYPE_TRANSFORMS[c.get('solrtype')], None)
            for c in self._model.columns
        }
        item_model['img'] = (List[ImageResponseModel], [])
        self.DocItemModel = create_model('DocItemModel',
            **item_model
        )
        
        self.ready = True
    
    async def list_collections(self):
        """user-facing collection names
        resets api if collections have changed
        """
        cols = await self._list_collections()
        if set(self._collections) != set(cols):
            loop = asyncio.get_event_loop()
            loop.create_task(self.start())
        return sorted(c.replace('vouchers', '') for c in cols)
    
    async def settings(self):
        # update collections/apis if needed
        await self.list_collections()
        settings = await asyncio.gather(*(api.settings() for api in self.apis))
        return {
            'search_syntax': {"OR": SpecifyApi.SYNTAX.OR, "AND": SpecifyApi.SYNTAX.AND}, 
            'collections': {st['shortName']: st for st in settings}
        }
    
    async def model(self, poke=True):
        stale_poke = False
        if poke:
            stale_poke = any(await asyncio.gather(*(api.check_if_stale() for api in self.apis)))
        if stale_poke or any(api.stale for api in self.apis):
            self._sync_models()
            self._new_cache()
        
        return self._model.serialized()

    def _sync_models(self):
        combined_model = self.apis[0].column_model
        for api in self.apis[1:]:
            other = api.column_model
            combined_model = combined_model.merged_model(other)
        for api in self.apis:
            api.set_follow_model(combined_model)
        self._model = combined_model

    async def _list_collections(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url) as resp:
                text = await resp.text()
        return re.findall(COLLECTION_PATTERN, text)

    def query_cache_keys(self, queryTerms, collections, sort, asc):
        return str([
            sorted(collections),
            0 if asc else 1,
            sort if sort else "",
            deephash(queryTerms)
        ])

    def _rand_drip(self, results, cursors, asc=False):
        buffers = {c: results[c]['docs'] for c in results if results[c]['docs']}
        order = sorted(buffers)
        weights = [results[c]['total'] for c in order]
        random.seed(buffers[order[0]][0]['spid'])
        while True:
            try:
                items = [(buffers[c][cursors[c][1]], c) for c in order]
            except IndexError:
                return
            # we can probably not change the weight each time
            chosen = random.choices(items, weights=weights)[0]
            cursors[chosen[1]][1] += 1
            yield chosen[0]
    
    def _collection_drip(self, results, cursors, asc=False):
        # technically with 'coll' now in return item, we could use field sort, but this might be more efficient
        buffers = {c: results[c]['docs'] for c in results if results[c]['docs']}
        order = sorted(buffers, reverse=not asc)
        for c in order:
            while cursors[c][1] < len(buffers[c]):
                yield buffers[c][cursors[c][1]]
                cursors[c][1] += 1
            return

    def _field_drip_maker(self, field):
        def _field_drip(results, cursors, asc=False):
            buffers = {c: results[c]['docs'] for c in results if results[c]['docs']}
            compare = min if asc else max
            while True:
                to_compare = []
                for c in buffers:
                    try:
                        to_compare.append((c, buffers[c][cursors[c][1]]))
                    except IndexError:
                        return
                chosen = compare(to_compare, key=lambda i: i[1][field])
                cursors[chosen[0]][1] += 1
                yield chosen[1]
        return _field_drip

    def _drip_generator(self, solrname=None):
        """drip orderers. None is random normalized distribution"""
        # problem with the way we've done this is the apis are giving us the items sorted by their types
        # but we're pulling them by the combined type
        if solrname is None:
            return self._rand_drip
        elif solrname == self.COLLECTION_SOLRNAME:
            return self._collection_drip
        else:
            return self._field_drip_maker(solrname)
    
    def rinse_cache_items(self, items, deep=False):
        """refreshes items in cache
        if deep is True
        replaces items in items array with cache instances and adds any new items to the cache
        This is a memory saving attempt to erase duplicates in cache
        """
        for i in range(len(items)):
            item = items[i]
            spid = item['spid']
            api = self._api_map[self.short_names[item['coll']]]
            citem = api.cache.get(spid)
            if deep:
                if citem:
                    items[i] = citem
                else:
                    api.cache.set(spid, item)

        return items
    
    def _api_pager_maker(self, api, queryTerms, ignore_missing, sort, asc, cache):
        async def pager(page):
            if pager.last_page is not None and page > pager.last_page:
                raise IndexError
            resp = await api.query(queryTerms, ignore_missing, sort, asc, page, cache)
            pager.last_page = resp['last_page']
            return resp
        pager.last_page = None
        return pager

    def _combine_facet_counts(self, a, b):
        c = {**a}
        for k, v in b.items():
            c[k] = c.get(k, 0) + v
        return c

    async def query(self, queryTerms, collections, sort=None, asc=False, page=0, cache=True):
        """queryTerms given as
            1. ["single search string",]
            2. [OR, 'terms', 'to', 'or']
            3. [AND, 'terms', 'to', 'and']
            4. ['field', 'search']
            5. ['field', 'from', 'to'] <- from and to must be strings. no additional search terms or parenthesis
            6. any combination of those ex. [SearchSyntax.OR, "single search", ["field", "search], [SearchSyntax.AND, ["field", "from", "to"], "other field", "search"]]

        when a field is used as sort, the results will be returned sorted by that field ascending or descending depending on asc
        page is the page number of results returned
        cache is whether or not to set and pull from cached results
        """
        if page < 0:
            raise ApiValidationError('page must be positive')
        sort_solrname = None
        if sort:
            try:
                sort_solrname = self._model._resolve_solrname_from_colname_or_solrname(sort)
            except KeyError:
                if sort == self.COLLECTION_SOLRNAME:
                    sort_solrname = sort
                else:
                    raise ApiValidationError(f'column {sort} does not exist')

        # if not cache:
        # else:

        key = self.query_cache_keys(queryTerms, collections, sort_solrname, asc)

        cache_dict = self.cache.get(key)

        if cache_dict:
            docs = cache_dict['pages'].get(page)
            
            if docs:
                # efficient flatten.. more efficient than leaving out these libraries and just recalling the function x times?
                needed_docs = reduce(iconcat, (cache_dict['pages'][i] for i in range(page + 1)), [])
                self.rinse_cache_items(needed_docs, deep=True)

                return {
                    'docs': docs,
                    'facet_counts': cache_dict['facet_counts'],
                    'total': cache_dict['total'],
                    'last_page': cache_dict['last_page'],
                }
        

        pagers = {
            c: self._api_pager_maker(self._api_map[c], queryTerms, True, sort_solrname, asc, cache)
            for c in collections
        }

        if cache_dict:
            cursors = cache_dict['ending_cursors']
            current_page = len(cache_dict['pages'])

            needed_docs = reduce(iconcat, (cache_dict['pages'][i] for i in range(current_page)), [])
            self.rinse_cache_items(needed_docs, deep=True)
        else:
            cursors = { c: [0, 0] for c in collections }
            current_page = 0
        
        results = {
            c: await self._api_map[c].query(queryTerms, True, sort_solrname, asc, cursors[c][0], cache=True) 
            for c in collections
        }

        if cache_dict is None:  
            geos_list = [r['facet_counts'] for r in results.values()]
            
            geos = geos_list[0]
            for g in geos_list[1:]:
                geos = self._combine_facet_counts(geos, g)

            total = sum(r['total'] for r in results.values())

            cache_dict = {
                'pages': {},
                'ending_cursors': cursors,
                'facet_counts': geos,
                'total': total,
                'last_page': -(-total // self.DEFAULT_QUERY_ROWS) - 1,
                'last_trickle': []
            }

        if page > cache_dict['last_page']:
            raise ApiValidationError(f'last page is {cache_dict["last_page"]}, requested page was {page}')

        dripper = self._drip_generator(sort_solrname)

        docs = cache_dict['last_trickle']  # this can't be []. it needs to pick up from previous queue
        # could have been avoided if drippers only dripped till DEFAULT_QUERY_ROWS
        while True:
            for item in dripper(results, cursors, asc):
                docs.append(item)
            at_end = [
                c for c in results if cursors[c][1] >= len(results[c]['docs'])
            ]
            end_and_more_pages = [
                c for c in at_end if cursors[c][0] < results[c]['last_page']
            ]
            
            self.rinse_cache_items(docs, deep=True)

            # add to cache_dict
            pages = [
                docs[i:i + self.DEFAULT_QUERY_ROWS]
                for i in range(0, len(docs), self.DEFAULT_QUERY_ROWS)
            ]

            if pages and len(pages[-1]) < self.DEFAULT_QUERY_ROWS:
                docs = pages.pop()
            else:
                docs = []

            for p in pages:
                cache_dict['pages'][current_page] = p
                current_page += 1
            
            if len(at_end) == len(results):
                if not end_and_more_pages:
                    if docs:
                        # last tiny page
                        cache_dict['pages'][current_page] = docs
                        current_page += 1
                        cache_dict['last_trickle'] = []
                    break
                
            # break here. cursors will be in limbo until next time where they'll roll oveer due to an empty dripper the next time
            if current_page > page:
                cache_dict['last_trickle'] = docs
                break

            for c in end_and_more_pages:
                cursors[c][0] += 1
                cursors[c][1] = 0
                results[c] = await pagers[c](cursors[c][0])
            
            for c in at_end:
                if c not in end_and_more_pages:
                    del results[c]

        # if current_page != page: doing with cache_dict['last_page']
        #     raise IndexError
        
        self.cache.set(key, cache_dict)
        return {
            'docs': cache_dict['pages'][page],
            'facet_counts': cache_dict['facet_counts'],
            'total': cache_dict['total'],
            'last_page': cache_dict['last_page']
        }


        # https://stackoverflow.com/a/52128389
