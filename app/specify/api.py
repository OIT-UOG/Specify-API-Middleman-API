import aiohttp
import orjson
from fastapi import HTTPException
import orjson as json
import re
import urllib
from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum
from cachetools import TTLCache
from contextlib import contextmanager
from .merge import merge


def deephash(li):
    a = sorted(li, key=lambda i: str(i))
    for b, i in enumerate(a):
        if isinstance(b, list):
            a[i] = deephash(b)
    return a


class ApiValidationError(HTTPException):
    def __init__(self, desc):
        super().__init__(status_code=422, detail=desc)


class Api():

    def __init__(self, url):
        self.url = url
    
    async def request(self, method, path='', base_url=None, data=None, resp_json=True, content_type=None, **params):
        base_url = base_url or self.url
        if not path:
            url = base_url
        elif path.startswith('/'):
            url = f'{base_url}{path}'
        else:
            url = f'{base_url}/{path}'

        extra_params = {}
        if content_type:
            extra_params['content_type'] = content_type

        def json_converter(resp):
            async def run():
                return await resp.json(**extra_params)
            return run

        # not sure if the orjson as bytes -> str -> bytes makes using it moot
        async with aiohttp.ClientSession(json_serialize=lambda x: orjson.dumps(x).decode()) as session:
            async with session.request(method, url, params=params, data=data) as resp:
                ok = 300 > resp.status >= 200
                converter, other_converter = resp.text, json_converter(resp)

                if resp_json:
                    converter, other_converter = json_converter(resp), resp.text
                try:
                    detail = await converter()
                except Exception as e:
                    if ok:
                        raise e
                    else:
                        try:
                            detail = await other_converter()
                        except Exception:
                            detail = None
                
                if not ok:
                    raise HTTPException(resp.status, detail=detail)
                return detail
                
    async def get(self, path='', base_url=None, resp_json=True, content_type=None, ** params):
        return await self.request('get', path=path, base_url=base_url, resp_json=resp_json, content_type=content_type, **params)
    
    async def post(self, path='', base_url=None, data=None, resp_json=True, content_type=None, **params):
        return await self.request('post', path=path, base_url=base_url, data=data, resp_json=resp_json, content_type=content_type, **params)


class StaleApiException(Exception):
    pass


class SearchSyntax(BaseModel):
    OR: int = 1
    AND: int = 2


class QueryCache():
    def __init__(self, ttl=60*60):
        self.cache = TTLCache(float('inf'), ttl)

    def get(self, key):
        res = self.cache.get(key)
        if res:
            self.cache[key] = res
        return res
    
    def set(self, key, value):
        self.cache[key] = value


class SpecifyApi():
    SYNTAX = SearchSyntax()

    def __init__(self, base_url, collection, query_rows=50, ttl=60*60):
        self.collection = collection
        self.shortName = self.collection.replace('vouchers', '')
        self.url = '/'.join(s.strip('/') for s in [base_url, collection])
        self.api = Api(self.url)
        self.settings_json = None
        self.model_json = None
        self.follow_model = None
        self.column_model = None
        self.stale = True
        self.ready = False
        self.ttl = ttl
        self.cache = None
        self.DEFAULT_QUERY_ROWS = query_rows
    
    async def start(self):
        self.settings_json = await self.settings()
        self.model_json = await self._model()
        self.ready = True
        self.cache = QueryCache(self.ttl)
    
    async def settings(self):
        settings = await self.api.get('/resources/config/settings.json')
        if settings != self.settings_json:
            self.settings_json = settings
            # self.stale = True
        return {**{'shortName': self.shortName}, **self.settings_json[0]}
    
    async def _model(self):
        model = await self.api.get('/resources/config/fldmodel.json')
        if model != self.model_json:
            self.stale = True
            self.column_model = FieldModel.from_json(model)
            # raise StaleApiException('field model is stale')
        return self.column_model.serialized()

    def _query_term(self, colname, search, end_search = None):
        solrname = self.column_model.get(colname).get('solrname')
        return f'{solrname}:[{search} TO {end_search}]' if end_search is not None else f'{solrname}:{search}'

    def _query_builder(self, terms, ignore_missing=False):
        if isinstance(terms, (str, int, float)):
            return f'({terms})'
        if len(terms) == 1:
            return f'({terms[0]})'

        q = self._query_builder

        combs = {
            self.SYNTAX.OR: " OR ",
            self.SYNTAX.AND: " AND "
        }
        if terms[0] in combs:
            return f'({combs[terms[0]].join(q(t) for t in terms[1:])})'
        
        try:
            field = self.column_model._resolve_solrname_from_colname_or_solrname(terms[0])
        except Exception as e:
            if ignore_missing:
                ret = f'{terms[0]}:'
            else:
                raise e
        else:
            if field == self.follow_model.COLLECTION_SOLRNAME:
                return '*'
            else:
                ret = f'{field}:'


        
        if len(terms) == 2:
            return ret + q(terms[1])
        
        if len(terms) > 3:
            raise ApiValidationError('range search must be between only 2 values')

        return ret + f'[{terms[1]} TO {terms[2]}]'

    async def _query(self, queryTerms=["*"], 
                     ignore_missing=True,
                     geo_count=False, 
                     sort=None, asc=False, 
                     page=0):
        """queryTerms given as
            1. ["single search string",]
            2. [OR, 'terms', 'to', 'or']
            3. [AND, 'terms', 'to', 'and']
            4. ['field', 'search']
            5. ['field', 'from', 'to'] <- from and to must be strings. no additional search terms or parenthesis
            6. any combination of those ex. [SearchSyntax.OR, "single search", ["field", "search], [SearchSyntax.AND, ["field", "from", "to"], "other field", "search"]]
        
        ignore_missing will ignore fields in queryTerms or sort if they don't exist in the api fields
        geo_count returns results with geo pins
        when a field is used as sort, the results will be returned sorted by that field ascending or descending depending on asc
        page is the page number of results returned
        """

        # TODO: add image and query cache later. rebuild on other container rebuild
        # could just read in the whole db

        params = {
            'wt': 'json',
            'rows': self.DEFAULT_QUERY_ROWS,
            'start': page * self.DEFAULT_QUERY_ROWS,
            **({
                'facet': 'on', 
                'facet.field': 'geoc',
                'facet.limit': -1,
                'facet.mincount': 1,
               } if geo_count else {})
        }

        if sort:
            try:
                field = self.column_model._resolve_solrname_from_colname_or_solrname(sort)
            except  Exception as e:
                if ignore_missing:
                    sort = None
                    asc = False
                else:
                    raise e
            else:
                if field != self.follow_model.COLLECTION_SOLRNAME:
                    params['sort'] = f'{field} {"asc" if asc else "desc"}'
                    
        q = urllib.parse.urlencode({
            'q': self._query_builder(queryTerms, ignore_missing=ignore_missing),
            **params
        })

        resp = await self.api.get('/select?' + q, content_type='text/plain')
        
        for i in resp['response']['docs']:
            del i['contents']

            for solrname, value in i.items():
                if solrname == 'img':
                    continue
                i[solrname] = self.column_model._type_casts[solrname](value)

            for find, replace in self.column_model.changed_solrnames.items():
                try:
                    i[replace] = i[find]
                except:
                    continue
                else:
                    del i[find]

            i['coll'] = self.shortName
            if 'img' not in i:
                continue
            img = re.sub(r'(\w+)(:(\".*?\"|.))', r'"\1"\2', i['img'])
            img = json.loads(img)
            i['img'] = [{
                'id': k['AttachmentID'],
                'name': k['AttachmentLocation'],
                'title': k['Title'],
                'coll': self.shortName
            } for k in img]

        ret = {
            'docs': resp['response']['docs'],
            'last_page': -(-resp['response']['numFound'] // self.DEFAULT_QUERY_ROWS) - 1,
            'total': resp['response']['numFound']
        }
        if geo_count:
            geoc = resp['facet_counts']['facet_fields']['geoc']
            ret['facet_counts'] = {geoc[i]: geoc[i+1] for i in list(range(0,len(geoc),2))}

        return ret
    
    def query_cache_key(self, queryTerms, sort, asc):
        return str([
            0 if asc else 1,
            sort if sort else "",
            deephash(queryTerms)
        ])

    def rinse_cache_items(self, items, deep=False):
        """refreshes items in cache
        if deep is True
        replaces items in items array with cache instances and adds any new items to the cache
        This is a memory saving attempt to erase duplicates in cache
        """
        for i in range(len(items)):
            spid = items[i]['spid']
            citem = self.cache.get(spid)
            if deep:
                if citem:
                    items[i] = citem
                else:
                    self.cache.set(spid, items[i])

        return items

    async def query(self, queryTerms=["*"],
                    ignore_missing=True,
                    sort=None, asc=False,
                    page=0, cache=True):
        if sort:
            try:
                sort = self.column_model._resolve_solrname_from_colname_or_solrname(sort)
            except Exception as e:
                if ignore_missing:
                    sort = None
                    asc = False
                else:
                    raise e
        if not cache:
            return await self._query(queryTerms=queryTerms,
                                    ignore_missing=ignore_missing,
                                    geo_count=True,
                                    sort=sort, asc=asc,
                                    page=page)
        else:
            key = self.query_cache_key(queryTerms, sort, asc)
            cache_dict = self.cache.get(key)
            fresh = False
            
            if cache_dict:            
                if page > cache_dict['last_page']:
                    docs = []
                else:
                    try:
                        docs = cache_dict['pages'][page]
                    except KeyError:  # page missing
                        raw = await self._query(queryTerms=queryTerms,
                                                ignore_missing=ignore_missing,
                                                sort=sort, asc=asc,
                                                page=page)
                        docs = raw['docs']
                        cache_dict['pages'][page] = docs
                        fresh = True
            else:
                raw = await self._query(queryTerms=queryTerms,
                                        ignore_missing=ignore_missing,
                                        geo_count=True,
                                        sort=sort, asc=asc,
                                        page=page)
                docs = raw['docs']
                cache_dict = {
                    'facet_counts': raw['facet_counts'],
                    'pages': {page: docs},
                    'last_page': raw['last_page'],
                    'total': raw['total']
                }
                self.cache.set(key, cache_dict)
                fresh = True
            
            docs = self.rinse_cache_items(docs, deep=fresh)
            geos = cache_dict['facet_counts']
            last_page = cache_dict['last_page']

        return {
            'facet_counts': geos,
            'docs': docs,
            'last_page': last_page,
            'total': cache_dict['total']
        }




    
    async def check_if_stale(self):
        await self._model()
        return self.stale
    
    def set_follow_model(self, column_model):
        self.follow_model = column_model
        self.column_model.set_follow_model(column_model)
        self.stale = False

    def search(self):
        pass


class FieldModel():
    COLLECTION_SOLRNAME = 'coll'

    def __init__(self, *columns):
        self.columns = columns
        if columns[0].id() != 'collection':
            self.columns = (
                Column(
                    colname="collection",
                    solrname=self.COLLECTION_SOLRNAME,
                    solrtype="string",
                    advancedsearch="True",
                    colidx=0,
                    displaycolidx=0
                ),
                *columns
            )
            for c in self.columns[1:]:
                if c.model['colidx'] is not None:
                    c.model['colidx'] += 1
                if c.model['displaycolidx'] is not None:
                    c.model['displaycolidx'] += 1
        self._col_dict = {
            v.id(): i
            for i, v in enumerate(self.columns)
        }
        self._solr_dict = {
            v.get('solrname'): i
            for i, v in enumerate(self.columns)
        }
        self.follow_model = None
        self.changed_solrnames = {}

    @classmethod
    def from_json(cls, json_list):
        return cls(*[Column(**col) for col in json_list])
    
    def get(self, colname, *default):
        """return column by colname. 
        if default is given, use that if column doesn't exist"""
        if len(default)>1:
            raise TypeError(f'default must be a single argument. Multiple arguments were given instead: {default}')
        i = self._col_dict.get(colname, -1)
        if i == -1:
            if default:
                return default[0]
            raise KeyError(f'column {colname} not found')
        return self.columns[i]
    
    def _resolve_solrname_from_colname_or_solrname(self, field):
        try:
            return self.get_by_solrname(field).get('solrname')
        except KeyError:
            return self.get(field).get('solrname')

    def get_by_solrname(self, solrname):
        """return column by solrname. Raises KeyError if not found"""
        i = self._solr_dict[solrname]
        return self.columns[i]

    def serialized(self):
        return [
            col.dict() for col in self.columns
        ]

    def premerge_repr(self):
        return [
            {col.id(): col.get("displaycolidx")}
            for col in self.columns 
        ]

    def set_follow_model(self, follow):
        self.follow_model = follow
        old_dict = self._solr_dict
        self._solr_dict = {
            follow.get(v.id()).get('solrname'): i
            for i, v in enumerate(self.columns)
        }
        self.changed_solrnames = {}
        changed_solrnames = set(self._solr_dict) - set(old_dict)
        for solrname in changed_solrnames:
            self.changed_solrnames[self.columns[self._solr_dict[solrname]].get('solrname')] = solrname
        
        self._type_casts = {
            c.get('solrname'): c.SOLRTYPE_TRANSFORMS[follow.get(c.id()).get('solrtype')] for c in self.columns
        }

        self.stale = False

    def merged_model(self, other):
        a = self.premerge_repr()
        b = other.premerge_repr()

        merged_repr = merge(a, b)
        
        merged_cols = []
        for d in merged_repr:
            for k, colidx in d.items():  # single item
                cols = list(filter(None, [fields.get(k, None) for fields in [self, other]]))
                merged = cols[0]
                if len(cols) > 1:
                    merged = cols[0].merged_column(cols[1])

                merged.replace_displaycolidx(colidx)
                merged_cols.append(merged)
        f = FieldModel(*merged_cols)
        return f
                

        # merged = []
        # names = []
        # merged = {}
        # col_order = []
        # for cols in [self.columns, other]:
        #     i = 0
        #     for col in cols:
        #         colname = col.id()
        #         if colname not in merged:
        #             merged[colname] = [col]
        #             if len(col_order) > i+1:
        #                 i += 1
        #             col_order.append(colname)
        #         else:
        #             merged[colname].append(col)
        #         i += 1
        #     names.append( {col.id(): {'i': i, 'col': col} for i, col in enumerate(cols)} )

        # for col in (names[0] | names[1]):
            

        # for col in self.columns:
        #     merged_column = col.merged_column()
        #     merged.append()


class SolrType(str, Enum):
    string = "string"
    tdouble = "tdouble"
    int = "int"
    list = "list"


class ColumnType(str, Enum):
    calendar = "java.util.Calendar"
    string = "java.lang.String"
    bigDecimal = "java.math.BigDecimal"
    array = "java.util.Arrays"


class ColumnModel(BaseModel):
    colname: str
    solrname: str
    solrtype: SolrType
    title: str
    type: ColumnType
    width: Optional[int] = None
    sptable: Optional[str] = None
    sptabletitle: Optional[str] = None
    spfld: Optional[str] = None
    spfldtitle: Optional[str] = None
    treeid: Optional[str] = None
    treerank: Optional[int] = None
    colidx: Optional[int] = None
    advancedsearch: Optional[str] = None
    displaycolidx: Optional[int] = None


class Column():
    REQUIRED_FIELDS = { 'colname', 'solrname', 'solrtype' }
    SOLRTYPE_HEIRARCHY = [ 'string', 'tdouble', 'int', 'list' ]
    SOLRTYPE_TRANSFORMS = {
        'string': str, 
        'tdouble': float, 
        'int': int,
        'list': list  # for imgs
    }
    OPTIONAL_FIELDS = {
        "title": lambda s, d: d['colname'],
        "type": lambda s, d: s._determine_type(d),
        "width": None,
        "sptable": None,
        "sptabletitle": None,
        "spfld": None,
        "spfldtitle": None,
        "treeid": None,
        "treerank": None,
        "colidx": None,
        "advancedsearch": False,
        "displaycolidx": None
    }

    def __init__(self, **fields):
        if not self.REQUIRED_FIELDS.issubset(fields):
            raise TypeError(f'required fields missing: {self.REQUIRED_FIELDS - fields}')
        self.model = {}
        for k in self.REQUIRED_FIELDS:
            self.model[k] = fields[k]
        for k, v in self.OPTIONAL_FIELDS.items():
            if k in fields:
                self.model[k] = fields[k]
            else:
                try:
                    self.model[k] = v(self, fields)
                except:
                    self.model[k] = v
        if fields['solrname'] == 'img':
            self.model['solrtype'] = 'list'
            self.model['type'] = self._determine_type(self.model)
    
    def id(self):
        return self.model['colname']
    
    def get(self, key):
        return self.model[key]
    
    def replace_displaycolidx(self, x):
        self.model["displaycolidx"] = x

    def replace_column(self, other):
        self.model = other.model

    def merged_column(self, other):
        def raise_(ex):
            raise ex
        
        def _max(a, b, k):
            return max([a,b])

        def _max_with_none(a, b, k):
            """None < number"""
            if a is None:
                return b
            if b is None:
                return a
            if a > b:
                return a
            return b
        
        def assert_equal(a, b, k):
            if a == b:
                return a
            raise TypeError(f"{k}s don't match")

        merge_rules = {
            "colname": assert_equal,
            "solrname": _max,
            "solrtype": lambda a, b, k: min([a, b], key=lambda i: self.SOLRTYPE_HEIRARCHY.index(i)),
            "title": assert_equal,
            "type": assert_equal,
            "width": _max_with_none,
            "sptable": assert_equal,
            "sptabletitle": _max_with_none,
            "spfld": assert_equal,
            "spfldtitle": assert_equal,
            "treeid": assert_equal,
            "treerank": assert_equal,
            "colidx": _max_with_none,  # naive
            "advancedsearch": lambda a, b, k: "true" if "true" in [a, b] else a if a==b else raise_(TypeError(f"{k}s don't match")),
            "displaycolidx": _max_with_none  # naive
        }

        merged = {}
        for k, v in self.model.items():
            merged[k] = merge_rules[k](v, other.model[k], k)
        
        return Column(**merged)

    def __repr__(self):
        return f'{self.dict()}'

    def dict(self):
        return {**self.model}

    def _determine_type(self, col):
        if col.get('title', '').endswith('Date') and col['solrtype'] == 'int':
            return "java.util.Calendar"
        
        return {
            'int': "java.lang.String",
            'string': "java.lang.String",
            'tdouble': "java.math.BigDecimal",
            'list': "java.util.Arrays"   # for images
        }[col['solrtype']]
