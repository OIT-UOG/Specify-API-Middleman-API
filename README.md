# Specify API middleman API

This is an api that consumes the different specify collection apis and provides a fluid interface that combines their results.  
This was made to overcome the task of  

* combining them in the client, which was leading to an increasing amount of problems and forseeable strain on the APIs 
* or combining the SOLR collections themselves (the varying collections having their own merge issues)
  
This is not a 100% smooth merge. Please take into account the [considerations](#considerations).

## Installation

\* for fastest speeds, this should be deployed on the same server as the SOLR apis.  

1. git clone this
2. ensure docker and docker-compose are installed and setup correctly
3. fill out an `.env` file following the `sample.env` file as an example
4. `docker-compose up -d`

## Deployment

For fastest speeds, this should be deployed on the same server as the SOLR apis.  
That is how this is set up currently, so check the deployment section in that repo.

## Usage

Docs can be found at the `/docs` endpoint.  
Basic usage is as follows:

1. Grab meta information to setup your retrieval  
    * `/settings` will give you 
      * the available collections
      * needed enums for `/search`'s `queryTerm` parameter
      * image base URLs to be used to complete the img urls given by `/search`
    * `/model` will give you 
      * the meta information for the fields in the items returned by `/search`
2. use the `/search` endpoint

## Development / Troubleshooting

* check out the logs: `docker-compose logs -f`
* use the dev compose file when developing `docker-compose -f dev-docker-compose.yml up -d --build --force-recreate`
* if you'd like to use VSC's debug tool, spin it up with the debug compose file `docker-compose -f debug-docker-compose.yml up -d --build --force-recreate`, open up a **Remote: Container**, and start debugging

## Considerations

1. Collection name should not be used in queryTerms in the `/search` endpoint. It will just be ignored (rather, the field search will be replaced with `"*"`). You can sort by collections though.
2. Due to the nature of the different collections under the hood having columns of conflicting datatypes, sorting may be a little off. The SOLR apis each sort by their own data types, then selection between the different collections is determined by their "merged" data type. ex.  
    | collection | \| | A | B | C |
    |-|-|-|-|-|
    | data type | \| | `int` | `int` | `str` |
    | sorted values | \| | `[1,2,10]` | `[10,11,12]` | `['100','20','a']` |
    | adjusted values | \| | `['1','2','10']` | `['10','11','12']` | `['100','20','a']` |  
    
    **resulting order**: 1 10 100 11 12 2 10 20 a
3. Not all collections have the same fields. As such, searching by them may have not easily definable behavior. It should be pretty smooth after [`77d88a6`](https://github.com/OIT-UOG/Specify-API-Middleman-API/commit/77d88a627d3f9c26785dcc4a0a03edc9590c8f08) , but searching for "no value" is still not possible.
    
