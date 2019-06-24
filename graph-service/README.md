# graph-service

This is a microservice which provides API for the Graph DB.

## API

| Method  | Path                               | Description                                  |
|---------|------------------------------------|----------------------------------------------|
|  GET    | ```/ping```                        | To check if service is healthy               |
|  GET    | ```/knowledge-graph/graphql```     | Returns GraphQL endpoint schema              |
|  POST   | ```/knowledge-graph/graphql```     | GraphQL query endpoint                       |

#### GET /ping

Verifies service's health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### GET /knowledge-graph/graphql

Returns Knowledge Graph GraphQL endpoint schema.

**Response**

| Status                     | Description                    |
|----------------------------|--------------------------------|
| OK (200)                   | Schema of the GraphQL endpoint |
| INTERNAL SERVER ERROR (500)| Otherwise                      |

#### POST /knowledge-graph/graphql

Endpoint to perform GraphQL queries on the Knowledge Graph data.

**Response**

| Status                     | Description                    |
|----------------------------|--------------------------------|
| OK (200)                   | Body containing queried data   |
| INTERNAL SERVER ERROR (500)| Otherwise                      |

Example of a query:
```
{
  "query": "{ 
    lineage(projectPath: \"rokroskar/vom_natt\") {
      nodes { id label } 
      edges { source target } 
    } 
  }"
}

```
Example of a response body:
```
{
  "data": {
    "lineage": {
      "edges": [
        {
          "source": "file:///blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
          "target": "file:///commit/1aaf360c2267bedbedb81900a214e6f36be04e87"
        },
        {
          "source": "file:///commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
          "target": "file:///blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet"
        }
      ],
      "nodes": [
        {
          "id": "file:///blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
          "label": "data/zhbikes@bbdc4293b79535ecce7c143b29538f7ff01db297"
        },
        {
          "id": "file:///commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
          "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet"
        },
        {
          "id": "file:///blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet",
          "label": "data/preprocessed/zhbikes.parquet@1aaf360c2267bedbedb81900a214e6f36be04e87"
        }
      ]
    }
  }
}
```

In case there's no data found for a given query, the response `json` will contain a property with the queried resource name and a `null` value. For instance if a user performs a lineage query and there's no lineage found, the response will look like below:
```
{
  "data": {
    "lineage": null
  }
}
```

**A curl command example**
```
curl -X POST -v -H "Content-Type: application/json" http://localhost:9004/knowledge-graph/graphql -d '{ "query": "{ lineage(projectPath: \"<namespace>/<project-name>\") { nodes { id label } edges { source target } } }"}'
```

## Trying out

The graph-service is a part of a multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f graph-service/Dockerfile -t graph-service .
```

- run the service

```bash
docker run --rm -p 9004:9004 graph-service
```
