# knowledge-graph

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

**Available queries**

* Lineage

Query example:
```
{
  "query": "{ 
    lineage(projectPath: \"namespace/project\", commitId: \"6e4f4cc8d30886a9f17192c65db6c799602bcd7d\", filePath: \"zhbikes.parquet\") {
      nodes { id label } 
      edges { source target } 
    } 
  }"
}

```
Response body example:
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

* Data-sets

Query example:
```
{
  "query": "{ 
    dataSets(projectPath: \"namespace/project\") {
      identifier!
      name!
      description
      created! {
        dateCreated! 
        agent { 
          email!
          name!
        }
      }
      published {
        datePublished
        creator {
          name!
          email
        }
      }
      hasPart {
        name!
        atLocation!
        dateCreated!
      }    
    } 
  }"
}

```
Response body example:
```
{
  "data": {
    "dataSets": [
      {
        "identifier": "e1fc7b62-e021-434b-9264-dda336bddd4f",
        "name": "data-set name",
        "description": "Data-set long description",
        "created": {
          "dateCreated": "1981-09-05T10:38:29.457Z",
          "agent": {
            "email": "user1@host",
            "name": "user 1"
          }
        },
        "published": {
          "datePublished": "2019-07-30",
          "creator": []
        },
        "hasPart": []
      },
      {
        "identifier": "5b7a1394-93b5-4e75-932d-e041cf46349d",
        "name": "xr",
        "description": null,
        "created": {
          "dateCreated": "1991-09-05T10:38:29.457Z",
          "agent": {
            "email": "user2@host",
            "name": "user 2"
          }
        },
        "published": {
          "datePublished": null,
          "creator": [{
            "name": "author1 name",
            "email": null
          }]
        },
        "hasPart": [{
          "name": "file1",
          "atLocation"": "data/data-set-name/file1",
          "dateCreated": "1991-09-05T10:38:29.457Z"
        }]
      },
      {
        "identifier": "b1aa58af-a488-4bae-97b4-d6d349f98412",
        "name": "chOorWhraw",
        "description": null,
        "created": {
          "dateCreated": "2001-09-05T10:38:29.457Z",
          "agent": {
            "email": "user3@host",
            "name": "user 3"
          }
        },
        "published": {
          "datePublished": "2001-09-05T10:38:29.457Z",
          "creator": [{
            "name": "author1 name",
            "email": "author1@mail.org"
          }, {
            "name": "author2 name",
            "email": "author2@mail.org"
          }]
        },
        "hasPart": [{
          "name": "file1",  
          "atLocation"": "data/chOorWhraw-name/file1",
          "dateCreated": "2001-09-05T10:38:29.457Z"
        }, {
          "name": "file2"  
          "atLocation"": "data/chOorWhraw-name/file2",
          "dateCreated": "2001-09-05T10:48:29.457Z"
        }]
      }
    ]
  }
}
```

In case there's no data found for a given query, the response `json` will contain a property with the queried resource name and a `null` value. For instance if a user performs a lineage query and there's no lineage found, the response will look like below:
```
{
  "data": {
    "dataSets": null
  }
}
```

**A curl command example**
```
curl -X POST -v -H "Content-Type: application/json" http://localhost:9004/knowledge-graph/graphql -d '{ "query": "{ lineage(projectPath: \"<namespace>/<project-name>\") { nodes { id label } edges { source target } } }"}'
```

## Trying out

The knowledge-graph is a part of a multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f knowledge-graph/Dockerfile -t knowledge-graph .
```

- run the service

```bash
docker run --rm -p 9004:9004 knowledge-graph
```
