# knowledge-graph

This is a microservice which provides API for the Graph DB.

## API

| Method  | Path                                                                    | Description                                                    |
|---------|-------------------------------------------------------------------------|----------------------------------------------------------------|
|  GET    | ```/knowledge-graph/datasets```                                         | Returns datasets.                                              |
|  GET    | ```/knowledge-graph/datasets/:id```                                     | Returns details of the dataset with the given `id`             |
|  GET    | ```/knowledge-graph/graphql```                                          | Returns GraphQL endpoint schema                                |
|  POST   | ```/knowledge-graph/graphql```                                          | GraphQL query endpoint                                         |
|  GET    | ```/knowledge-graph/projects/:namespace/:name```                        | Returns details of the project with the given `namespace/name` |
|  GET    | ```/knowledge-graph/projects/:namespace/:name/datasets```               | Returns datasets of the project with the given `path`          |
|  GET    | ```/ping```                                                             | To check if service is healthy                                 |

#### GET /knowledge-graph/datasets?query=\<phrase\>&sort=\<property\>:asc|desc&page=\<page\>&per_page=\<per_page\>

Finds datasets which `name`, `description` or creator `name` matches the given `phrase` or returns all the datasets if no `query` parameter is given.

NOTES: 
* the `phrase` query parameter has to be url encoded and it cannot be blank.
* the `sort` query parameter is optional and defaults to `name:asc`. Allowed property names are: `name`, `datePublished` and `projectsCount`.
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`.

**Response**

| Status                     | Description                                                                                    |
|----------------------------|------------------------------------------------------------------------------------------------|
| OK (200)                   | If there are datasets for the project or `[]` if nothing is found                              |
| BAD_REQUEST (400)          | If the `query` parameter is blank or `sort` is invalid or `page` or `per_page` is not positive |
| INTERNAL SERVER ERROR (500)| Otherwise                                                                                      |

Response headers:

| Header        | Description                                                                           |
|---------------|---------------------------------------------------------------------------------------|
| `Total`       | The total number of items                                                             |
| `Total-Pages` | The total number of pages                                                             |
| `Per-Page`    | The number of items per page                                                          |
| `Page`        | The index of the current page (starting at 1)                                         |
| `Next-Page`   | The index of the next page (optional)                                                 |
| `Prev-Page`   | The index of the previous page (optional)                                             |
| `Link`        | The set of `prev`/`next`/`first`/`last` link headers (`prev` and `next` are optional) |

Link response header example:

Assuming the total is `30` and the URL `https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=2&per_page=10`

```
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="prev"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="next"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="first"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="last"
```

Response body example:
```
[  
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "name":"rmDaYfpehl",
      "description": "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
      "published": {
        "datePublished": "2012-10-14T03:02:25.639Z", // optional property
        "creator": [
          {
            "name": "e wmtnxmcguz"
          },
          {
            "name": "iilmadw vcxabmh",
            "email": "ticUnrW@cBmrdomoa"             // optional property
          }
        ]
      },
      "projectsCount": 2,
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c"
         }
      ]
   },
   {  
      "identifier": "a1b1cb86-c664-4250-a1e3-578a8a22dcbb",
      "name": "a",
      "published": {
        "creator": [
          {
            "name": "e wmtnxmcguz"
          }
        ]
      },
      "projectsCount": 1,
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         }
      ]
   }
]
```

#### GET /knowledge-graph/datasets/:id

Finds details of the dataset with the given `id`.

**Response**

| Status                     | Description                   |
|----------------------------|-------------------------------|
| OK (200)                   | If dataset details are found  |
| NOT_FOUND (404)            | If dataset is not found       |
| INTERNAL SERVER ERROR (500)| Otherwise                     |

Response body example:
```
{
  "_links" : [
    {
      "rel" : "self",
      "href" : "https://zemdgsw:9540/datasets/6f622603-2129-4058-ad29-3ff927481461"
    }
  ],
  "identifier" : "6f622603-2129-4058-ad29-3ff927481461",
  "name" : "dataset name",
  "url" : "http://host/url1",  // optional property
  "sameAs" : "http://host/url1",  // optional property
  "description" : "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
  "published" : {
    "datePublished" : "2012-10-14T03:02:25.639Z", // optional property
    "creator" : [
      {
        "name" : "e wmtnxmcguz"
        "affiliation" : "SDSC"                    // optional property
      },
      {
        "name" : "iilmadw vcxabmh",
        "email" : "ticUnrW@cBmrdomoa"             // optional property
      }
    ]
  },
  "hasPart" : [
    {
      "name" : "o",
      "atLocation" : "data/dataset-name/file1"
    },
    {
      "name" : "rldzpwo",
      "atLocation" : "data/dataset-name/file2"
    }
  ],
  "isPartOf" : [
    {
      "_links" : [
        {
          "rel" : "project-details",
          "href" : "https://zemdgsw:9540/projects/namespace1/project1-name"
        }
      ],
      "path" : "namespace1/project1-name",
      "name" : "project1 name",
      "created" : {
        "dateCreated" : "1970-05-12T06:06:41.448Z",
        "agent" : {
          "email" : "n@ulQdsXl",
          "name" : "v imzn"
        }
      }
    },
    {
      "_links" : [
        {
          "rel" : "project-details",
          "href" : "https://zemdgsw:9540/projects/namespace2/project2-name"
        }
      ],
      "path" : "namespace2/project2-name",
      "name" : "project2 name",
      "created" : {
        "dateCreated" : "1970-06-12T06:06:41.448Z",
        "agent" : {
          "email" : "name@ulQdsXl",
          "name" : "v imzn"
        }
      }
    }
  ]
}
```

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
          "source": "/blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
          "target": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87"
        },
        {
          "source": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
          "target": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet"
        }
      ],
      "nodes": [
        {
          "id": "/blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
          "label": "data/zhbikes@bbdc4293b79535ecce7c143b29538f7ff01db297"
        },
        {
          "id": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
          "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet"
        },
        {
          "id": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet",
          "label": "data/preprocessed/zhbikes.parquet@1aaf360c2267bedbedb81900a214e6f36be04e87"
        }
      ]
    }
  }
}
```

#### GET /knowledge-graph/projects/:namespace/:name

Finds details of the project with the given `namespace/name`.

**Response**

| Status                     | Description                                             |
|----------------------------|---------------------------------------------------------|
| OK (200)                   | If project with the given `namespace/name` can be found |
| NOT_FOUND (404)            | If there is no project with the given `namespace/name`  |
| INTERNAL SERVER ERROR (500)| Otherwise                                               |

Response body example:
```
{
  "path": "namespace/project-name", 
  "name": "Some project name",
  "created": {
    "dateCreated": "2001-09-05T10:48:29.457Z",
    "creator": {
      "name": "author name",
      "email": "author@mail.org"
    }
  },
  "url": {
    "ssh": "git@renku.io:namespace/project-name.git",
    "http": "https://renku.io/gitlab/namespace/project-name.git"
  },
  "_links":[  
    {  
      "rel":"self",
      "href":"http://t:5511/projects/namespace/project-name"
    },
    {  
      "rel":"datasets",
      "href":"http://t:5511/projects/namespace/project-name/datasets"
    }
  ]
}
```

#### GET /knowledge-graph/projects/:namespace/:name/datasets

Finds list of datasets of the project with the given `namespace/name`.

**Response**

| Status                     | Description                                                       |
|----------------------------|-------------------------------------------------------------------|
| OK (200)                   | If there are datasets for the project or `[]` if nothing is found |
| INTERNAL SERVER ERROR (500)| Otherwise                                                         |

Response body example:
```
[  
   {  
      "identifier":"9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "name":"rmDaYfpehl",
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c"
         }
      ]
   },
   {  
      "identifier":"a1b1cb86-c664-4250-a1e3-578a8a22dcbb",
      "name":"a",
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         }
      ]
   }
]
```

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

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
