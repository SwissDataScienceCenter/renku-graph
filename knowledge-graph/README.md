# knowledge-graph

This is a microservice which provides API for the Graph DB.

## API

| Method  | Path                                                                    | Description                                                    |
|---------|-------------------------------------------------------------------------|----------------------------------------------------------------|
|  GET    | ```/knowledge-graph/datasets?query=<phrase>&sort=<property:asc\desc>``` | Returns datasets matching the given `phrase`.                  |
|  GET    | ```/knowledge-graph/datasets/:id```                                     | Returns details of the dataset with the given `id`             |
|  GET    | ```/knowledge-graph/graphql```                                          | Returns GraphQL endpoint schema                                |
|  POST   | ```/knowledge-graph/graphql```                                          | GraphQL query endpoint                                         |
|  GET    | ```/knowledge-graph/projects/:namespace/:name```                        | Returns details of the project with the given `namespace/name` |
|  GET    | ```/knowledge-graph/projects/:namespace/:name/datasets```               | Returns datasets of the project with the given `path`          |
|  GET    | ```/ping```                                                             | To check if service is healthy                                 |

#### GET /knowledge-graph/datasets?query=\<phrase\&sort=\<property\>:asc|desc

Finds datasets which `name`, `description` or creator `name` matches the given `phrase`.

NOTES: 
* the `phrase` query parameter has to be url encoded.
* the `sort` query parameter is optional and defaults to `name:asc`. Allowed property names are: `name`.

**Response**

| Status                     | Description                                            |
|----------------------------|--------------------------------------------------------|
| OK (200)                   | If there are datasets for the project                  |
| BAD_REQUEST (400)          | If the `query` parameter is blank or `sort` is invalid |
| NOT_FOUND (404)            | If there are no datasets found or no `query` parameter |
| INTERNAL SERVER ERROR (500)| Otherwise                                              |

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
  "description" : "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
  "published" : {
    "datePublished" : "2012-10-14T03:02:25.639Z", // optional property
    "creator" : [
      {
        "name" : "e wmtnxmcguz"
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

* Data-sets

Query example:
```
{
  "query": "{ 
    datasets(projectPath: \"namespace/project\") {
      identifier!
      name!
      description
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
      }
      isPartOf {
        path!
        name!
        created! {
          dateCreated! 
          agent { 
            email!
            name!
          }
        }
      }
    } 
  }"
}

```
Response body example:
```
{
  "data": {
    "datasets": [
      {
        "identifier": "e1fc7b62-e021-434b-9264-dda336bddd4f",
        "name": "dataset name",
        "description": "Data-set long description",
        "published": {
          "datePublished": "2019-07-30",
          "creator": []
        },
        "hasPart": [],
        "isPartOf": [{
          "path": "namespace/project",
          "name": "project name",
          "created" : {
            "dateCreated" : "1970-05-12T06:06:41.448Z",
            "agent" : {
              "email" : "n@ulQdsXl",
              "name" : "v imzn"
            }
          }
        }]
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
          "atLocation"": "data/dataset-name/file1"
        }],
        "isPartOf": [{
          "path": "namespace/project",
          "name": "project name",
          "created" : {
            "dateCreated" : "1970-05-12T06:06:41.448Z",
            "agent" : {
              "email" : "n@ulQdsXl",
              "name" : "v imzn"
            }
          }
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
          "atLocation"": "data/chOorWhraw-name/file1"
        }, {
          "name": "file2"  
          "atLocation"": "data/chOorWhraw-name/file2"
        }],
        "isPartOf": [{
          "path": "namespace/project1",
          "name": "project1 name",
          "created" : {
            "dateCreated" : "1970-10-12T06:06:41.448Z",
            "agent" : {
              "email" : "n@ulQdsXl",
              "name" : "v imzn"
            }
          }
        }, {
          "path": "namespace/project2",
          "name": "project2 name",
          "created" : {
            "dateCreated" : "1970-05-12T06:06:41.448Z",
            "agent" : {
              "email" : "n@ulQdsXl",
              "name" : "v imzn"
            }
          }
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
    "datasets": null
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

| Status                     | Description                            |
|----------------------------|----------------------------------------|
| OK (200)                   | If there are datasets for the project  |
| NOT_FOUND (404)            | If there are no datasets found         |
| INTERNAL SERVER ERROR (500)| Otherwise                              |

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
