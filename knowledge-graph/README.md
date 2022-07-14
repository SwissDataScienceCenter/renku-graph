# knowledge-graph

This is a microservice which provides API for the Graph DB.

## API
The following routes may be slightly different when accessed via the main renku api, which uses the gateway service (e.g. /api/kg/datasets)

| Method | Path                                                                     | Description                                                          |
|--------|--------------------------------------------------------------------------|----------------------------------------------------------------------|
| GET    | ```/api/knowledge-graph/datasets```                                          | Returns datasets filtered by the given predicates.                   |
| GET    | ```/api/knowledge-graph/datasets/:id```                                      | Returns details of the dataset with the given `id`                   |
| GET    | ```/api/knowledge-graph/entities```                                          | Returns entities filtered by the given predicates`                   |
| GET    | ```/api/knowledge-graph/graphql```                                           | Returns GraphQL endpoint schema                                      |
| POST   | ```/api/knowledge-graph/graphql```                                           | GraphQL query endpoint                                               |
| GET    | ```/api/knowledge-graph/projects/:namespace/:name```                         | Returns details of the project with the given `namespace/name`       |
| GET    | ```/api/knowledge-graph/projects/:namespace/:name/datasets```                | Returns datasets of the project with the given `path`                |
| GET    | ```/api/knowledge-graph/projects/:namespace/:name/files/:location/lineage``` | Returns the lineage for a the path (location) of a file on a project |
| GET    | ```/metrics```                                                           | Serves Prometheus metrics                                            |
| GET    | ```/ping```                                                              | To check if service is healthy                                       |
| GET    | ```/version```                                                           | Returns info about service version                                   |

#### GET /api/knowledge-graph/datasets

Finds datasets which `title`, `description`, `keywords`, or creator `name` matches the given `phrase` or returns all the
datasets if no `query` parameter is given.

NOTES:

* the `query` query parameter has to be url-encoded and it cannot be blank.
* the `sort` query parameter is optional and defaults to `title:asc`. Allowed property names are: `title`,
  `datePublished`, `date` and `projectsCount`.
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`.

**Response**
****

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

Assuming the total is `30` and the
URL `https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=2&per_page=10`

```
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="prev"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="next"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="first"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="last"
```

Response body example:

```
[
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "title":"rmDaYfpehl",
      "name": "mniouUnmal",
      "description": "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
      "published": {
        "datePublished": "2012-10-14", // optional property
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
      "date": "2012-10-14T03:02:25.639Z",            // either datePublished or dateCreated
      "projectsCount": 2,
      "keywords": ["grüezi", "안녕", "잘 지내?"],
      "images": [
        {
          "location": "image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
             }
          ]
        }
      ],
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
      "date": "2012-11-15T10:00:00.000Z",            // either datePublished or dateCreated
      "projectsCount": 1,
      "keywords": [],
      "images": [
        {
          "location": "https://blah.com/image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://blah.com/image.png"
             }
          ]
        }
      ],
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         }
      ]
   }
]
```

#### GET /api/knowledge-graph/entities

Allows finding `projects`, `datasets`, `workflows`, and `persons`.

Filtering:
* `query`      - to filter by matching field (e.g., title, keyword, description, etc. as specified below)
* `type`       - to filter by entity type(s); allowed values: `project`, `dataset`, `workflow`, and `person`; multiple `type` parameters allowed
* `creator`    - to filter by creator(s); the filter would require creator's name; multiple `creator` parameters allowed
* `visibility` - to filter by visibility(ies) (restricted vs. public); allowed values: `public`, `internal`, `private`; multiple `visibility` parameters allowed
* `since`      - to filter by entity's creation date to >= the given date
* `until`      - to filter by entity's creation date to <= the given date

**NOTE:** all query parameters have to be url-encoded.

When the `query` parameter is given, the match is done on the following fields:
* name/title
* namespace (for the project entity)
* creator (note: workflows has no creator for now)
* keyword
* description

Sorting:
* `matchingScore` - to sort by match score
* `name` - to sort by entity name - **default when no `query` parameter is given**
* `date` - to sort by entity creation date

**NOTE:** the sorting has to be requested by giving the `sort` query parameter with the property name and sorting order (`asc` or `desc`). The default order is ascending so `sort`=`name` means the same as `sort`=`name:asc`.

**Paging:**
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`; max value is `100`.

**Response**

| Status                     | Description                                    |
|----------------------------|------------------------------------------------|
| OK (200)                   | If results are found; `[]` if nothing is found |
| BAD_REQUEST (400)          | If illegal values for query parameters are given |
| INTERNAL SERVER ERROR (500)| Otherwise                                      |

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

Assuming the total is `30` and the URL is `https://renku/api/knowledge-graph/entities?query=phrase&sort=name:asc&page=2&per_page=10` the following links are added to the response:

```
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="prev"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="next"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="first"
Link: <https://renku/api/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="last"
```

Response body example:

```json
[
  {
    "type": "project",
    "matchingScore": 1.0055376,
    "name": "name",
    "path": "group/subgroup/name",
    "namespace": "group/subgroup",
    "visibility": "public",
    "date": "2012-11-15T10:00:00.000Z",
    "creator": "Jan Kowalski",
    "keywords": [
      "keyword1",
      "keyword2"
    ],
    "description": "desc",
    "_links": [
      {
        "rel": "details",
        "href": "http://t:5511/projects/group/subgroup/name"
      }
    ]
  },
  {
    "type": "dataset",
    "matchingScore": 3.364836,
    "name": "name",
    "visibility": "public",
    "date": "2012-11-15T10:00:00.000Z", // either datePublished or dateCreated
    "creators": [
      "Jan Kowalski",
      "Zoe"
    ],
    "keywords": [
      "keyword1",
      "keyword2"
    ],
    "description": "desc",
    "images": [
      {
        "location": "image.png",
        "_links":[
          {
            "rel":  "view",
            "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
          }
        ]
      }
    ],
    "_links": [
      {
        "rel": "details",
        "href": "http://t:5511/datasets/122334344"
      }
    ]
  },
  {
    "type": "workflow",
    "matchingScore": 5.364836,
    "name": "name",
    "visibility": "public",
    "date": "2012-11-15T10:00:00.000Z",
    "keywords": [
      "keyword1",
      "keyword2"
    ],
    "description": "desc",
    "_links": []
  },
  {
    "type": "person",
    "matchingScore": 4.364836,
    "name": "name",
    "_links": []
  }
]
```

#### GET /api/knowledge-graph/datasets/:id

Finds details of the dataset with the given `id`.

**Response**

| Status                     | Description                                                                                       |
|----------------------------|---------------------------------------------------------------------------------------------------|
| OK (200)                   | If dataset details are found                                                                      |
| UNAUTHORIZED (401)         | If given auth header cannot be authenticated                                                      |
| NOT_FOUND (404)            | If dataset is not found or user is not authorised to access project where this dataset belongs to |
| INTERNAL SERVER ERROR (500)| Otherwise                                                                                         |

Response body example:

```
{
  "_links" : [
    {
      "rel" : "self",
      "href" : "https://zemdgsw:9540/datasets/22222222-2222-2222-2222-222222222222"
    },
    {
      "rel" : "initial-version",
      "href" : "https://zemdgsw:9540/datasets/11111111-1111-1111-1111-111111111111"
    }
  ],
  "identifier" : "22222222-2222-2222-2222-222222222222",
  "versions" : {
    "initial": "11111111-1111-1111-1111-111111111111"
  },
  "title" : "dataset title",
  "name" : "dataset alternate name",
  "url" : "http://host/url1",                     // optional property
  "sameAs" : "http://host/url2",                  // optional property when no "derivedFrom" exists
  "derivedFrom" : "http://host/url1",             // optional property when no "sameAs" exists
  "description" : "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
  "published" : {
    "datePublished" : "2012-10-14",               // optional property
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
  "created" : "2012-10-15T03:02:25.639Z",         // optional property
  "hasPart" : [
    {
      "atLocation" : "data/dataset-name/file1"
    },
    {
      "atLocation" : "data/dataset-name/file2"
    }
  ],
  "project":  {
    "_links" : [
      {
        "rel" : "project-details",
        "href" : "https://zemdgsw:9540/projects/namespace1/project1-name"
      }
    ],
    "path" : "namespace1/project1-name",
    "name" : "project1 name"
  },
  "usedIn" : [
    {
      "_links" : [
        {
          "rel" : "project-details",
          "href" : "https://zemdgsw:9540/projects/namespace1/project1-name"
        }
      ],
      "path" : "namespace1/project1-name",
      "name" : "project1 name"
    },
    {
      "_links" : [
        {
          "rel" : "project-details",
          "href" : "https://zemdgsw:9540/projects/namespace2/project2-name"
        }
      ],
      "path" : "namespace2/project2-name",
      "name" : "project2 name"
    }
  ],
  "keywords": [ "rldzpwo", "gfioui" ],
  "images": [
    {
      "location": "image.png",
      "_links":[  
         {  
            "rel":  "view",
            "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
         }
      ]
    },
    {
      "location": "http://host/external-image.png",
      "_links":[  
         {  
            "rel":  "view",
            "href": "http://host/external-image.png"
         }
      ]
    }
  ],
}
```

#### GET /api/knowledge-graph/graphql

Returns Knowledge Graph GraphQL endpoint schema.

**Response**

| Status                     | Description                    |
|----------------------------|--------------------------------|
| OK (200)                   | Schema of the GraphQL endpoint |
| INTERNAL SERVER ERROR (500)| Otherwise                      |

**A curl command example**

```
curl -X POST -v -H "Content-Type: application/json" http://localhost:9004/api/knowledge-graph/graphql -d '{ "query": "{ lineage(projectPath: \"<namespace>/<project-name>\") { nodes { id label } edges { source target } } }"}'
```

#### POST /api/knowledge-graph/graphql

Endpoint to perform GraphQL queries on the Knowledge Graph data.

**Response**

| Status                     | Description                                  |
|----------------------------|----------------------------------------------|
| OK (200)                   | Body containing queried data                 |
| UNAUTHORIZED (401)         | If given auth header cannot be authenticated |
| INTERNAL SERVER ERROR (500)| Otherwise                                    |

**Available queries**

* Lineage

Query example:

```
{
  "query": "{ 
    lineage(projectPath: \"namespace/project\", filePath: \"zhbikes.parquet\") {
      nodes { id location label type } 
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
          "location": "data/zhbikes",
          "label": "data/zhbikes@bbdc4293b79535ecce7c143b29538f7ff01db297",
          "type": "Directory"
        },
        {
          "id": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
          "location": ".renku/workflow/3144e9aa470441cf905f94105e1d27ca_python.cwl",
          "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet",
          "type": "ProcessRun"
        },
        {
          "id": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet",
          "location": "data/preprocessed/zhbikes.parquet",
          "label": "data/preprocessed/zhbikes.parquet@1aaf360c2267bedbedb81900a214e6f36be04e87",
          "type": "File"
        }
      ]
    }
  }
}
```

#### GET /api/knowledge-graph/projects/:namespace/:name

Finds details of the project with the given `namespace/name`. The endpoint requires an authorization token to be passed
in the request for non-public projects. Supported headers are:

- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab
- There's no need for a security headers for public projects

**Response**

| Status                     | Description                                                                                            |
|----------------------------|--------------------------------------------------------------------------------------------------------|
| OK (200)                   | If project with the given `namespace/name` can be found                                                |
| UNAUTHORIZED (401)         | If given auth header cannot be authenticated                                                           |
| NOT_FOUND (404)            | If there is no project with the given `namespace/name` or user is not authorised to access the project |
| INTERNAL SERVER ERROR (500)| Otherwise                                                                                              |

Response body example:

```
{
  "identifier":  123,
  "path":        "namespace/project-name", 
  "name":        "Some project name",
  "description": "This is a longer text describing the project", // optional
  "visibility":  "public|private|internal",
  "created": {
    "dateCreated": "2001-09-05T10:48:29.457Z",
    "creator": { // optional
      "name":  "author name",
      "email": "author@mail.org" // optional
    }
  },
  "updatedAt":  "2001-10-06T10:48:29.457Z",
  "urls": {
    "ssh":    "git@renku.io:namespace/project-name.git",
    "http":   "https://renku.io/gitlab/namespace/project-name.git",
    "web":    "https://renku.io/gitlab/namespace/project-name",
    "readme": "https://renku.io/gitlab/namespace/project-name/blob/master/README.md"
  },
  "forking": {
    "forksCount": 1,
    "parent": { // optional
      "path":       "namespace/parent-project",
      "name":       "Parent project name",
      "created": {
        "dateCreated": "2001-09-04T10:48:29.457Z",
        "creator": { // optional
          "name":  "parent author name", 
          "email": "parent.author@mail.org" // optional
        }
      }
    }
  },
  "keywords": ["keyword1", "keyword2"],
  "starsCount": 0,
  "permissions": {
    "projectAccess": { // optional
      "level": {"name": "Developer", "value": 30}
    },
    "groupAccess": { // optional
      "level": {"name": "Guest", "value": 10}
    }
  },
  "statistics": {
    "commitsCount":     1,
    "storageSize":      1000,
    "repositorySize":   1001,
    "lfsObjectsSize":   0,
    "jobArtifactsSize": 0
  },
  "version": "9",  // optional
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

#### GET /api/knowledge-graph/projects/:namespace/:name/datasets

Finds list of datasets of the project with the given `namespace/name`.

**Response**

| Status                     | Description                                                       |
|----------------------------|-------------------------------------------------------------------|
| OK (200)                   | If there are datasets for the project or `[]` if nothing is found |
| UNAUTHORIZED (401)         | If given auth header cannot be authenticated                      |
| NOT_FOUND (404)            | If there is no project with the given `namespace/name` or user is not authorised to access this project |
| INTERNAL SERVER ERROR (500)| Otherwise                                                         |

Response body example:

```
[  
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "versions" : {
        "initial": "11111111-1111-1111-1111-111111111111"
      },
      "title": "rmDaYfpehl",
      "name": "mniouUnmal",
      "sameAs": "http://host/url1",
      "derivedFrom" : "http://host/url1",
      "images": [],
      "_links": [  
         {  
            "rel": "details",
            "href": "http://t:5511/datasets/9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c"
         },
         {
           "rel" : "initial-version",
           "href" : "https://zemdgsw:9540/datasets/11111111-1111-1111-1111-111111111111"
         }
      ]
   },
   {  
      "identifier": "a1b1cb86-c664-4250-a1e3-578a8a22dcbb",
      "versions" : {
        "initial": "22222222-2222-2222-2222-222222222222"
      },
      "name": "a",
      "sameAs" : "http://host/url2",        // optional property when no "derivedFrom" exists
      "derivedFrom" : "http://host/url2",   // optional property when no "sameAs" exists
      "images": [
        {
          "location": "image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
             }
          ]
        },
        {
          "location": "http://host/external-image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "http://host/external-image.png"
             }
          ]
        }
      ],
      "_links": [  
         {  
            "rel": "details",
            "href": "http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         },
         {
           "rel" : "initial-version",
           "href" : "https://zemdgsw:9540/datasets/22222222-2222-2222-2222-222222222222"
         }
      ]
   }
]
```

#### GET /api/knowledge-graph/projects/:namespace/:name/files/:location/lineage

Fetches lineage for a given project `namespace`/`name` and file `location` (URL-encoded relative path to the file). This endpoint is intended to replace the graphql endpoint.

| Status                     | Description                                                       |
|----------------------------|-------------------------------------------------------------------|
| OK (200)                   | If there are datasets for the project or `[]` if nothing is found |
| UNAUTHORIZED (401)         | If given auth header cannot be authenticated                      |
| NOT_FOUND (404)            | If there is no project with the given `namespace/name` or user is not authorised to access this project |
| INTERNAL SERVER ERROR (500)| Otherwise                                                         |


Response body example:

```json
{
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
        "location": "data/zhbikes",
        "label": "data/zhbikes@bbdc4293b79535ecce7c143b29538f7ff01db297",
        "type": "Directory"
      },
      {
        "id": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
        "location": ".renku/workflow/3144e9aa470441cf905f94105e1d27ca_python.cwl",
        "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet",
        "type": "ProcessRun"
      },
      {
        "id": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet",
        "location": "data/preprocessed/zhbikes.parquet",
        "label": "data/preprocessed/zhbikes.parquet@1aaf360c2267bedbedb81900a214e6f36be04e87",
        "type": "File"
      }
    ]
  }
}
```

#### GET /api/knowledge-graph/spec.json

Returns OpenApi json spec 

| Status                     | Description          |
|----------------------------|----------------------|
| OK (200)                   | If spec is found     |
| INTERNAL SERVER ERROR (500)| Otherwise            |

#### GET /metrics  (Internal use only)

Serves Prometheus metrics.

**Response**

| Status                     | Description          |
|----------------------------|----------------------|
| OK (200)                   | If metrics are found |
| INTERNAL SERVER ERROR (500)| Otherwise            |

#### GET /ping (Internal use only)

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### GET /version  (Internal use only)

Returns info about service version

**Response**

| Status                     | Description            |
|----------------------------|------------------------|
| OK (200)                   | If version is returned |
| INTERNAL SERVER ERROR (500)| Otherwise              |

Response body example:

```json
{
  "name": "commit-event-service",
  "versions": [
    {
      "version": "2.3.0"
    }
  ]
}
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
