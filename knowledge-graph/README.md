# knowledge-graph

This is a microservice which provides API for the Graph DB.

## API

The following routes may be slightly different when accessed via the main Renku API, which uses the gateway service (e.g. /api/kg/datasets)

| Method | Path                                                                     | Description                                                                          |
|--------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| GET    | ```/knowledge-graph/datasets```                                          | Returns datasets filtered by the given predicates.                                   |
| GET    | ```/knowledge-graph/datasets/:id```                                      | Returns details of the dataset with the given `id`                                   |
| GET    | ```/knowledge-graph/entities```                                          | Returns entities filtered by the given predicates`                                   |
| GET    | ```/knowledge-graph/entities/current-user/recently-viewed```             | Returns entities recently viewed by the user introducing himself with the token.     |
| GET    | ```/knowledge-graph/ontology```                                          | Returns ontology used in the Knowledge Graph                                         |
| POST   | ```/knowledge-graph/projects```                                          | Creates a project from the given payload in GitLab and in the Knowledge Graph        |
| DELETE | ```/knowledge-graph/projects/:namespace/:name```                         | Deletes the project with the given `namespace/name` from knowledge-graph and GitLab  |
| GET    | ```/knowledge-graph/projects/:namespace/:name```                         | Returns details of the project with the given `namespace/name`                       |
| PATCH  | ```/knowledge-graph/projects/:namespace/:name```                         | Updates selected properties of the project with the given `namespace/name`           |
| GET    | ```/knowledge-graph/projects/:namespace/:name/datasets```                | Returns datasets of the project with the given `slug`                                |
| GET    | ```/knowledge-graph/projects/:namespace/:name/datasets/:dsName/tags```   | Returns tags of the dataset with the given `dsName` on project with the given `slug` |
| GET    | ```/knowledge-graph/projects/:namespace/:name/files/:location/lineage``` | Returns the lineage for a the path (location) of a file on a project                 |
| GET    | ```/knowledge-graph/spec.json```                                         | Returns OpenAPI specification of the service's resources                             |
| GET    | ```/knowledge-graph/users/:id/projects```                                | Returns all user's projects                                                          |
| GET    | ```/knowledge-graph/version```                                           | Returns info about service version                                                   |
| GET    | ```/metrics```                                                           | Serves Prometheus metrics                                                            |
| GET    | ```/ping```                                                              | To check if service is healthy                                                       |
| GET    | ```/version```                                                           | Returns info about service version; same as `GET /knowledge-graph/version`           |

#### GET /knowledge-graph/datasets

Finds datasets which `title`, `description`, `keywords`, or creator `name` matches the given `phrase` or returns all the
datasets if no `query` parameter is given.

**NOTES:**

* the `query` query parameter has to be url-encoded and it cannot be blank.
* the `sort` query parameter is optional and defaults to `title:asc`. Allowed property names are: `title`,
  `datePublished`, `date` and `projectsCount`.
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`.

**Response**

| Status                       | Description                                                                                    |
|------------------------------|------------------------------------------------------------------------------------------------|
| OK (200)                     | If there are datasets for the project or `[]` if nothing is found                              |
| BAD_REQUEST (400)            | If the `query` parameter is blank or `sort` is invalid or `page` or `per_page` is not positive |
| UNAUTHORIZED (401)           | If given auth header cannot be authenticated                                                   |
| INTERNAL SERVER ERROR (500)  | Otherwise                                                                                      |

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
URL `https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=2&per_page=10`

```
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="prev"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="next"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="first"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="last"
```

Response body example:

```json
[
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "title":"rmDaYfpehl",
      "name": "mniouUnmal",
      "slug": "mniouUnmal",      
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
                "href": "https://renkulab.io/gitlab/project_slug/raw/master/data/mniouUnmal/image.png"
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
            "rel":  "details",
            "href": "http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         }
      ]
   }
]
```

#### GET /knowledge-graph/datasets/:id

Finds details of the dataset with the given `id`. The `id` can be either Dataset's _Identifier_ or a _ResourceId_ of a group of the same Datasets existing on multiple projects.

**Response**

| Status                       | Description                                                                                       |
|------------------------------|---------------------------------------------------------------------------------------------------|
| OK (200)                     | If dataset details are found                                                                      |
| UNAUTHORIZED (401)           | If given auth header cannot be authenticated                                                      |
| NOT_FOUND (404)              | If dataset is not found or user is not authorised to access project where this dataset belongs to |
| INTERNAL SERVER ERROR (500)  | Otherwise                                                                                         |

Response body example:

```json
{
  "_links": [
    {
      "rel":  "self",
      "href": "https://zemdgsw:9540/datasets/sa23KQwnti57su5gsbmTB78JVrRZpPg9Rqb6y5MNAbxz7Rsav4bhw2s8TuaFpyGTGJgNZfCubX2fABZab5to5kDuuLigYbVDj"
    },
    {
      "rel":  "initial-version",
      "href": "https://zemdgsw:9540/datasets/11111111-1111-1111-1111-111111111111"
    },
    {
      "rel":  "tags",
      "href": "https://zemdgsw:9540/knowledge-graph/projects/namespace1/project1-name/datasets/dataset-name/tags"
    }
  ],
  "identifier": "sa23KQwnti57su5gsbmTB78JVrRZpPg9Rqb6y5MNAbxz7Rsav4bhw2s8TuaFpyGTGJgNZfCubX2fABZab5to5kDuuLigYbVDj",
  "versions": {
    "initial": "11111111-1111-1111-1111-111111111111"
  },
  "tags": {                                      // optional
    "initial": {
      "name":        "1.0.1",
      "description": "some tag"                  //optional
    }
  },
  "title":       "dataset title",
  "name":        "dataset-name",
  "slug":        "dataset-name",
  "url":         "http://host/url1",             // optional property
  "sameAs":      "http://host/url2",             // optional property when no "derivedFrom" exists
  "derivedFrom": "http://host/url1",             // optional property when no "sameAs" exists
  "description": "vbnqyyjmbiBQpubavGpxlconuqj",  // optional property
  "published": {
    "datePublished": "2012-10-14",               // optional property
    "creator": [
      {
        "name":        "e wmtnxmcguz",
        "affiliation": "SDSC"                    // optional property
      },
      {
        "name":  "iilmadw vcxabmh",
        "email": "ticUnrW@cBmrdomoa"             // optional property
      }
    ]
  },
  "created":      "2012-10-15T03:02:25.639Z",    // optional property
  "dateModified": "2012-11-15T03:02:25.639Z",    // optional property,
  "hasPart": [
    {
      "atLocation": "data/dataset-name/file1"
    },
    {
      "atLocation": "data/dataset-name/file2"
    }
  ],
  "project":  {
    "_links": [
      {
        "rel":  "project-details",
        "href": "https://zemdgsw:9540/projects/namespace1/project1-name"
      }
    ],
    "dataset": {
      "identifier": "22222222-2222-2222-2222-222222222222"
    },
    "path":       "namespace1/project1-name",
    "slug":       "namespace1/project1-name",
    "name":       "project1 name",
    "visibility": "public"
  },
  "usedIn": [
    {
      "_links": [
        {
          "rel":  "project-details",
          "href": "https://zemdgsw:9540/projects/namespace1/project1-name"
        }
      ],
      "dataset": {
        "identifier": "33333333333"
      },
      "path":       "namespace1/project1-name",
      "slug":       "namespace1/project1-name",
      "name":       "project1 name",
      "visibility": "public"
    },
    {
      "_links": [
        {
          "rel":  "project-details",
          "href": "https://zemdgsw:9540/projects/namespace2/project2-name"
        }
      ],
      "dataset": {
        "identifier": "4444444444"
      },
      "path":       "namespace2/project2-name",
      "slug":       "namespace2/project2-name",
      "name":       "project2 name",
      "visibility": "public"
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
  ]
}
```

#### GET /knowledge-graph/entities

Allows finding `projects`, `datasets`, `workflows`, and `persons`.

**Filtering:**
* `query`      - to filter by matching field (e.g., title, keyword, description, etc. as specified below)
* `type`       - to filter by entity type(s); allowed values: `project`, `dataset`, `workflow`, and `person`; multiple `type` parameters allowed
* `creator`    - to filter by creator(s); the filter would require creator's name; multiple `creator` parameters allowed
* `visibility` - to filter by visibility(ies) (restricted vs. public); allowed values: `public`, `internal`, `private`; multiple `visibility` parameters allowed
* `namespace`  - to filter by namespace(s); there might be multiple values given; for nested namespaces the whole path has be used, e.g. `group/subgroup` 
* `since`      - to filter by entity's creation date to >= the given date
* `until`      - to filter by entity's creation date to <= the given date

**NOTE:** all query parameters have to be url-encoded.

When the `query` parameter is given, the match is done on the following fields:
* name/title
* namespace (for the project entity)
* creator (note: workflows has no creator for now)
* keyword
* description

**Sorting:**
* `matchingScore` - to sort by match score
* `name` - to sort by entity name - **default when no `query` parameter is given**
* `dateModified` - to sort by entity modification date
* `date` - to sort by entity creation date

**NOTE:** the sorting has to be requested by giving the `sort` query parameter with the property name and sorting order (`asc` or `desc`). The default order is ascending so `sort`=`name` means the same as `sort`=`name:asc`.

**Paging:**
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`; max value is `100`.

**Response**

| Status                       | Description                                      |
|------------------------------|--------------------------------------------------|
| OK (200)                     | If results are found; `[]` if nothing is found   |
| BAD_REQUEST (400)            | If illegal values for query parameters are given |
| UNAUTHORIZED (401)           | If given auth header cannot be authenticated     |
| INTERNAL SERVER ERROR (500)  | Otherwise                                        |

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

Assuming the total is `30` and the URL is `https://renku/knowledge-graph/entities?query=phrase&sort=name:asc&page=2&per_page=10` the following links are added to the response:

```
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="prev"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="next"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=1&per_page=10>; rel="first"
Link: <https://renku/knowledge-graph/datasets?query=phrase&sort=name:asc&page=3&per_page=10>; rel="last"
```

Response body example:

```json
[
  {
    "type":          "project",
    "matchingScore": 1.0055376,
    "name":          "name",
    "slug":          "group/subgroup/name",
    "path":          "group/subgroup/name",
    "namespace":     "group/subgroup",
    "namespaces": [
      {
        rel:       "group",
        namespace: "group"
      },
      {
        rel:       "subgroup",
        namespace: "group/subgroup"
      }
    ],
    "visibility":    "public",
    "date":          "2012-11-15T10:00:00.000Z",
    "dateCreated":   "2012-11-15T10:00:00Z",
    "dateModified":  "2012-11-16T10:00:00Z",
    "creator":       "Jan Kowalski",
    "keywords":      [ "keyword1", "keyword2" ],
    "description":   "desc",
    "_links": [
      {
        "rel":  "details",
        "href": "http://t:5511/projects/group/subgroup/name"
      }
    ],
    "images": [
      {
        "location": "image.png",
        "_links":[
          {
            "rel":  "view",
            "href": "https://renkulab.io/gitlab/group/subgroup/name/raw/master/data/mniouUnmal/image.png"
          }
        ]
      }
    ]
  },
  {
    "type":          "dataset",
    "matchingScore": 3.364836,
    "name":          "name",
    "slug":          "name",
    "visibility":    "public",
    "date":          "2012-11-15T10:00:00.000Z", // either datePublished or dateCreated
    "dateCreated":   "2012-11-15T10:00:00Z",
    "dateModified":  "2013-11-15T10:00:00Z",
    "creators":      [ "Jan Kowalski", "Zoe" ],
    "keywords":      [ "keyword1", "keyword2" ],
    "description":   "desc",
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
        "rel":  "details",
        "href": "http://t:5511/datasets/122334344"
      }
    ]
  },
  {
    "type":          "workflow",
    "matchingScore": 5.364836,
    "name":          "name",
    "visibility":    "public",
    "date":          "2012-11-15T10:00:00.000Z",
    "keywords":      [ "keyword1", "keyword2" ],
    "description":   "desc",
    "_links":        []
  },
  {
    "type":          "person",
    "matchingScore": 4.364836,
    "name":          "name",
    "_links":        []
  }
]
```

#### GET /knowledge-graph/entities/current-user/recently-viewed

Returns entities recently viewed by the user introducing himself with the token.

**Filtering:**
* `type`  - to filter by entity type(s); allowed values: `project` and/or `dataset`; multiple `type` parameters allowed
* `limit` - limit the results by this amount; must be > 0 and <= 200; defaults to 10

**NOTE:** all query parameters have to be url-encoded.

**Sorting:**
The results are sorted by the viewing time desc.

**Security**
The endpoint requires an authorization token passed in the request header as:
- `Authorization: Bearer <token>` with oauth token obtained from gitlab
- `PRIVATE-TOKEN: <token>` with user's personal access token in gitlab

**Response**

| Status                       | Description                                      |
|------------------------------|--------------------------------------------------|
| OK (200)                     | If results are found; `[]` if nothing is found   |
| BAD_REQUEST (400)            | If illegal values for query parameters are given |
| UNAUTHORIZED (401)           | If given auth header is not valid                |
| INTERNAL SERVER ERROR (500)  | Otherwise                                        |

Response body example:

```json
[
  {
    "_links": [
      {
        "rel":  "details",
        "href": "https://renku-kg-dev.dev.renku.ch/knowledge-graph/projects/group/subgroup/name"
      }
    ],
    "type":          "project",
    "description":   "Some project",
    "creator":       "Jan Kowalski",
    "matchingScore": 1,
    "name":          "name",
    "slug":          "group/subgroup/name",
    "path":          "group/subgroup/name",
    "namespace":     "group/subgroup",
    "namespaces": [
      {
        "rel":       "group",
        "namespace": "group"
      },
      {
        "rel":       "subgroup",
        "namespace": "group/subgroup"
      }
    ],
    "visibility":   "public",
    "date":         "2012-11-15T10:00:00Z",
    "dateCreated":  "2012-11-15T10:00:00Z",
    "dateModified": "2012-11-16T10:00:00Z",
    "keywords": [ 
      "key" 
    ],
    "images": [
      {
        "_links": [
          {
            "rel":  "view",
            "href": "https://gitlab.dev.renku.ch/group/subgroup/name/raw/master/image.png"
          }
        ],
        "location": "image.png"
      }
    ]
  },
  {
    "_links": [
      {
        "rel":  "details",
        "href": "https://renku-kg-dev.dev.renku.ch/knowledge-graph/datasets/sa3A8evQPNEfcTsXtDcKcEvhryXBu9agJw"
      }
    ],
    "type":          "dataset",
    "description":   "Some project",
    "matchingScore": 1,
    "name":          "name",
    "slug":          "name",
    "visibility":    "public",
    "date":          "2012-11-15T10:00:00Z",
    "dateCreated":   "2012-11-15T10:00:00Z",
    "dateModified":  "2013-11-15T10:00:00Z",
    "creators": [
      "Jan Kowalski"
    ],
    "keywords": [
      "key"
    ],
    "images": [
      {
        "_links": [
          {
            "rel":  "view",
            "href": "https://gitlab.dev.renku.ch/group/subgroup/name/raw/master/image.png"
          }
        ],
        "location": "image.png"
      }
    ]
  }
]
```

#### GET /knowledge-graph/ontology

Returns ontology used in the Knowledge Graph as HTML page or JSON-LD.

The resource supports `text/html` and `application/ld+json` `Accept` headers.

**Response**

| Status                       | Description                           |
|------------------------------|---------------------------------------|
| OK (200)                     | If generating ontology was successful |
| INTERNAL SERVER ERROR (500)  | Otherwise                             |

Response body example for `Accept: application/ld+json`:

```json
[
  {
    "@id":   "https://swissdatasciencecenter.github.io/renku-ontology",
    "@type": "http://www.w3.org/2002/07/owl#Ontology",
    "http://www.w3.org/2002/07/owl#imports": [
      {
        "@id": "http://www.w3.org/ns/oa#"
      }
    ]
  },
  {
    "@id":   "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf",
    "@type": "http://www.w3.org/2002/07/owl#Class"
  },
  {
    "@id":   "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/name",
    "@type": "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2000/01/rdf-schema#domain": [
      {
        "@id": "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf"
      }
    ],
    "http://www.w3.org/2000/01/rdf-schema#range": [
      {
        "@id": "http://www.w3.org/2001/XMLSchema#string"
      }
    ]
  },
  {
    "@id":   "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/number",
    "@type": "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2000/01/rdf-schema#domain": [
      {
        "@id": "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf"
      }
    ],
    "http://www.w3.org/2000/01/rdf-schema#range": [
      {
        "@id": "http://www.w3.org/2001/XMLSchema#number"
      }
    ]
  }
]
```

#### POST /knowledge-graph/projects

API to create a new project from the given payload in both the Triples Store and GitLab

The endpoint requires an authorization token to be passed. Supported headers are:

- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab

**Request**

```
POST /knowledge-graph/projects HTTP/1.1
Host: dev.renku.ch
Authorization: Bearer <XXX>
Content-Length: 575
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="name"

project name
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="namespaceId"

15
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="description"

project description
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="keywords[]"

key1
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="keywords[]"

key2
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="visibility"

public
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="templateRepositoryUrl"

https://github.com/SwissDataScienceCenter/renku-project-template
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="templateId"

python-minimal
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="image"; filename="image.png"
Content-Type: image/png

(data)
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

**Response**

| Status                      | Description                                            |
|-----------------------------|--------------------------------------------------------|
| CREATED (201)               | If the project is created                              |
| BAD REQUEST (400)           | If the given payload is invalid                        |
| UNAUTHORIZED (401)          | If the given auth header cannot be authenticated       |
| FORBIDDEN (403)             | If the user cannot create the project in the namespace |
| INTERNAL SERVER ERROR (500) | Otherwise                                              |

Response body example for `CREATED (201)`:

```json
{
  "message": "Project created",
  "slug":    "namespace/project-path"
}
```

#### DELETE /knowledge-graph/projects/:namespace/:name

API to remove the project with the given `namespace/name` from both knowledge-graph and GitLab

The endpoint requires an authorization token to be passed. Supported headers are:

- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab

**Response**

| Status                      | Description                                                                                                |
|-----------------------------|------------------------------------------------------------------------------------------------------------|
| ACCEPTED (202)              | If the deletion process for the project with the given `namespace/name` was successfully scheduled         |
| UNAUTHORIZED (401)          | If given auth header cannot be authenticated                                                               |
| NOT_FOUND (404)             | If there is no project with the given `namespace/name` or the user is not authorised to access the project |
| INTERNAL SERVER ERROR (500) | Otherwise                                                                                                  |

#### GET /knowledge-graph/projects/:namespace/:name

Finds details of the project with the given `namespace/name`. The endpoint requires an authorization token to be passed
in the request for non-public projects. Supported headers are:

- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab
- There's no need for a security headers for public projects

The resource supports `application/json` and `application/ld+json` `Accept` headers.

**Response**

| Status                       | Description                                                                                            |
|------------------------------|--------------------------------------------------------------------------------------------------------|
| OK (200)                     | If project with the given `namespace/name` can be found                                                |
| UNAUTHORIZED (401)           | If given auth header cannot be authenticated                                                           |
| NOT_FOUND (404)              | If there is no project with the given `namespace/name` or user is not authorised to access the project |
| INTERNAL SERVER ERROR (500)  | Otherwise                                                                                              |

Response body example for `Accept: application/json`:

```json
{
  "identifier":  123,
  "path":        "namespace/project-name", 
  "slug":        "namespace/project-name", 
  "name":        "Some project name",
  "description": "This is a longer text describing the project", // optional
  "visibility":  "public|private|internal",
  "created": {
    "dateCreated": "2001-09-05T10:48:29.457Z",
    "creator": { // optional
      "name":        "author name",
      "email":       "author@mail.org", // optional
      "affiliation": "SDSC" // optional
    }
  },
  "dateModified": "2001-10-06T10:48:29.457Z",
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
      "slug":       "namespace/parent-project",
      "name":       "Parent project name",
      "created": {
        "dateCreated": "2001-09-04T10:48:29.457Z",
        "creator": { // optional
          "name":        "parent author name", 
          "email":       "parent.author@mail.org", // optional
          "affiliation": "SDSC" // optional
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
      "rel": "self",
      "href":"http://t:5511/projects/namespace/project-name"
    },
    {
      "rel": "datasets",
      "href":"http://t:5511/projects/namespace/project-name/datasets"
    }
  ],
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
  ]
}
```

Response body example for `Accept: application/ld+json`:

```json
{
  "@id": "http://wwywiir:3577/yobqsDoboi/projects/d_llli5Zo/2nTaozqw/llosas_/__-6h3a",
  "@type": [
    "http://www.w3.org/ns/prov#Location",
    "http://schema.org/Project"
  ],
  "https://swissdatasciencecenter.github.io/renku-ontology#projectPath": {
    "@value": "d_llli5Zo/2nTaozqw/llosas_/__-6h3a"
  },
  "https://swissdatasciencecenter.github.io/renku-ontology#slug": {
    "@value": "d_llli5Zo/2nTaozqw/llosas_/__-6h3a"
  },
  "http://schema.org/description": {
    "@value": "Zs oJtagvqvIn diw cywpaj ordCPacr vnnkjj cgtzizxkb clfPe xuhrqT vK"
  },
  "http://schema.org/dateModified": {
    "@type":  "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value": "1990-07-16T21:51:12.949Z"
  },
  "http://schema.org/identifier": {
    "@value": 402288
  },
  "http://schema.org/creator": {
    "@id": "http://wwywiir:3577/yobqsDoboi/persons/69212174",
    "@type": [
      "http://www.w3.org/ns/prov#Person",
      "http://schema.org/Person"
    ],
    "http://schema.org/email": {
      "@value": "kpgj2u65iv@nezifs"
    },
    "http://schema.org/name": {
      "@value": "flawal dolBA`ql"
    }
  },
  "http://schema.org/schemaVersion": {
    "@value": "42.31.9"
  },
  "https://swissdatasciencecenter.github.io/renku-ontology#projectVisibility": {
    "@value": "internal"
  },
  "http://schema.org/name": {
    "@value": "__-6h3a"
  },
  "http://schema.org/dateCreated": {
    "@type":  "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value": "2007-01-27T17:58:52.739Z"
  },
  "http://schema.org/keywords": [
    {
      "@value": "bNalprNSye"
    },
    {
      "@value": "jffkdfe"
    },
    {
      "@value": "qscrvP"
    },
    {
      "@value": "ywnzgRbu"
    }
  ]
}
```

#### PATCH /knowledge-graph/projects/:namespace/:name

API to update project data.

Each of the properties can be either set to a new value or omitted in case there's no new value for it.
The new values should be sent as a `multipart/form-data` in case there's new image or as JSON in case no update for an image is needed.

The properties that can be updated are:
* description - possible values are:
  * `null` for removing the current description
  * any non-blank String value
* image - possible values are:
  * `null` for removing the current image
  * any image file; at the moment GitLab accepts images of size 200kB max and media type of: `image/png`, `image/jpeg`, `image/gif`, `image/bmp`, `image/tiff`, `image/vnd.microsoft.icon` 
* keywords - an array of String values; an empty array removes all the keywords
* visibility - possible values are: `public`, `internal`, `private`

In case no properties are set, no data will be changed.
The endpoint requires an authorization token to be passed. Supported headers are:

- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab

**Request**

* Multipart request (preferred) 
```
PATCH /knowledge-graph/projects/namespace/path HTTP/1.1
Host: dev.renku.ch
Authorization: Bearer <XXX>
Content-Length: 575
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="visibility"

public
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="description"

desc test 1
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="keywords[]"

key1
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="keywords[]"

key2
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="image"; filename="image.png"
Content-Type: image/png

(data)
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

* JSON request (updating image not possible) 

```json
{
  "description": "a new project description",
  "keywords":    ["keyword1", "keyword2"],
  "visibility":  "public|internal|private"
}
```

**Response**

| Status                      | Description                                                                                                |
|-----------------------------|------------------------------------------------------------------------------------------------------------|
| ACCEPTED (202)              | If the update process was successfully scheduled                                                           |
| BAD_REQUEST (400)           | If the given payload is empty or malformed                                                                 |
| UNAUTHORIZED (401)          | If given auth header cannot be authenticated                                                               |
| FORBIDDEN (403)             | If the user is not authorised to update the project                                                        |
| NOT_FOUND (404)             | If there is no project with the given `namespace/name` or the user is not authorised to access the project |
| CONFLICT (409)              | If updating the data is not possible, e.g. the user cannot push to the default branch                      |
| INTERNAL SERVER ERROR (500) | Otherwise                                                                                                  |


#### GET /knowledge-graph/projects/:namespace/:name/datasets

Finds list of datasets of the project with the given `namespace/name`.

**Sorting:**
* `name` - to sort by Dataset name - **default when no `query` parameter is given**
* `dateModified` - to sort by modification date; in case a dataset hasn't been modified yet its creation date is considered.

**NOTE:** the sorting has to be requested by giving the `sort` query parameter with the property name and sorting order (`asc` or `desc`). The default order is ascending so `sort`=`name` means the same as `sort`=`name:asc`.

Multiple `sort` parameters are allowed.

**Paging:**
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`; max value is `100`.

**Response**

| Status                      | Description                                                                                             |
|-----------------------------|---------------------------------------------------------------------------------------------------------|
| OK (200)                    | If there are datasets for the project or `[]` if nothing is found                                       |
| BAD_REQUEST (400)           | In case of invalid query parameters                                                                     |
| UNAUTHORIZED (401)          | If given auth header cannot be authenticated                                                            |
| NOT_FOUND (404)             | If there is no project with the given `namespace/name` or user is not authorised to access this project |
| INTERNAL SERVER ERROR (500) | Otherwise                                                                                               |

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

Response body example:

```json
[  
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "versions": {
        "initial": "11111111-1111-1111-1111-111111111111"
      },
      "title":         "rmDaYfpehl",
      "name":          "mniouUnmal",
      "slug":          "mniouUnmal",
      "datePublished": "1990-07-16",              // optional, if not exists dateCreated is present
      "dateCreated":   "1990-07-16T21:51:12.949Z, // optional, if not exists datePublished is present
      "dateModified":  "1990-07-16T21:51:12.949Z, // only if derivedFrom exists
      "sameAs":        "http://host/url1",
      "images": [],
      "_links": [  
        {  
           "rel":  "details",
           "href": "http://t:5511/datasets/9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c"
        },
        {
          "rel":  "initial-version",
          "href": "https://zemdgsw:9540/datasets/11111111-1111-1111-1111-111111111111"
        },
        {
          "rel":  "tags",
          "href": "https://zemdgsw:9540/knowledge-graph/projects/namespace/name/datasets/mniouUnmal/tags"
        }
      ]
   },
   {  
      "identifier": "a1b1cb86-c664-4250-a1e3-578a8a22dcbb",
      "versions": {
        "initial": "22222222-2222-2222-2222-222222222222"
      },
      "name":          "a",
      "slug":          "a",
      "datePublished": "1990-07-16",              // optional, if not exists dateCreated is present
      "dateCreated":   "1990-07-16T21:51:12.949Z, // optional, if not exists datePublished is present
      "dateModified":  "1990-07-16T21:51:12.949Z, // only if derivedFrom exists
      "derivedFrom":   "http://host/url2",   // optional property when no "sameAs" exists
      "images": [
        {
          "location": "image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://renkulab.io/gitlab/project_slug/raw/master/data/mniouUnmal/image.png"
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
           "rel":  "details",
           "href": "http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
        },
        {
          "rel":  "initial-version",
          "href": "https://zemdgsw:9540/datasets/22222222-2222-2222-2222-222222222222"
        },
        {
          "rel":  "tags",
          "href": "https://zemdgsw:9540/knowledge-graph/projects/namespace/name/datasets/a/tags"
        }
      ]
   }
]
```

#### GET /knowledge-graph/projects/:namespace/:name/datasets/:dsName/tags

Finds list of tags existing on the Dataset with the given `dsName` on the project with the given `namespace/name`.

**Paging:**
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`; max value is `100`.

**Response**

| Status                      | Description                                                                                   |
|-----------------------------|-----------------------------------------------------------------------------------------------|
| OK (200)                    | If tags are found or `[]` if nothing is found                                                 |
| BAD_REQUEST (400)           | In case of invalid query parameters                                                           |
| UNAUTHORIZED (401)          | If given auth header cannot be authenticated                                                  |
| NOT_FOUND (404)             | If there is no project with the given `namespace/name` or user is not authorised to access it |
| INTERNAL SERVER ERROR (500) | Otherwise                                                                                     |

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

Response body example:

```json
[
  {
    "name":        "name",
    "date":        "2012-11-15T10:00:00.000Z",
    "description": "desc",
    "_links": [
      {
        "rel": "dataset-details",
        "href": "http://t:5511/knowledge-graph/datasets/1232444"
      }
    ]
  }
]
```

#### GET /knowledge-graph/projects/:namespace/:name/files/:location/lineage

Fetches lineage for a given project `namespace`/`name` and file `location` (URL-encoded relative path to the file).

| Status                       | Description                                                                                             |
|------------------------------|---------------------------------------------------------------------------------------------------------|
| OK (200)                     | If there are datasets for the project or `[]` if nothing is found                                       |
| UNAUTHORIZED (401)           | If given auth header cannot be authenticated                                                            |
| NOT_FOUND (404)              | If there is no project with the given `namespace/name` or user is not authorised to access this project |
| INTERNAL SERVER ERROR (500)  | Otherwise                                                                                               |


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

#### GET /knowledge-graph/spec.json

Returns OpenApi json spec 

| Status                       | Description      |
|------------------------------|------------------|
| OK (200)                     | If spec is found |
| INTERNAL SERVER ERROR (500)  | Otherwise        |

#### GET /knowledge-graph/users/:id/projects

Returns all projects of the user with the given GitLab id.

**Filtering:**
* `state`      - to filter by project state; allowed values: 'ACTIVATED', 'NOT_ACTIVATED', 'ALL'; default value is 'ALL'

**Paging:**
* the `page` query parameter is optional and defaults to `1`.
* the `per_page` query parameter is optional and defaults to `20`; max value is `100`.

**Response**

| Status                      | Description                                         |
|-----------------------------|-----------------------------------------------------|
| OK (200)                    | If results are found; `[]` if no projects are found |
| BAD_REQUEST (400)           | If illegal values for query parameters are given    |
| UNAUTHORIZED (401)          | If given auth header cannot be authenticated        |
| INTERNAL SERVER ERROR (500) | Otherwise                                           |

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

Response body example:

```json
[
  {
    "name":        "name",
    "slug":        "group/subgroup/name",
    "path":        "group/subgroup/name",
    "visibility":  "public",
    "date":        "2012-11-15T10:00:00.000Z",
    "creator":     "Jan Kowalski",
    "description": "desc",
    "keywords": [
      "keyword1",
      "keyword2"
    ],
    "_links": [
      {
        "rel": "details",
        "href": "http://t:5511/projects/group/subgroup/name"
      }
    ]
  },
  {
    "id":          123,
    "name":        "name",
    "slug":        "group/subgroup/name",
    "path":        "group/subgroup/name",
    "visibility":  "public",
    "date":        "2012-11-15T10:00:00.000Z",
    "creator":     "Jan Kowalski",
    "description": "desc",
    "keywords": [
      "keyword1",
      "keyword2"
    ],
    "_links": [
      {
        "rel": "activation",
        "href": "http://t:5511/projects/123/webhooks",
        "method": "POST"
      }
    ]
  }
]
```

#### GET /knowledge-graph/version

Returns info about service version. It's the same as `GET /version` but it's exposed to the Internet.

**Response**

| Status                       | Description            |
|------------------------------|------------------------|
| OK (200)                     | If version is returned |
| INTERNAL SERVER ERROR (500)  | Otherwise              |

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

#### GET /metrics  (Internal use only)

Serves Prometheus metrics.

**Response**

| Status                       | Description          |
|------------------------------|----------------------|
| OK (200)                     | If metrics are found |
| INTERNAL SERVER ERROR (500)  | Otherwise            |

#### GET /ping (Internal use only)

Verifies service health.

**Response**

| Status                       | Description           |
|------------------------------|-----------------------|
| OK (200)                     | If service is healthy |
| INTERNAL SERVER ERROR (500)  | Otherwise             |

#### GET /version (Internal use only)

Returns info about service version

**Response**

| Status                       | Description            |
|------------------------------|------------------------|
| OK (200)                     | If version is returned |
| INTERNAL SERVER ERROR (500)  | Otherwise              |

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
