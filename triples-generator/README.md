# triples-generator

This microservice deals with all Triples Store administrative and provisioning events.

## API

| Method | Path                  | Description                          |
|--------|-----------------------|--------------------------------------|
| POST   | ```/events```         | To send an event for processing      |
| GET    | ```/metrics```        | Serves Prometheus metrics            |
| GET    | ```/ping```           | To check if service is healthy       |
| PATCH  | ```/projects/:slug``` | API to update project data in the TS |
| GET    | ```/version```        | Returns info about service version   |

#### POST /events

Accepts an event as multipart requests.

##### Supported event categories:

- **AWAITING_GENERATION**

**Multipart Request**

`event` part:

```json
{
  "categoryName": "AWAITING_GENERATION",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 12
  }
}
```

`payload` part as a string:

```
"JSON payload as string"
```

`payload` example:

```json
{
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "parents": [
    "f307326be71b17b90db5caaf47bcd44710fe119f"
  ],
  "project": {
    "id":   123,
    "slug": "namespace/project-name"
  }
}
```

- **TRIPLES_GENERATED**

**Multipart Request**

`event` part:

```json
{
  "categoryName": "TRIPLES_GENERATED",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "slug": "project/path"
  }
}
```

`payload` part: binary of `application/zip` content-type

- **ADD_MIN_PROJECT_INFO**

Upon arrival, triples-generator will
* gather minimal project info based on data found in GitLab;
* send the info to the TS

**Multipart Request**

`event` part:

```json
{
  "categoryName": "ADD_MIN_PROJECT_INFO",
  "project": {
    "id":   12,
    "slug": "project/path"
  }
}
```

- **SYNC_REPO_METADATA**

Upon arrival, triples-generator will
* fetch info about `visibility`, `images`, `name`, `description` and `keywords` from GitLab;
* fetch the payload of the latest project event
* extract the same info from the payload as fetched from GitLab;
* calculate diffs and update the TS

**Multipart Request**

`event` part:

```json
{
  "categoryName": "SYNC_REPO_METADATA",
  "project": {
    "slug": "project/path"
  }
}
```

- **MEMBER_SYNC**

**Multipart Request**

`event` part:

```json
{
  "categoryName": "MEMBER_SYNC",
  "project": {
    "slug": "namespace/project-name"
  }
}
```

- **CLEAN_UP**

**Multipart Request**

`event` part:

```json
{
  "categoryName": "CLEAN_UP",
  "project": {
    "id":   12,
    "slug": "project/path"
  }
}
```

- **TS_MIGRATION_REQUEST**

Once an event of the type is sent, triples-generator checks if it's running the requested version.
If yes, it kicks-off execution of all configured Triples Store migrations.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "TS_MIGRATION_REQUEST",
  "subscriber": {
    "version": "1.22.4-10-g34454567"
  }
}
```

- **PROJECT_ACTIVATED**

Once an event of the type is sent, triples-generator inserts project viewing info into the TS in case there's no viewing data for the project set, yet.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "PROJECT_ACTIVATED",
  "project": {
    "slug": "project/path"
  },
  "date": "2001-09-04T10:48:29.457Z"
}
```
- 
- **PROJECT_VIEWED**

Once an event of the type is sent, triples-generator upserts project viewing info in the TS with the data from the payload.
The upsert happens only if the date from the event is newer than the date from the TS.
In case there's no project with the given slug in the TS, the event is discarded.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "PROJECT_VIEWED",
  "project": {
    "slug": "project/path"
  },
  "date": "2001-09-04T10:48:29.457Z",
  "user": {
    "id":    123,      // optional
    "email": "a@b.com" // optional
  }
}
```

**NOTE:** It should be either `user.id` or `user.email` given. In case both are given, the `id` will be used.

- **DATASET_VIEWED**

Once an event of the type is sent, triples-generator locates a project where Dataset with the given identifier is defined
and upserts project viewing info in the TS for the project.
In case there's no dataset with the given identifier or no project where this dataset exists, the event is discarded.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "DATASET_VIEWED",
  "dataset": {
    "identifier": "123456"
  },
  "date": "2001-09-04T10:48:29.457Z",
  "user": {
    "id": 123 // optional
  }
}
```

- **PROJECT_VIEWING_DELETION**

Once an event of the type is sent, triples-generator removes project viewing info from the TS.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "PROJECT_VIEWING_DELETION",
  "project": {
    "slug": "project/path"
  }
}
```

##### Response

| Status                     | Description                                                                  |
|----------------------------|------------------------------------------------------------------------------|
| ACCEPTED (202)             | When event is accepted                                                       |
| BAD_REQUEST (400)          | When request body is not a valid JSON Event                                  |
| TOO_MANY_REQUESTS (429)    | When server is busy dealing with other requests and cannot take any more now |
| INTERNAL SERVER ERROR (500)| When there are problems with event creation                                  |

#### GET /metrics

Serves Prometheus metrics.

**Response**

| Status                     | Description          |
|----------------------------|----------------------|
| OK (200)                   | If metrics are found |
| INTERNAL SERVER ERROR (500)| Otherwise            |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### PATCH /knowledge-graph/projects/:slug

API to update project data in the Triples Store.

Each of the properties can be either set to a new value or omitted in case there's no new value.

The properties that can be updated are:
* description - possible values are: 
  * `null` for removing the current description
  * any non-blank String value 
* images - an array of either relative or absolute links to the images; an empty array removes all the images 
* keywords - an array of String values; an empty array removes all the keywords
* visibility - possible values are: `public`, `internal`, `private`

In case no properties are set, no data will be changed in the TS. 

**Request**

* case when there are updates for all properties

```json
{
  "description": "a new project description",
  "images":      ["image.png", "http://image.com/image.png"],
  "keywords":    ["some keyword"],
  "visibility":  "public|internal|private"
}
```

* case when there's an update only for the `description` that removes it

```json
{
  "description": null
}
```

* case with the `description` set to a blank String means the same as `"description": null`

```json
{
  "description": ""
}
```

**Response**

| Status                      | Description                           |
|-----------------------------|---------------------------------------|
| OK (200)                    | When project is updated successfully  |
| BAD_REQUEST (400)           | When the given payload is malformed   |
| NOT_FOUND (404)             | When project does not exist in the TS |
| INTERNAL SERVER ERROR (500) | In case of failures                   |

#### GET /version

Returns info about service version

**Response**

| Status                     | Description            |
|----------------------------|------------------------|
| OK (200)                   | If version is returned |
| INTERNAL SERVER ERROR (500)| Otherwise              |

Response body example:

```json
{
  "name": "triples-generator",
  "versions": [
    {
      "version": "2.3.0"
    }
  ]
}
```

### Trying out

The triples-generator is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f triples-generator/Dockerfile -t triples-generator .
```

- run the service

```bash
docker run --rm -e 'JENA_BASE_URL=<jena-url>' -e 'JENA_ADMIN_PASSWORD=<jena-password>' -e 'GITLAB_BASE_URL=<gitlab-url>' -e 'EVENT_LOG_POSTGRES_HOST=<postgres-host>' -e 'EVENT_LOG_POSTGRES_USER=<user>' -e 'EVENT_LOG_POSTGRES_PASSWORD=<password>' -p 9002:9002 triples-generator
```

- check if service is running

```bash
curl http://localhost:9002/ping

