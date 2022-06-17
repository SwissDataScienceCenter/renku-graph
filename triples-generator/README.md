# triples-generator

This is a microservice which:

- listens to notification from the Event Log,
- clones the Git project, checks out the commit `id` in order to create RDF triples by invoking `renku log --format rdf`
  ,
- uploads the generated triples to Jena Fuseki

## API

| Method | Path             | Description                        |
|--------|------------------|------------------------------------|
| POST   | ```/events```    | To send an event for processing    |
| GET    | ```/metrics```   | Serves Prometheus metrics          |
| GET    | ```/ping```      | To check if service is healthy     |
| GET    | ```/version```   | Returns info about service version |

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
    "id": 123,
    "path": "namespace/project-name"
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
    "id": 12,
    "path": "project/path"
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
    "id": 12,
    "path": "project/path"
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
    "path": "namespace/project-name"
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
    "id": 12,
    "path": "project/path"
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

