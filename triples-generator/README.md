# triples-generator

This is a microservice which:

- listens to notification from the Event Log,
- clones the Git project, checks out the commit `id` in order to create RDF triples by invoking `renku log --format rdf`
  ,
- uploads the generated triples to Jena Fuseki

## API

| Method | Path                            | Description                             |
|--------|---------------------------------|-----------------------------------------|
| POST   | ```/events```                   | To send an event for processing         |
| GET    | ```/ping```                     | To check if service is healthy          |

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

`payload` part as a string:

```
"JSON payload as string"
```

`payload` example:

```json
{
  "payload": "json-ld payload as string",
  "schemaVersion": "8"
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

##### Response

| Status                     | Description                                                                  |
|----------------------------|------------------------------------------------------------------------------|
| ACCEPTED (202)             | When event is accepted                                                       |
| BAD_REQUEST (400)          | When request body is not a valid JSON Event                                  |
| TOO_MANY_REQUESTS (429)    | When server is busy dealing with other requests and cannot take any more now |
| INTERNAL SERVER ERROR (500)| When there are problems with event creation                                  |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

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

