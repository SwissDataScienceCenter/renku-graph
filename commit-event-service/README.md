# commit-event-service

This is a microservice which:

- listens to notification from the Event Log,
- generates events for commits the Event Log missed

## API

| Method | Path                            | Description                             |
|--------|---------------------------------|-----------------------------------------|
| POST   | ```/events```                   | To consume an event for processing      |
| GET    | ```/ping```                     | To check if service is healthy          |

#### POST /events

Accepts an event as multipart requests.

##### Supported event categories:

- **COMMIT_SYNC**

**Multipart Request**

`event` part:

```json
{
  "categoryName": "COMMIT_SYNC",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 12,
    "path": "project/path"
  },
  "lastSynced": "2001-09-04T11:00:00.000Z"
}
```

or

```json
{
  "categoryName": "COMMIT_SYNC",
  "project": {
    "id": 12,
    "path": "project/path"
  }
}
```

- **GLOBAL_COMMIT_SYNC**

**Request**

```json
{
  "categoryName": "GLOBAL_COMMIT_SYNC",
  "subscriber": {
    "url": "http://host/path",
    "id": "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "GLOBAL_COMMIT_SYNC",
  "project": {
    "id": 12,
    "path": "project/path"
  },
  "commits": [
    "commitId1",
    "commitId2"
  ]
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

The commit-event-service is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f commit-event-service/Dockerfile -t commit-event-service .
```

- run the service

```bash
docker run --rm -e 'GITLAB_BASE_URL=<gitLab-url>' -e 'EVENT_LOG_BASE_URL=<eventLog-url>' -p 9006:9006 commit-event-service
```

- check if service is running

```bash
curl http://localhost:9006/ping
```
