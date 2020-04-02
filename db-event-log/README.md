# event-log

This is a microservice which provides CRUD operations for Event Log DB.

## API

| Method | Path                               | Description                                        |
|--------|------------------------------------|----------------------------------------------------|
|  GET   | ```/ping```                        | Verifies service health                            |
|  POST  | ```/events```                      | Creates an event with a `NEW` status               |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description           |
|----------------------------|-----------------------|
| OK (200)                   | If service is healthy |
| INTERNAL SERVER ERROR (500)| Otherwise             |

#### POST /events

Creates an event with the `NEW` status.

**Request**

```json
{
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   123,
    "path": "namespace/project-name"
  },
  "date":    "2001-09-04T10:48:29.457Z",
  "batchDate": "2001-09-04T11:00:00.000Z",
  "body":      "JSON payload"
}
```

Event Body example:

```json
{
  "id":            "df654c3b1bd105a29d658f78f6380a842feac879",
  "message":       "some text",
  "committedDate": "2001-09-04T10:48:29.457Z",
  "author": {
    "name":  "author name",
    "email": "author@mail.com"    // optional
  },
  "committer": {
    "name":  "committer name",
    "email": "committer@mail.com" // optional
  },
  "parents": [
    "f307326be71b17b90db5caaf47bcd44710fe119f"
  ],
  "project": {
    "id":    123,
    "path": "namespace/project-name"
  }
}
```

**Response**

| Status                     | Description                                                                                     |
|----------------------------|-------------------------------------------------------------------------------------------------|
| OK (200)                   | When event with the given `id` for the given project already exists in the Event Log            |
| CREATED (201)              | When a new event was created in the Event Log                                                   |
| INTERNAL SERVER ERROR (500)| When there are problems with event creation                                                     |

## Trying out

The event-log is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f db-event-log/Dockerfile -t event-log .
```

- run the service

```bash
docker run --rm -p 9005:9005 db-event-log
```
