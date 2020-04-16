# event-log

This is a microservice which provides CRUD operations for Event Log DB.

## API

| Method | Path                                     | Description                                                    |
|--------|------------------------------------------|----------------------------------------------------------------|
|  POST  | ```/events```                            | Creates an event with a `NEW` status                           |
|  GET   | ```/events/latest```                     | Finds events for all the projects with the latest `event_date` |
|  PATCH | ```/events/:id/projects/:id/status```    | Updates event status                                           |
|  GET   | ```/events/projects/:id/status```        | Finds processing status of events belonging to a project       |
|  POST  | ```/events/status/NEW```                 | Changes status of all the events to `NEW`                      |
|  POST  | ```/events/subscriptions?status=READY``` | Adds a subscription for the events                             |
|  GET   | ```/ping```                              | Verifies service health                                        |

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
| BAD_REQUEST (400)          | When request body is not a valid JSON Event                                                     |
| INTERNAL SERVER ERROR (500)| When there are problems with event creation                                                     |

#### GET /events/latest

Finds events for all the projects with the latest `event_date`.

**Response**

| Status                     | Description                                                  |
|----------------------------|--------------------------------------------------------------|
| OK (200)                   | If there are events found for the projects or `[]` otherwise |
| INTERNAL SERVER ERROR (500)| When there are problems                                      |

Response body example:

```json
{
  "id":     "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   123,
    "path": "namespace/project-name"
  },
  "body":   "JSON payload"
}
```

#### PATCH /events/:id/projects/:id/status

Updates status of the event with given `id` and `project_id`.

**Response**

| Status                     | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| OK (200)                   | If status update is successful                                              |
| NOT_FOUND (404)            | When there's no event with the given `id` and `project_id`                  |
| CONFLICT (409)             | When current status of the event does not allow to become the requested one |
| INTERNAL SERVER ERROR (500)| When some problems occurs                                                   |

**Request**

There are different payloads required for different status types transitions:
- `NEW`
```json
{
  "status": "NEW"
}
```
**Notice** `CONFLICT (409)` returned when current event status is different than `PROCESSING`.
- `TRIPLES_STORE`
```json
{
  "status": "TRIPLES_STORE"
}
```
**Notice** `CONFLICT (409)` returned when current event status is different than `PROCESSING`.
- `RECOVERABLE_FAILURE`
```json
{
  "status": "RECOVERABLE_FAILURE",
  "message": "error message"
}
```
**Notice** `CONFLICT (409)` returned when current event status is different than `PROCESSING`.
- `NON_RECOVERABLE_FAILURE`
```json
{
  "status": "NON_RECOVERABLE_FAILURE",
  "message": "error message"
}
```
**Notice** `CONFLICT (409)` returned when current event status is different than `PROCESSING`.

#### GET /events/projects/:id/status

Finds processing status of events belonging to a project from the latest batch.

**Response**

| Status                     | Description                                                  |
|----------------------------|---------------------------------------------------------|
| OK (200)                   | If there are events for the project with the given `id` |
| NOT_FOUND (404)            | If no events can be found for the given project         |
| INTERNAL SERVER ERROR (500)| When some problems occurs                               |

Response body examples:
- all events from the latest batch are processed
```json
{
  "done": 20,
  "total": 20,
  "progress": 100.00
}
```
- some events from the latest batch are being processed
```json
{
  "done": 10,
  "total": 20,
  "progress": 50.00
}
```

#### POST /events/status/NEW

Triggers changing status of all the events in the Log to `NEW`.

**NOTICE:** 
By calling this endpoint all the events will get re-processed.

**Response**

| Status                     | Description                                          |
|----------------------------|------------------------------------------------------|
| ACCEPTED (202)             | When setting the status got accepted                 |
| INTERNAL SERVER ERROR (500)| When there were problems with processing the request |

#### POST /events/subscriptions?status=READY

Adds a subscription to the events with certain statuses. Once a service gets successfully subscribed by receiving an OK,
event-log service will start distributing events with the given `status` to the URL presented in the request body. 

**NOTICE:** 
As a good practice, the subscription should be renewed periodically in case of restart or URL change.

**Request**

```json
{
  "url": "http://host/path"
}
```

**Response**

| Status                     | Description                                                         |
|----------------------------|---------------------------------------------------------------------|
| ACCEPTED (202)             | When subscription was successfully added/renewed                    |
| BAD_REQUEST (400)          | When there's no `status=READY` parameter or request body is invalid |
| INTERNAL SERVER ERROR (500)| When there were problems with processing the request                |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description           |
|----------------------------|-----------------------|
| OK (200)                   | If service is healthy |
| INTERNAL SERVER ERROR (500)| Otherwise             |

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
