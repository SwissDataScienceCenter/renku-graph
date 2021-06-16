# event-log

This is a microservice which provides CRUD operations for Event Log DB.

## API

| Method | Path                                    | Description                                                    |
|--------|-----------------------------------------|----------------------------------------------------------------|
|  GET   | ```/events/:event-id/:project-id```     | Retrieve chosen event's data                                   |
|  PATCH | ```/events```                           | Changes events' data by applying the given patch               |
|  POST  | ```/events```                           | Send an event for processing                                   |
|  PATCH | ```/events/:event-id/:project-id```     | Updates chosen event's data                                    |
|  GET   | ```/metrics```                          | Returns Prometheus metrics of the service                      |
|  GET   | ```/ping```                             | Verifies service health                                        |
|  GET   | ```/processing-status?project-id=:id``` | Finds processing status of events belonging to a project       |
|  POST  | ```/subscriptions```                    | Adds a subscription for events                                 |

#### GET /events/:event-id/:project-id`

Find event details.

**Response**

| Status                     | Description                                                   |
|----------------------------|---------------------------------------------------------------|
| OK (200)                   | If the details are found                                      |
| NOT_FOUND (404)            | If the event does not exists                                  |
| INTERNAL SERVER ERROR (500)| When there are problems                                       |

Response body example:

```json
{
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 123
  },
  "body": "JSON payload"
}
```

#### PATCH /events

Changes events' data by applying the given patch.

**NOTICE:**
Be aware that the given patch affects all the events in the Event Log.

**Request**

```json
{
  "status": "NEW"
}
```

**Response**

| Status                     | Description                                          |
|----------------------------|------------------------------------------------------|
| ACCEPTED (202)             | When the given data patch got accepted               |
| BAD_REQUEST (400)          | When request body is not valid                       |
| INTERNAL SERVER ERROR (500)| When there were problems with processing the request |

#### POST /events

Accepts an event as multipart requests.

##### Supported event categories:

- **CREATION**

  Creates an event with either the `NEW` or `SKIPPED` status.

**Multipart Request**

`event` part:

In the case of a *NEW* event

```json
{
  "categoryName": "CREATION",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 123,
    "path": "namespace/project-name"
  },
  "date": "2001-09-04T10:48:29.457Z",
  "batchDate": "2001-09-04T11:00:00.000Z",
  "body": "JSON payload",
  "status": "NEW"
}
```

In the case of a *SKIPPED* event. Note that a non-blank `message` is required.

```json
{
  "categoryName": "CREATION",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 123,
    "path": "namespace/project-name"
  },
  "date": "2001-09-04T10:48:29.457Z",
  "batchDate": "2001-09-04T11:00:00.000Z",
  "body": "JSON payload",
  "status": "SKIPPED",
  "message": "reason for skipping"
}
```

- **EVENTS_STATUS_CHANGE**

Changes the status of events. The events for which the status will be changed are defined within the event as well as the new status.

####Changing status of all project events older than the given one to `TRIPLES_GENERATED`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 12,
    "path": "namespace/project-name"
  },
  "newStatus": "TRIPLES_GENERATED"
}
```

- **ZOMBIE_CHASING**

Changes the status of a zombie event

**Multipart Request**

`event` part:

```json
{
  "categoryName": "ZOMBIE_CHASING",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 12,
    "path": "namespace/project-name"
  },
  "status": "GENERATING_TRIPLES|TRANSFORMING_TRIPLES"
}
```

- **COMMIT_SYNC_REQUEST**

Forces issuing a commit sync event for the given project

**Multipart Request**

`event` part:

```json
{
  "categoryName": "COMMIT_SYNC_REQUEST",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  }
}
```

**Response**

| Status                     | Description                                                                          |
|----------------------------|--------------------------------------------------------------------------------------|
| ACCEPTED (202)             | When event is accepted                                                               |
| BAD_REQUEST (400)          | When request body is not a valid JSON Event                                          |
| INTERNAL SERVER ERROR (500)| When there are problems with event creation                                          |

#### GET /metrics

To fetch various Prometheus metrics of the service.

**Response**

| Status                     | Description            |
|----------------------------|------------------------|
| OK (200)                   | Containing the metrics |
| INTERNAL SERVER ERROR (500)| Otherwise              |

#### PATCH /events/:event-id/:project-id

Updates event's data with the given payload.

**Request**

Currently, only status changing payloads are allowed:

- for transitioning event from status `GENERATING_TRIPLES` to `NEW`
- each status transition can optionally provide a processing_time for the status change in the ISO_8601 format
  PnDTnHnMn.nS

```json
{
  "status": "NEW",
  "processing_time (optional)": "PT2.023S"
}
```

- for transitioning event from status `TRIPLES_GENERATED` to `TRIPLES_STORE`

```json
{
  "status": "TRIPLES_STORE",
  "processing_time (optional)": "PT2.023S"
}
```

- for transitioning event from status `GENERATING_TRIPLES` to `TRIPLES_GENERATED`
- a multipart request is required with the `event` part as follow

```json
{
  "status": "TRIPLES_GENERATED",
  "processing_time (optional)": "PT2.023S"
}

```

- and a `payload` part as a __STRING encoded json__

```json
{
  "payload": "json-ld as string",
  "schemaVersion": "schema version of the triples"
}
```

- for transitioning event from status `GENERATING_TRIPLES` to `GENERATION_RECOVERABLE_FAILURE`

```json
{
  "status": "GENERATION_RECOVERABLE_FAILURE",
  "message": "error message",
  "processing_time (optional)": "PT2.023S"
}
```

- for transitioning event from status `GENERATING_TRIPLES` to `GENERATION_NON_RECOVERABLE_FAILURE`

```json
{
  "status": "GENERATION_NON_RECOVERABLE_FAILURE",
  "message": "error message",
  "processing_time (optional)": "PT2.023S"
}
```

- for transitioning event from status `TRANSFORMING_TRIPLES` to `TRANSFORMATION_RECOVERABLE_FAILURE`

```json
{
  "status": "TRANSFORMATION_RECOVERABLE_FAILURE",
  "message": "error message",
  "processing_time (optional)": "PT15M"
}
```

- for transitioning event from status `TRANSFORMING_TRIPLES` to `TRANSFORMATION_NON_RECOVERABLE_FAILURE`

```json
{
  "status": "TRANSFORMATION_NON_RECOVERABLE_FAILURE",
  "message": "error message",
  "processing_time (optional)": "P2DT3H4M"
}
```

- for transitioning event from any status to `AWAITING_DELETION`

```json
{
  "status": "AWAITING_DELETION",
  "processing_time (optional)": "PT15M"
}
```

**Response**

| Status                     | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| OK (200)                   | If status update is successful                                              |
| BAD_REQUEST (400)          | When invalid payload is given                                               |
| NOT_FOUND (404)            | When the event does not exists                                              |
| INTERNAL SERVER ERROR (500)| When some problems occurs                                                   |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description           |
|----------------------------|-----------------------|
| OK (200)                   | If service is healthy |
| INTERNAL SERVER ERROR (500)| Otherwise             |

#### GET /processing-status?project-id=:id

Finds processing status of events belonging to the project with the given `id` from the latest batch.

**Response**

| Status                     | Description                                                                        |
|----------------------------|------------------------------------------------------------------------------------|
| OK (200)                   | If there are events for the project with the given `id`                            |
| BAD_REQUEST (400)          | If the `project-id` parameter is not given or invalid                              |
| NOT_FOUND (404)            | If no events can be found for the given project or no `project-id` parameter given |
| INTERNAL SERVER ERROR (500)| When some problems occurs                                                          |

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

#### POST /subscriptions

Allow subscription for __categories__ specified bellow.

**NOTICE:**
As a good practice, the subscription should be renewed periodically in case of restart or URL change.

All events are sent as multipart requests

- **AWAITING_GENERATION**

**Request**

```json
{
  "categoryName": "AWAITING_GENERATION",
  "subscriber": {
    "url":      "http://host/path",
    "id":       "20210302140653-8641",
    "capacity": 4
  }
}
```

**Event example**

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

- **TRIPLES_GENERATED**

**Request**

```json
{
  "categoryName": "TRIPLES_GENERATED",
  "subscriber": {
    "url":      "http://host/path",
    "id":       "20210302140653-8641"
  }
}
```

**Event example**

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

```json
{
  "payload": "json-ld payload as string",
  "schemaVersion": "8"
}
```

- **MEMBER_SYNC**

**Request**

```json
{
  "categoryName": "MEMBER_SYNC",
  "subscriber": {
    "url":      "http://host/path",
    "id":       "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "MEMBER_SYNC",
  "project": {
    "path": "namespace/project-name"
  }
}
```

- **COMMIT_SYNC**

**Request**

```json
{
  "categoryName": "COMMIT_SYNC",
  "subscriber": {
    "url":      "http://host/path",
    "id":       "20210302140653-8641"
  }
}
```

**Event example**

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

- **ZOMBIE_CHASING**

**Request**

```json
{
  "categoryName": "ZOMBIE_CHASING",
  "subscriber": {
    "url":      "http://host/path",
    "id":       "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "ZOMBIE_CHASING",
  "id": "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id": 12,
    "path": "namespace/project-name"
  },
  "status": "GENERATING_TRIPLES|TRANSFORMING_TRIPLES"
}
```

**Response**

| Status                     | Description                                                                                         |
|----------------------------|-----------------------------------------------------------------------------------------------------|
| ACCEPTED (202)             | When subscription was successfully added/renewed                                                    |
| BAD_REQUEST (400)          | When there payload is invalid e.g. no `statuses` are different than `NEW` and `RECOVERABLE_FAILURE` |
| INTERNAL SERVER ERROR (500)| When there were problems with processing the request                                                |

## DB schema

Event-log uses relational database as an internal storage. The DB has the following schema:

| event                                |
|--------------------------------------|
| event_id   VARCHAR    PK    NOT NULL |
| project_id INT4       PK FK NOT NULL |
| status     VARCHAR          NOT NULL |
| created_date TIMESTAMPTZ    NOT NULL |
| execution_date TIMESTAMPTZ  NOT NULL |
| event_date TIMESTAMPTZ      NOT NULL |
| batch_date TIMESTAMPTZ      NOT NULL |
| event_body TEXT             NOT NULL |
| message TEXT                         |

| project                                   |
|-------------------------------------------|
| project_id        INT4        PK NOT NULL |
| project_path      VARCHAR        NOT NULL |
| latest_event_date TIMESTAMPTZ    NOT NULL |

| event_payload                        |
|--------------------------------------|
| event_id   VARCHAR    PK FK NOT NULL |
| project_id INT4       PK FK NOT NULL |
| payload    TEXT             NOT NULL |
| schema_version TEXT   PK    NOT NULL |

| subscription_category_sync_time       |
|---------------------------------------|
| project_id     INT4  PK FK   NOT NULL |
| category_name  TEXT  PK      NOT NULL |
| last_synced    TIMESTAMPTZ   NOT NULL |

| status_processing_time                    |
|-------------------------------------------|
| event_id        VARCHAR    PK FK NOT NULL |
| project_id      INT4       PK FK NOT NULL |
| status          VARCHAR    PK    NOT NULL |
| processing_time INTERVAL         NOT NULL |

| subscriber                           |
|--------------------------------------|
| source_url   VARCHAR     PK NOT NULL |
| delivery_url VARCHAR     PK NOT NULL |
| delivery_id  VARCHAR(19)    NOT NULL |

| event_delivery                          |
|-----------------------------------------|
| event_id     VARCHAR     PK FK NOT NULL |
| project_id   INT4        PK FK NOT NULL |
| delivery_id  VARCHAR(19)       NOT NULL |

## Trying out

The event-log is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f event-log/Dockerfile -t event-log .
```

- run the service

```bash
docker run --rm -p 9005:9005 event-log
```
