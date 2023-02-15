# event-log

This is a microservice which provides CRUD operations for Event Log DB.

## API

| Method | Path                                        | Description                                                                |
|--------|---------------------------------------------|----------------------------------------------------------------------------|
| GET    | ```/events```                               | Returns info about events                                                  |
| GET    | ```/events/:event-id/:project-path```       | Returns info about event with the given `id` and `project-path`            |
| GET    | ```/events/:event-id/:project-id/payload``` | Returns payload associated with the event having the `id` and `project-id` |
| POST   | ```/events```                               | Sends an event for processing                                              |
| GET    | ```/metrics```                              | Returns Prometheus metrics of the service                                  |
| GET    | ```/migration-status```                     | Returns whether or not DB is currently migrating                           |
| GET    | ```/ping```                                 | Verifies service health                                                    |
| GET    | ```/status```                               | Returns info about the state of the service                                |
| POST   | ```/subscriptions```                        | Adds a subscription for events                                             |
| GET    | ```/version```                              | Returns info about service version                                         |

All endpoints (except for `/ping` and `/metrics`) will return 503 while the database is migrating.

### GET /events

Returns information about the selected events.

| Query Parameter | Mandatory | Default        | Description                                                                       |
|-----------------|-----------|----------------|-----------------------------------------------------------------------------------|
| project-id      | No        | -              | Project id                                                                        |
| project-path    | No        | -              | Url-encoded non-blank project path                                                |
| status          | No        | -              | Event status e.g. `TRIPLES_STORE`, `TRIPLES_GENERATED`                            |
| since           | No        | -              | To find events after or on this date; ISO 8601 format required `YYYY-MM-DDTHH:MM:SSZ` |
| until           | No        | -              | To find events before or on this date; ISO 8601 format required `YYYY-MM-DDTHH:MM:SSZ` |
| page            | No        | 1              | Page number                                                                       |
| per_page        | No        | 20             | Number of items per page                                                          |
| sort            | No        | eventDate:DESC | Sorting; allowed properties: `eventDate`, directions: `ASC`, `DESC`               |

NOTES:

* at least `project-id`, `project-path` or `status` query parameter has to be given.
* the returned events are sorted by the `event_date`.

**Response**

| Status                          | Description                        |
|---------------------------------|------------------------------------|
| OK (200)                        | If finding events is successful    |
| BAD REQUEST (400)               | For invalid query parameter values |
| INTERNAL SERVER ERROR (500)     | When there are problems            |
| SERVICE UNAVAILABLE ERROR (503) | When a migration is running        |

Response body example:

```json
[
  {
    "id":              "df654c3b1bd105a29d658f78f6380a842feac879",
    "project":         {
      "id":   123,
      "path": "namespace/project-name"
    },
    "status":          "NEW",
    "processingTimes": [],
    "date":            "2001-09-04T10:48:29.457Z",
    "executionDate":   "2001-09-04T10:48:29.457Z"
  },
  {
    "id":      "df654c3b1bd105a29d658f78f6380a842feac878",
    "project":         {
      "id":   1234,
      "path": "namespace2/project-name"
    },
    "status":  "TRANSFORMATION_NON_RECOVERABLE_FAILURE",
    "message": "detailed info about the cause of the failure",
    "processingTimes": [
      {
        "status":         "TRIPLES_GENERATED",
        "processingTime": "PT20.345S"
      }
    ],
    "date":          "2001-09-04T10:48:29.457Z",
    "executionDate": "2001-09-04T10:48:29.457Z"
  }
]
```

### GET /events/:event-id/:project-id`

Finds event details.

**Response**

| Status                     | Description                                                   |
|----------------------------|---------------------------------------------------------------|
| OK (200)                   | If the details are found                                      |
| NOT_FOUND (404)            | If the event does not exists                                  |
| INTERNAL SERVER ERROR (500)| When there are problems                                       |
| SERVICE UNAVAILABLE ERROR (503)| When a migration is running |

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

### GET /events/:event-id/:project-path/payload`

Finds event's payload.

**Response**

| Status                          | Description                                                                |
|---------------------------------|----------------------------------------------------------------------------|
| OK (200)                        | If payload for the given event exists                                      |
| NOT_FOUND (404)                 | If either no event or no payload for the given `event-id` and `project-id` |
| INTERNAL SERVER ERROR (500)     | When there are problems                                                    |
| SERVICE UNAVAILABLE ERROR (503) | When a migration is running                                                |

Response contains a `application/gzip` binary format. In addition to `content-type`, `content-length` and `content-disposition` headers are given. 

### POST /events

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
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   123,
    "path": "namespace/project-name"
  },
  "date":      "2001-09-04T10:48:29.457Z",
  "batchDate": "2001-09-04T11:00:00.000Z",
  "body":      "JSON payload",
  "status":    "NEW"
}
```

In the case of a *SKIPPED* event. Note that a non-blank `message` is required.

```json
{
  "categoryName": "CREATION",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   123,
    "path": "namespace/project-name"
  },
  "date":      "2001-09-04T10:48:29.457Z",
  "batchDate": "2001-09-04T11:00:00.000Z",
  "body":      "JSON payload",
  "status":    "SKIPPED",
  "message":   "reason for skipping"
}
```

- **EVENTS_STATUS_CHANGE**

Changes the status of events. The events for which the status will be changed are defined within the event as well as
the new status.

#### Changing status of the specified event from `GENERATING_TRIPLES` to `NEW`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus": "NEW"
}
```

#### Changing status of the specified event from processing statuses to failure statuses

**Allowed combinations**

| Processing status    | Failure status                         |
| -------------------- | -------------------------------------- |
| GENERATING_TRIPLES   | GENERATION_NON_RECOVERABLE_FAILURE     |
| GENERATING_TRIPLES   | GENERATION_RECOVERABLE_FAILURE         |
| TRANSFORMING_TRIPLES | TRANSFORMATION_NON_RECOVERABLE_FAILURE |
| TRANSFORMING_TRIPLES | TRANSFORMATION_RECOVERABLE_FAILURE     |

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "message":   "<failure message>",
  "newStatus": "<failure status>"
}
```

#### Changing status of the specified event from `TRANSFORMING_TRIPLES` to `TRIPLES_GENERATED`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus": "TRIPLES_GENERATED"
}
```

#### Changing status of the given project's latest event in `TRIPLES_STORE` to `TRIPLES_GENERATED` or triggering `ADD_MIN_PROJECT_INFO` event in case there's no event in `TRIPLES_STORE`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "project": {
    "path": "namespace/project-name"
  },
  "newStatus": "TRIPLES_GENERATED"
}
```

#### Changing status of the specified event to `AWAITING_DELETION`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus": "AWAITING_DELETION"
}
```

#### Changing status of all project events older than the given one to `TRIPLES_GENERATED`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus":      "TRIPLES_GENERATED",
  "processingTime": "PT2.023S"
}
```

`payload` part: binary of `application/zip` content-type

#### Changing status of all project events older than the given one to `TRIPLES_STORE`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus":      "TRIPLES_STORE",
  "processingTime": "PT2.023S"
}
```

#### Changing status of the all events of a specific project to `NEW`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus": "NEW"
}
```

#### Changing status of the all events of a specific project to `AWAITING_DELETION` from `DELETING`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "newStatus": "AWAITING_DELETION"
}
```

#### Changing status of all events to `NEW`

**Multipart Request**

`event` part:

```json
{
  "categoryName": "EVENTS_STATUS_CHANGE",
  "newStatus":    "NEW"
}
```

- **ZOMBIE_CHASING**

Changes the status of a zombie event.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "ZOMBIE_CHASING",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  },
  "status": "GENERATING_TRIPLES|TRANSFORMING_TRIPLES"
}
```

- **MIGRATION_STATUS_CHANGE**

Changes the status of undergoing TS migration.
The record for which the status will be changed is defined in the `event` part of the payload.

The corresponding record's status will be modified only when its current status is `SENT`.

Allowed values for the `newStatus` property are: `DONE`, `NON_RECOVERABLE_FAILURE` and `RECOVERABLE_FAILURE`.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "MIGRATION_STATUS_CHANGE",
  "subscriber": {
    "url":     "http://host/path",
    "id":      "20210302140653-8641",
    "version": "1.22.4-10-g34454567"
  },
  "newStatus": "DONE",
  "message":   "cause of failure" // required for failure statuses
}
```

- **CLEAN_UP_REQUEST**

Enqueues a `CLEAN_UP` event for the project with the given `id` and `path`. The `CLEAN_UP` event performs re-provisioning process for a project in the Triples Store.

**NOTICE**: When a `CLEAN_UP_REQUEST` event without project `id` is sent, there's an attempt to find the `id` based on the given `path`. If there's no project with the given `path`, no `CLEAN_UP` event will be created. 

**Multipart Request**

`event` part:

```json
{
  "categoryName": "CLEAN_UP_REQUEST",
  "project": {
    "id":   123  // optional,
    "path": "namespace/project-name"
  }
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

- **GLOBAL_COMMIT_SYNC_REQUEST**

Forces issuing a GLOBAL_COMMIT_SYNC event for the given project.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "GLOBAL_COMMIT_SYNC_REQUEST",
  "project": {
    "id":   12,
    "path": "namespace/project-name"
  }
}
```

- **PROJECT_SYNC**

Checks if the data stored for the project in EL matches the data in GitLab. If not, the process fixes the data in EL as well as sends a relevant `GLOBAL_COMMIT_SYNC` and `CLEAN_UP` events.

**Multipart Request**

`event` part:

```json
{
  "categoryName": "PROJECT_SYNC",
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
| SERVICE UNAVAILABLE ERROR (503)| When a migration is running |

### GET /metrics

To fetch Prometheus metrics of the service.

**Response**

| Status                     | Description            |
|----------------------------|------------------------|
| OK (200)                   | Containing the metrics |
| INTERNAL SERVER ERROR (500)| Otherwise              |

### GET /migration-status

Verifies service health.

**Response**

| Status                     | Description                                         |
|----------------------------|-----------------------------------------------------|
| OK (200)                   | Containing JSON with migration status (true/false)  |
| INTERNAL SERVER ERROR (500)| Otherwise                                           |

Response body example:

```json
{
  "isMigrating": false
}
```

### GET /status

Returns info about the state of the service

**Response**

| Status                          | Description                                    |
|---------------------------------|------------------------------------------------|
| OK (200)                        | If finding info about the state was successful |
| INTERNAL SERVER ERROR (500)     | When there were problems finding the data      |
| SERVICE UNAVAILABLE ERROR (503) | When a migration is running                    |

Response body example:

```json
{
  "subscriptions": [
    {
      "categoryName": "AWAITING_GENERATION",
      "subscribers": {
        "total": 5,
        "busy": 1
      }
    }
  ]
}
```

### POST /subscriptions

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
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
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
    "url": "http://host/path",
    "id":  "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "TRIPLES_GENERATED",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
    "path": "project/path"
  }
}
```

`payload` part as a string:

```json
{
  "payload":       "json-ld payload as string",
  "schemaVersion": "8"
}
```

- **MEMBER_SYNC**

**Request**

```json
{
  "categoryName": "MEMBER_SYNC",
  "subscriber": {
    "url": "http://host/path",
    "id":  "20210302140653-8641"
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
    "url": "http://host/path",
    "id":  "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "COMMIT_SYNC",
  "id":           "df654c3b1bd105a29d658f78f6380a842feac879",
  "project": {
    "id":   12,
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
    "id":   12,
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
    "id":  "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "GLOBAL_COMMIT_SYNC",
  "project": {
    "id":   12,
    "path": "project/path"
  },
  "commits": {
    "count":  100,
    "latest": "121435453edf"
  }
}
```

- **PROJECT_SYNC**

Events of the type are issued for all the projects with the frequency of 1 per 24H.

**Request**

```json
{
  "categoryName": "PROJECT_SYNC",
  "subscriber": {
    "url": "http://host/path",
    "id":  "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "PROJECT_SYNC",
  "project": {
    "id":   12,
    "path": "project/path"
  }
}
```

- **ADD_MIN_PROJECT_INFO**

Events of the type are issued for all the projects that have no events in the `TRIPLES_STORE` status and for which `ADD_MIN_PROJECT_INFO` has not been sent, yet.

**Request**

```json
{
  "categoryName": "ADD_MIN_PROJECT_INFO",
  "subscriber": {
    "url": "http://host/path",
    "id":  "20210302140653-8641"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "ADD_MIN_PROJECT_INFO",
  "project": {
    "id":   1,
    "path": "namespace/project-name"
  }
}
```

- **ZOMBIE_CHASING**

**Request**

```json
{
  "categoryName": "ZOMBIE_CHASING",
  "subscriber": {
    "url": "http://host/path",
    "id":  "20210302140653-8641"
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

- **TS_MIGRATION_REQUEST**

**Request**

```json
{
  "categoryName": "TS_MIGRATION_REQUEST",
  "subscriber": {
    "url":     "http://host/path",
    "id":      "20210302140653-8641",
    "version": "1.22.4-10-g34454567"
  }
}
```

**Event example**

`event` part:

```json
{
  "categoryName": "TS_MIGRATION_REQUEST",
  "version":      "1.22.4-10-g34454567"
}
```

**Response**

| Status                     | Description                                                                                         |
|----------------------------|-----------------------------------------------------------------------------------------------------|
| ACCEPTED (202)             | When subscription was successfully added/renewed                                                    |
| BAD_REQUEST (400)          | When there payload is invalid e.g. no `statuses` are different than `NEW` and `RECOVERABLE_FAILURE` |
| INTERNAL SERVER ERROR (500)| When there were problems with processing the request                                                |
| SERVICE UNAVAILABLE ERROR (503)| When a migration is running |

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
  "name": "event-log",
  "versions": [
    {
      "version": "2.3.0"
    }
  ]
}
```

## DB schema

Event-log uses relational database as an internal storage. The DB has the following schema:

| event                                     |
|-------------------------------------------|
| event_id       VARCHAR     PK    NOT NULL |
| project_id     INT4        PK FK NOT NULL |
| status         VARCHAR           NOT NULL |
| created_date   TIMESTAMPTZ       NOT NULL |
| execution_date TIMESTAMPTZ       NOT NULL |
| event_date     TIMESTAMPTZ       NOT NULL |
| batch_date     TIMESTAMPTZ       NOT NULL |
| event_body     TEXT              NOT NULL |
| message        VARCHAR                    |

| project                                   |
|-------------------------------------------|
| project_id        INT4        PK NOT NULL |
| project_path      VARCHAR        NOT NULL |
| latest_event_date TIMESTAMPTZ    NOT NULL |

| event_payload                     |
|-----------------------------------|
| event_id   VARCHAR PK FK NOT NULL |
| project_id INT4    PK FK NOT NULL |
| payload    BYTEA         NOT NULL |

| subscription_category_sync_time           |
|-------------------------------------------|
| project_id     INT4        PK FK NOT NULL |
| category_name  TEXT        PK    NOT NULL |
| last_synced    TIMESTAMPTZ       NOT NULL |

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

| event_delivery                            |
|-------------------------------------------|
| event_id      VARCHAR      FK UK NULL     |
| project_id    INT4         FK UK NOT NULL |
| delivery_id   VARCHAR(19)        NOT NULL |
| event_type_id VARCHAR         UK NULL     | 

| status_change_events_queue         |
|------------------------------------|
| id         SERIAL      PK NOT NULL |
| date       TIMESTAMPTZ    NOT NULL |
| event_type VARCHAR        NOT NULL |
| payload    TEXT           NOT NULL |

| clean_up_events_queue                |
|--------------------------------------|
| id           SERIAL      PK NOT NULL |
| date         TIMESTAMPTZ    NOT NULL |
| project_id   INT4           NOT NULL |
| project_path VARCHAR        NOT NULL |

| ts_migration                                |
|---------------------------------------------|
| subscriber_version  VARCHAR     PK NOT NULL |
| subscriber_url      VARCHAR     PK NOT NULL |
| status              VARCHAR        NOT NULL |
| change_date         TIMESTAMPTZ    NOT NULL |
| message             TEXT                    |

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
