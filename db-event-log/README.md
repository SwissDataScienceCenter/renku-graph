# event-log

This is a microservice which provides CRUD operations for Event Log DB.

## API

| Method  | Path                               | Description                                        |
|---------|------------------------------------|----------------------------------------------------|
|  GET    | ```/ping```                        | To check if service is healthy                     |

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
