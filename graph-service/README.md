# graph-service

This is a microservice which provides API for the Graph DB.

## API

| Method  | Path                               | Description                                  |
|---------|------------------------------------|----------------------------------------------|
|  GET    | ```/ping```                        | To check if service is healthy               |

#### GET /ping

Verifies service's health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

## Trying out

The graph-service is a part of a multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f graph-service/Dockerfile -t graph-service .
```

- run the service

```bash
docker run --rm -p 9004:9004 graph-service
```
