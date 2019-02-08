# token-repository

This is a microservice which provides CRUD operations for `projectId` -> `access token` associations.

## API

| Method | Path                               | Description                               |
|--------|------------------------------------|-------------------------------------------|
|  GET   | ```/ping```                        | To check if service is healthy            |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

## Trying out

The token-repository is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f token-repository/Dockerfile -t token-repository .
```

- run the service

```bash
docker run --rm -e 'DB_URL=<url>' -p 9003:9000 token-repository
```
