# token-repository

This is a microservice which provides CRUD operations for `projectId` -> `access token` associations.

## API

| Method | Path                               | Description                                |
|--------|------------------------------------|--------------------------------------------|
|  GET   | ```/ping```                        | To check if service is healthy             |
|  GET   | ```/projects/:id/tokens```         | Fetches an access token for the project id |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### GET /projects/:id/tokens

Fetches an access token for a project with the given id.

**Request format**

The endpoint requires a `TOKEN` in the request headers.

**Response**

| Status                     | Description                                                                           |
|----------------------------|---------------------------------------------------------------------------------------|
| OK (200)                   | When an access token can be found for the project                                     |
| NOT_FOUND (404)            | When an access token cannot be found for the project                                  |
| UNAUTHORIZED (401)         | When there's no `TOKEN` in the header or it's invalid                                 |
| INTERNAL SERVER ERROR (500)| When there were problems with finding the token                                       |

Response for a case when the token is a Personal Access Token
```
{ "personalAccessToken": "<some-token-value>" }
```

Response for a case when the token is an OAuth Access Token
```
{ "oauthAccessToken": "<some-token-value>" }
```

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
