# token-repository

This is a microservice which provides CRUD operations for `projectId` -> `access token` associations.

## API

| Method  | Path                               | Description                                  |
|---------|------------------------------------|----------------------------------------------|
|  GET    | ```/ping```                        | To check if service is healthy               |
|  GET    | ```/projects/:id/tokens```         | Fetches an access token for the project id   |
|  PUT    | ```/projects/:id/tokens```         | Associates the given token and project id    |
|  DELETE | ```/projects/:id/tokens```         | Deletes the token and project id association |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### GET /projects/:id/tokens

Fetches an access token for a project id.

**Response**

| Status                     | Description                                                                           |
|----------------------------|---------------------------------------------------------------------------------------|
| OK (200)                   | When an access token can be found for the project                                     |
| NOT_FOUND (404)            | When an access token cannot be found for the project                                  |
| INTERNAL SERVER ERROR (500)| When there were problems with finding the token                                       |

Response for a case when the token is a Personal Access Token
```
{ "personalAccessToken": "<some-token-value>" }
```

Response for a case when the token is an OAuth Access Token
```
{ "oauthAccessToken": "<some-token-value>" }
```

#### PUT /projects/:id/tokens

Associates the given token and project id. It succeeds regardless of the association is newly created, it existed before or it got updated. 

**Request format**

The endpoint requires a token to sent in the request JSON body. Allowed payloads are:

* Personal Access Tokens
```
{ "personalAccessToken": "<some-token-value>" }
```

* OAuth Access Tokens
```
{ "oauthAccessToken": "<some-token-value>" }
```

**Response**

| Status                     | Description                                                            |
|----------------------------|------------------------------------------------------------------------|
| NO_CONTENT (204)           | When the association was successful                                    |
| BAD_REQUEST (400)          | When the request body is invalid                                       |
| INTERNAL SERVER ERROR (500)| When there were problems with associating the token and the project id |

#### DELETE /projects/:id/tokens

Deletes the association of a token and a project id. The deletion is successful regardless the association existed or not.

**Response**

| Status                     | Description                                            |
|----------------------------|--------------------------------------------------------|
| NO_CONTENT (204)           | When deletion was successful                           |
| INTERNAL SERVER ERROR (500)| When there were problems with deleting the association |

## Trying out

The token-repository is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f token-repository/Dockerfile -t token-repository .
```

- run the service

```bash
docker run --rm -p 9003:9003 token-repository
```
