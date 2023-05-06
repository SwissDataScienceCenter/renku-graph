# webhook-service

This microservice:
- consumes Git push events,
- creates webhooks,
- validates webhooks

## API

| Method | Path                                    | Description                                           |
|--------|-----------------------------------------|-------------------------------------------------------|
| GET    | ```/metrics```                          | Serves Prometheus metrics                             |
| GET    | ```/ping```                             | To check if service is healthy                        |
| GET    | ```/projects/:id/events/status```       | Gives info about processing progress of recent events |
| POST   | ```/projects/:id/webhooks```            | Creates a webhook for a project in GitLab             |
| DELETE | ```/projects/:id/webhooks```            | Deletes a webhook for a project in Gitlab             |
| POST   | ```/projects/:id/webhooks/validation``` | Validates the project's webhook                       |
| GET    | ```/version```                          | Returns info about service version                    |
| POST   | ```/webhooks/events```                  | Consumes push events sent from GitLab                 |

#### GET /metrics

Serves Prometheus metrics.

**Response**

| Status                     | Description          |
|----------------------------|----------------------|
| OK (200)                   | If metrics are found |
| INTERNAL SERVER ERROR (500)| Otherwise            |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### GET /projects/:id/events/status

Returns information about activation and processing progress of project events.

**request format**

The endpoint accepts an authorization headers passed in the request:
- `Authorization: Bearer <token>` with oauth token obtained from gitlab
- `PRIVATE-TOKEN: <token>` with user's personal access token in gitlab
The headers are not required.

**Response**

| Status                      | Description                                            |
|-----------------------------|--------------------------------------------------------|
| OK (200)                    | When there is a hook for the project                   |
| NOT_FOUND (404)             | When project (or a valid access token) cannot be found |
| INTERNAL SERVER ERROR (500) | When there are problems with finding the status        |

Response examples:
- project not activated
```
{
  "activated": false,
  "progress": {
    "done":       0,
    "total":      5,
    "percentage": 0.00
  }
}
```
- project activated but some events are still under processing
```
{
  "activated": true,
  "progress": {
    "done":       2,
    "total":      5,
    "percentage": 40.00
  },
  "details": {
    "status":     "in-progress|success|failure",
    "message":    "generating triples",
    "stacktrace": "some stack trace" // optional
  }
}
```

#### POST /projects/:id/webhooks

creates a webhook for a project with the given `project id`.

**request format**

The endpoint requires an authorization token passed in the request header as:
- `Authorization: Bearer <token>` with oauth token obtained from gitlab
- `PRIVATE-TOKEN: <token>` with user's personal access token in gitlab

**response**

| status                      | description                                                                                     |
|-----------------------------|-------------------------------------------------------------------------------------------------|
| OK (200)                    | when hook already exists for the project                                                        |
| CREATED (201)               | when a new hook was created                                                                     |
| UNAUTHORIZED (401)          | when there is neither `private-token` nor `authorization: bearer` in the header or it's invalid |
| INTERNAL_SERVER_ERROR (500) | when there are problems with webhook creation                                                   |

#### DELETE /projects/:id/webhooks

deletes a webhook for a project with the given `project id`.

**request format**

The endpoint requires an authorization token passed in the request header as:
- `Authorization: Bearer <token>` with oauth token obtained from gitlab
- `PRIVATE-TOKEN: <token>` with user's personal access token in gitlab

**response**

| status                      | description                                                                                     |
|-----------------------------|-------------------------------------------------------------------------------------------------|
| OK (200)                    | when hook is successfully deleted                                                               |
| NOT_FOUND (404)             | when the project or its hook does not exists                                                    | 
| UNAUTHORIZED (401)          | when there is neither `private-token` nor `authorization: bearer` in the header or it's invalid |
| INTERNAL_SERVER_ERROR (500) | when there are problems with webhook creation                                                   |


#### POST /projects/:id/webhooks/validation

**Notice**
This API validates the renku webhook for the project with the given `id`.
It succeeds (OK) if the hook exists. If there's no webhook the call responds with NOT_FOUND.

**Request format**

The endpoint requires an authorization token passed in the request header as:
- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab

**Response**

| Status                     | Description                                                      |
|----------------------------|------------------------------------------------------------------|
| OK (200)                   | When the hook exists for the project                             |
| NOT_FOUND (404)            | When there's no hook for the project or the hook cannot be found |
| UNAUTHORIZED (401)         | When using does not have access to the project                   |
| INTERNAL SERVER ERROR (500)| When there are problems with validating the hook presence        |

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
  "name": "webhook-service",
  "versions": [
    {
      "version": "2.3.0"
    }
  ]
}
```

#### POST /webhooks/events

Consumes a Push Event.

**Request format** (for more details look at [GitLab documentation](https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#push-events))

A valid `X-Gitlab-Token` is required.

```
{
  "after": "df654c3b1bd105a29d658f78f6380a842feac879",
  "before": "f307326be71b17b90db5caaf47bcd44710fe119f",
  "user_id": 4,
  "user_username": "jsmith",
  "user_email": "john@example.com",
  "project": {
    "id": 15,
    "path_with_namespace":"mike/diaspora"
  }
}
```

**Response**

| Status                     | Description                                                     |
|----------------------------|-----------------------------------------------------------------|
| ACCEPTED (202)             | For valid payloads                                              |
| BAD REQUEST (400)          | When payload is invalid                                         |
| UNAUTHORIZED (401)         | When there is no `X-Gitlab-Token` in the header or it's invalid |
| INTERNAL SERVER ERROR (500)| When queue is not accepting new events                          |

## Trying out

The webhook-service is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f webhook-service/Dockerfile -t webhook-service .
```

- run the service

```bash
docker run --rm -e 'HOOK_TOKEN_SECRET=<generated with openssl rand -hex 8|base64>' -e 'GITLAB_BASE_URL=<gitlab-url>' -p 9001:9001 webhook-service
```

- play with the endpoint

```bash
curl -X POST --header "Content-Type: application/json" \
  --data '{"before": "<commit_id>", "after": "<commit_id>", "user_id": <user-id>, "user_username": "<user-name>", "user_email": "<user-email>", "project": {"id": <project-id>, "path_with_namespace": "<org-name>/<project-name>"}}' \
  http://localhost:9001/webhooks/events
```
