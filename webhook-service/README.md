# webhook-service

This is a microservice which:
- consumes Git push events,
- translate them to commit events,
- uploads the events to the Event Log

## API

| Method | Path                                      | Description                                    |
|--------|-------------------------------------------|------------------------------------------------|
|  GET   | ```/ping```                               | To check if service is healthy                 |
|  POST  | ```/projects/:id/webhooks```              | Creates a webhook for a project in GitLab      |
|  POST  | ```/projects/:id/webhooks/validation```   | Validates the project's webhook                |
|  POST  | ```/webhooks/events```                    | Consumes push events sent from GitLab          |
     
#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### POST /projects/:id/webhooks

Creates a webhook for a project with the given `project id`.

**Request format**

The endpoint requires an authorization token. It can be passed in the request header as:
- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab
- `OAUTH-TOKEN: <token>` with OAuth Token obtained from GitLab (deprecated)

**Response**

| Status                     | Description                                                                           |
|----------------------------|---------------------------------------------------------------------------------------|
| OK (200)                   | When hook already exists for the project                                              |
| CREATED (201)              | When a new hook was created                                                           |
| UNAUTHORIZED (401)         | When there is neither `PRIVATE-TOKEN` nor `OAUTH-TOKEN` in the header or it's invalid |
| INTERNAL SERVER ERROR (500)| When there are problems with webhook creation                                         |

#### POST /projects/:id/webhooks/validation

**Notice**
This endpoint is under development and works just for public projects. For non-public projects it responds with INTERNAL SERVER ERROR (500).

Validates the webhook for the project with the given `project id`. It succeeds (OK) if either the project is public and there's a hook for it or it's private, there's a hook for it and a Personal Access Token (PAT). If either there's no webhook or there's no PAT in case of a private project, the call results with NOT_FOUND. In case of private projects, if there's a hook created for a project but no PAT available (or the PAT doesn't work), the hook will be removed as part of the validation process.

**Request format**

The endpoint requires an authorization token. It can be passed in the request header as:
- `Authorization: Bearer <token>` with OAuth Token obtained from GitLab
- `PRIVATE-TOKEN: <token>` with user's Personal Access Token in GitLab
- `OAUTH-TOKEN: <token>` with OAuth Token obtained from GitLab (deprecated)

**Response**

| Status                     | Description                                                                                                                                                       |
|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| OK (200)                   | When the hook exists for the project and the project is either public or there's a Personal Access Token available for it                                         |
| NOT_FOUND (404)            | When the hook either does not exists or there's no Personal Access Token available for it. If the hook exists but there's no PAT for it, the hook will be removed |
| UNAUTHORIZED (401)         | When there is neither `PRIVATE-TOKEN` nor `OAUTH-TOKEN` in the header or it's invalid                                                                             |
| INTERNAL SERVER ERROR (500)| When there are problems with validating the hook presence                                                                                                         |

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
docker run --rm -e 'HOOK_TOKEN_SECRET=<generated with openssl rand -hex 8|base64>' -e 'GITLAB_BASE_URL=<gitlab-url>' -p 9001:9000 webhook-service
```

- play with the endpoint

```bash
curl -X POST --header "Content-Type: application/json" \
  --data '{"before": "<commit_id>", "after": "<commit_id>", "user_id": <user-id>, "user_username": "<user-name>", "user_email": "<user-email>", "project": {"id": <project-id>, "path_with_namespace": "<org-name>/<project-name>"}}' \
  http://localhost:9001/webhooks/events
```