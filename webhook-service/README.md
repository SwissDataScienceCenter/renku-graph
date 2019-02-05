# webhook-service

This is a microservice which:
- consumes Git push events,
- translate them to commit events,
- uploads the events to the Event Log

## API

| Method | Path                               | Description                           |
|--------|------------------------------------|---------------------------------------|
|  GET   | ```/ping```                        | To check if service is healthy        |
|  POST  | ```/webhook-event```               | Consumes push events sent from GitLab |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### POST /projects/:id/hooks

Creates a webhook for a project with the given `project id`.

**Request format**

The endpoint requires an authorization token. It has to be
- either `PRIVATE-TOKEN` with user's personal access token in GitLab
- or `OAUTH-TOKEN` with oauth token obtained from GitLab

**Response**

| Status                     | Description                                                                           |
|----------------------------|---------------------------------------------------------------------------------------|
| CREATED (201)              | For valid payloads                                                                    |
| UNAUTHORIZED (401)         | When there is neither `PRIVATE-TOKEN` nor `OAUTH-TOKEN` in the header or it's invalid |
| CONFLICT (409)             | When the hook already exists                                                          |
| INTERNAL SERVER ERROR (500)| When there were problems with webhook creation                                        |

#### POST /webhook-event

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
docker run --rm -e 'PLAY_APPLICATION_SECRET=tLm_qFcq]L2>s>s`xd6iu6R[BHfK]>hgd/=HOx][][Yldf@kQIvrh:;C6P08?Fmh' -e 'GITLAB_BASE_URL=<gitlab-url>' -p 9001:9000 webhook-service
```

- play with the endpoint

```bash
curl -X POST --header "Content-Type: application/json" \
  --data '{"before": "<commit_id>", "after": "<commit_id>", "user_id": <user-id>, "user_username": "<user-name>", "user_email": "<user-email>", "project": {"id": <project-id>, "path_with_namespace": "<org-name>/<project-name>"}}' \
  http://localhost:9001/webhook-event
```