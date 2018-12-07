# webhook-service

This is a microservice which:
- consumes Git push events,
- translate them to commit events,
- uploads the events to the Event Log

## API

| Method | Path                               | Description                                        |
|--------|------------------------------------|----------------------------------------------------|
|  POST  | ```/webhook-event```               | Consumes a push event sent from GitLab.           |

#### POST /webhook-event

Consumes a Push Event.

**Request format** (for more details look at [GitLab documentation](https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#push-events))

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

|Status               |Description                             |
|---------------------|----------------------------------------|
|ACCEPTED             | For valid payloads.                    |
|BAD REQUEST          | When payload is invalid.               |
|INTERNAL SERVER ERROR| When queue is not accepting new events.|

## Trying out

- build the docker image

```bash
docker build -t webhook-service .
```

- run the service

```bash
docker run --rm -e 'PLAY_APPLICATION_SECRET=tLm_qFcq]L2>s>s`xd6iu6R[BHfK]>hgd/=HOx][][Yldf@kQIvrh:;C6P08?Fmh' -e 'GITLAB_BASE_URL=<gitlab-url>' -p 9001:9001 webhook-service
```

- play with the endpoint

```bash
curl -X POST --header "Content-Type: application/json" \
  --data '{"before": "<commit_id>", "after": "<commit_id>", "user_id": <user-id>, "user_username": "<user-name>", "user_email": "<user-email>", "project": { "id": <project-id>, "path_with_namespace":"<org-name>/<project-name>" }"repository": {"git_http_url": "<repo-url>"}, "project": {"name": "<project-name>"}}' \
  http://localhost:9000/webhook-event
```