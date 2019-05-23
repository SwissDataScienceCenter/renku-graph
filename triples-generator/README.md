# triples-generator

This is a microservice which:
- listens to notification from the Event Log,
- clones the Git project, checks out the commit `id` in order to create RDF triples by invoking `renku log --format rdf`,
- uploads the generated triples to Jena Fuseki

## API

| Method | Path                            | Description                                                                           |
|--------|---------------------------------|---------------------------------------------------------------------------------------|
| GET    | ```/ping```                     | To check if service is healthy                                                        |
| DELETE | ```/triples/projects```         | Truncates the RDF storage and re-provisions it with all the events from the Event Log |

#### GET /ping

Verifies service health.

**Response**

| Status                     | Description             |
|----------------------------|-------------------------|
| OK (200)                   | If service is healthy   |
| INTERNAL SERVER ERROR (500)| Otherwise               |

#### DELETE /triples/projects

Truncates the RDF storage and re-provisions it with all the events from the Event Log.
The endpoint is protected with Basic Auth authorization. 

**Response**

| Status                     | Description                                     |
|----------------------------|-------------------------------------------------|
| ACCEPTED (202)             | When re-provisioning got triggered successfully |
| UNAUTHORIZED (401)         | If no or invalid credentials were given         |
| INTERNAL SERVER ERROR (500)| Otherwise                                       |

### Trying out

The triples-generator is a part of multi-module sbt project thus it has to be built from the root level.

- build the docker image

```bash
docker build -f triples-generator/Dockerfile -t triples-generator .
```

- run the service

```bash
docker run --rm -e 'JENA_BASE_URL=<jena-url>' -e 'JENA_ADMIN_PASSWORD=<jena-password>' -e 'GITLAB_BASE_URL=<gitlab-url>' -e 'EVENT_LOG_POSTGRES_HOST=<postgres-host>' -e 'EVENT_LOG_POSTGRES_USER=<user>' -e 'EVENT_LOG_POSTGRES_PASSWORD=<password>' -p 9002:9002 triples-generator
```

- check if service is running

```bash
curl http://localhost:9002/ping

