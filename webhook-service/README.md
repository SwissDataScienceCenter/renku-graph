# webhook-service

This is a microservice which:
- consumes Git Push Events,
- clones the Git project, checks out the `checkout_sha` commit in order to create RDF triples by invoking `renku log --format rdf`,
- uploads the generated triples to Jena Fuseki

### Trying out

- build the docker image

```bash
docker build -t webhook-service .
```

- run the service

```bash
docker run --rm -p 9000:9000 webhook-service
```

- play with the endpoint

```bash
curl -X POST --header "Content-Type: application/json" \
  --data '{"checkout_sha": "<commit_id>","repository": {"git_http_url": "<repo-url>"}, "project": {"name": "<project-name>"}}' \
  http://localhost:9000/webhook-event
```