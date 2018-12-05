# triples-generator

This is a microservice which:
- listens to notification from the Event Log,
- clones the Git project, checks out the commit `id` in order to create RDF triples by invoking `renku log --format rdf`,
- uploads the generated triples to Jena Fuseki

### Trying out

- build the docker image

```bash
docker build -t triples-generator .
```

- run the service

```bash
docker run --rm -e 'PLAY_APPLICATION_SECRET=tLm_qFcq]L2>s>s`xd6iu6R[BHfK]>hgd/=HOx][][Yldf@kQIvrh:;C6P08?Fmh' -e 'JENA_BASE_URL=<jena-url>' -e 'GITLAB_BASE_URL=<gitlab-url>' -p 9002:9002 triples-generator
```
