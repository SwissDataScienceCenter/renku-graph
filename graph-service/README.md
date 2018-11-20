# renku-graph-service

How to try the graphql endpoint:

1. Build the docker image

```bash
docker build -t renku-graph-service .
```

2. Run the service

```bash
docker run --rm -e "PLAY_APPLICATION_SECRET=nosecret" -p 9000:9000 renku-graph-service
```

3. Play with graphql

The service will answer to:
```bash
curl -v "http://localhost:9000/ping"
curl -v "http://localhost:9000/schema"
curl -v "http://localhost:9000/graphql"
```

To interactively play with the graphql endpoint, go to [graphqlbin](https://legacy.graphqlbin.com/new),
and use `http://localhost:9000/graphql`.
