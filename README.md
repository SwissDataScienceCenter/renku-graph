[![pullreminders](https://pullreminders.com/badge.svg)](https://pullreminders.com?ref=badge)

# renku-graph

#### Repository structure

- `helm-chart` helm chart, published using chartpress
- `acceptance-tests` acceptance tests for the services
- `db-event-log` Postgres based Event Log module
- `graph-commons` common classes for all the services
- `token-repository` a microservice managing Access Tokens for projects
- `triples-generator` a microservice translating Event Log events to RDF triples in an RDF store
- `webhook-service` a microservice managing Graph Services hooks and external events

#### Running the tests

```bash
sbt clean test && sbt "project acceptance-tests" test
```

#### Releasing

```bash
sbt release
```
