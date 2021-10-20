[![pullreminders](https://pullreminders.com/badge.svg)](https://pullreminders.com?ref=badge)

# renku-graph

#### Repository structure

- `helm-chart` helm chart, published using chartpress
- `graph-commons` common classes for all the services
- `acceptance-tests` acceptance tests for the services
- `webhook-service` a microservice managing Graph Services hooks and external events
- `commit-event-service` a microservice synchronizing commit events
- `event-log` a microservice providing CRUD operations on the Event Log
- `triples-generator` a microservice translating Event Log events to RDF triples in an RDF store
- `token-repository` a microservice managing Access Tokens for projects

#### Running the tests

```bash
sbt clean test && sbt "project acceptance-tests" test
```

Depending on your global configuration of sbt you have installed, you might need to set `SBT_OPTS` to avoid OutOfMemory exception. 
If such error is raised, try setting the variable with the following:

```bash
export SBT_OPTS="-XX:+UseG1GC -XX:+CMSClassUnloadingEnabled -Xmx2G"
```

#### Releasing

The standard release process is done manually.

#### Hotfixes

In a case of hotfixes, changes to a relevant commit/tag needs to be done and pushed to a special branch with name
following the `hotfix-<major>.<minor>` pattern. Once the fix is pushed, CI will test the change with other Renku
services. Tagging has to be done manually.

### Event Flow

Overview

```mermaid
graph TD;
    A[GitLab] -- 1. new commit event --> B[KG] 
    B -- 2. generation of triples --> B
    B -- 3. storing triples --> C[TriplesStore]

```

New commit process

```mermaid
graph TD;
    A[GitLab] -- 1. new commit event  --> B[WebhookService]
    B -- 2. event for new commit  --> C[EventLog]
    C -- 3. fetch newest event --> C
    C -- 4. send event for triples generation  --> D[TriplesGenerator]
    D -- 5. triples generated --> C
    C -- 6. storing triples --> E[TriplesStore]
```
