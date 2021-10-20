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

#### Project creation flow and new commit flow

When a project is created on GitLab or when a new commit is pushed to the gitlab instance, the following flow is
triggered:

```mermaid
graph TD;
    A[GitLab] -- 1. new commit event --> B(KG services) 
    B -- 2. generation of triples --> B
    B -- 3. storing triples --> C[(TriplesStore)]

```

The same process with more details on how the KG services interact with each other

```mermaid
graph TB;
    A[GitLab] -- 1. new commit event  --> B[WebhookService]
    subgraph kg[KG Services]
    B -- 2. event for new commit  --> C(Event Services)
    C -- 3. fetch newest event --> C
    C -- 4. send event for triples generation  --> D[TriplesGenerator]
    D -- 5. triples generated --> C
    end
    C -- 6. storing triples --> E[(TriplesStore)]
```

Again the same process with more details on the Event services

```mermaid
graph TB;
    A[WebhookService] -- 1. event for new commit  --> B[EventLog]
    subgraph kg[Event Services]
    C[CommitSyncService] -- 2. fetch newest event --> B
    B -- 3. send event --> C
    C -- 4. create event for triples generation --> B
    end
    C -- 5. send event for triples generation  --> D[TriplesGenerator]
    D -- 6. triples generated --> B
    C -- 7. storing triples --> E[(TriplesStore)]
```

