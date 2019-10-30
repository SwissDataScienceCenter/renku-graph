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

Depending on your global configuration of sbt you have installed, you might need to set `SBT_OPTS` to avoid OutOfMemory exception. 
If such error is raised, try setting the variable with the following:

```bash
export SBT_OPTS="-XX:+UseG1GC -XX:+CMSClassUnloadingEnabled -Xmx1G"
```

#### Releasing

The standard release process is done by Travis and it happens automatically on a Pull Request merge to master.

#### Hotfixes

In a case of hoftixes, changes to a relevant commit/tag needs to be done and pushed to a special branch with name following the `hotfix-<major>.<minor>` pattern. Once the fix is pushed, Travis will automatically release a new hotfix version according to the versions set in `version.sbt` and `helm-chart/renku-graph/Chart.yaml`. Both mentioned files needs to be updated manually so 
- version in the `version.sbt` has to follow `0.10.1-SNAPSHOT`
- version in the `helm-chart/renku-graph/Chart.yaml` has to follow `0.10.1-706fb4d`

Additionally, a change to the version bump scheme needs to be done in the `build.sbt` and the default `releaseVersionBump := sbtrelease.Version.Bump.Minor` has to become `releaseVersionBump := sbtrelease.Version.Bump.Bugfix`.
