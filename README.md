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

- create a release branch,
- set the upstream on the branch with ```git push --set-upstream origin <release-branch-name>```,
- create a release with ```sbt release```; enter relevant versions for the release and the snapshot; do not choose to push the new commits,
- create a new branch for the new snapshot,
- checkout the release branch and hard reset to the pre-new-snapshot commit with ```git reset --hard HEAD~1```,
- push the release branch with ```git push --follow-tags origin <release-branch-name>```,
- checkout the new snapshot branch and push it,
- create PRs for both branches; remember to merge the release PR first.
