\[![pullreminders](https://pullreminders.com/badge.svg)](https://pullreminders.com?ref=badge)

# renku-graph

#### Repository structure

- `helm-chart` helm charts for publishing with chartpress
- `generators` a set of common use scalacheck generators
- `tiny-types` a module containing tooling for Tiny Types
- `graph-commons` common classes for all the services
- `renku-model` defines both production and testing Renku metadata model
- `webhook-service` a microservice managing GitLab hooks and incoming external events
- `event-log` a microservice responsible for events management
- `commit-event-service` a microservice synchronizing commit events between KG and GitLab
- `triples-generator` a microservice generating, transforming and taking care of data in the Triples Store
- `token-repository` a microservice managing projects' Access Tokens
- `acceptance-tests` acceptance tests for the services

#### Running the tests

```bash
sbt clean test && sbt "project acceptance-tests" test
```

Depending on your global configuration of sbt you have installed, you might need to set `SBT_OPTS` to avoid OutOfMemory exception. 
If such error is raised, try setting the variable with the following:

```bash
export SBT_OPTS="-Xmx2G -Xss5M"
```

#### Development

###### Coding molds

Renk Graph was built with code readability and maintainability as a value. We believe that high coding standards can:
* reduce time needed for adding new features;
* reduce number of bugs;
* lower cognitive load for developers;
* make the work with the code more fun ;)

Hence, we are trying to find and then follow good patterns in naming, code organization on a method, class, package and module level. The following list has a work-in-progress style is it supposed to be in constant improvement.
* `camelCase` notation is used everywhere in Scala code;
* the lower scope of a variable, method, class, package, the shorter, less descriptive name can be;
* readability has always a higher value than succinctness;
* `class`es should rather have noun names;
* `class`es names should rather not be comprised of more than three words;
* `def`s should be verbs;
* `def`s should rather be very short (let's keep it an exception for a `def` having more than 10 lines);
* `class`es should rather be short;
* `class`es and `def`s should be having single purpose;
* nesting `if`s or any other structures should not exceed three levels; preferable one level of nesting should be used;
* great attention should be paid to the scopes on all levels and as a rule it's preferable that the visibility is kept as low as possible;
* variables should rather be defined the closest possible the usage; we don't follow the old way of top-variables-definition-block as it's hard to see the aim of a variable;
* ADTs (Abstract Data Types) are used everywhere to make compiler preventing accidental mixing up different domain values; only in very exceptional cases that can be lifted;
* technical names are rather discouraged in naming as domain and business language; an obvious exception here is more abstract utility defs/classes/objects etc.;
* packages' names if possible should also carry business info rather than technicalities;
* it's preferred to use simple names of classes/objects/traits and introduce namespacing through packages over putting context info into compilation unit names; e.g. it's fine to repeat _'Endpoint'_ as a class name of all the endpoints as each endpoint should live in its own package;
* `implicit`s and Context Bound should/may be used extensively but wisely;
* `show` String Interpolator should be the first choice over the `s` and `toString`; 
* obviously the rules above can be always lifted if favour of readability;
* code should be formatted with the rules defined in the `.scalafmt.conf` file;
* once code readability issue is found, it should be either fix straight away or an issue should be created.
* jsonLD should be used instead of jsonLd or jsonld

#### Releasing

The standard release process is done manually. There are multiple
repositories taking part in the process. The
[renku](https://github.com/SwissDataScienceCenter/renku) project
contains helm charts for deploying to kubernetes and the acceptance
tests. The
[terraform-renku](https://github.com/SwissDataScienceCenter/terraform-renku)
project contains deployment descriptions for all environments.

**renku-graph project:**
- create a branch off the `origin/release` branch, name it
  `prep-<version>`
- merge development into it (`git merge origin/development`)
- make a PR for this branch to be merged into origin/release
  ([example](https://github.com/SwissDataScienceCenter/renku-graph/pull/1095))
- list some points in the description for what has been changed
- merge the PR and create a release on github
  - release description: list the points again but organized in
    Features, Bugfixes etc
    ([example](https://github.com/SwissDataScienceCenter/renku-graph/releases/tag/2.19.0))
  - the release notes are public, so don't list refactorings.
  - this will trigger actions that:
    - Runs acceptance tests
    - creates a PR on the
      [renku](https://github.com/SwissDataScienceCenter/renku) project
      to change the version in the `requirements.yml`
      ([example](https://github.com/SwissDataScienceCenter/renku/pull/2704))

**renku project:**
- amend the PR created by the bot with the following:
  - create a new entry in `CHANGELOG.rst` 
  - change the version in
  [`helm-chart/renku/CHart.yaml`](https://github.com/SwissDataScienceCenter/renku/blob/master/helm-chart/renku/Chart.yaml)
  - if there already exists a release PR from another project, apply
    the changes directly to the existing PR (including changelog
    entries) and close the obsolete PR from `renku-graph` afterwards.
- merge the PR when satisfiled
- create a new release in the renku project.
  ([example](https://github.com/SwissDataScienceCenter/renku/releases/tag/0.17.2))
- then it will publish new renku helm chart and create all relevant
  PRs in the
  [terraform-renku](https://github.com/SwissDataScienceCenter/terraform-renku)
  repo which will contain changes to the deployment
  - Examples:
    - https://github.com/SwissDataScienceCenter/terraform-renku/pull/858
    - https://github.com/SwissDataScienceCenter/terraform-renku/pull/857

**terraform-renku:**
- these PRs are merged by the YAT team which will release to prod

**Cleanup (renku-graph):**
- merge the `release` branch back into `development`
- push directly to the `development` branch


#### Hotfixes

In a case of hotfixes, changes to a relevant commit/tag needs to be done and pushed to a special branch with name
following the `hotfix-<major>.<minor>` pattern. Once the fix is pushed, CI will test the change with other Renku
services. Tagging has to be done manually.

### Event Flow

This section describes the flow of events starting from a commit on GitLab until the data is stored in the triples
store. The solid lines represent an event being sent and the dotted lines represent non-event-like data (request or
response).

#### Opting in a Project into KG

The assumption is that the Project already exists in GitLab.

```mermaid
sequenceDiagram
    participant UI
    participant WebhookService
    participant GitLab
    participant TokenRepository
    participant EventLog
    
    UI ->> WebhookService: POST /projects/:id/webhooks
    activate WebhookService
    WebhookService ->> GitLab: Create a KG webhook     
    WebhookService ->> TokenRepository: PUT /projects/:id/tokens
    WebhookService ->> EventLog: sends COMMIT_SYNC_REQUEST
    WebhookService ->> UI: 200/201
    deactivate WebhookService
```

#### A new commit flow

The assumption is that there's Renku Webhook for a Project created and GitLab sends a Push Event for the project.

```mermaid
sequenceDiagram
    participant GitLab
    participant WebhookService
    participant EventLog
    
    GitLab ->> WebhookService: POST /webhooks/events
    WebhookService ->> EventLog: sends COMMIT_SYNC_REQUEST 
```

#### Commit Sync flow

This flow traverses the commit history for a Project in GitLab until it finds a commit EventLog knows about.

```mermaid
sequenceDiagram
    participant EventLog
    participant CommitEventService
    participant CommitEventService
    participant GitLab

    EventLog ->> CommitEventService: sends COMMIT_SYNC 
    activate CommitEventService
    CommitEventService ->> TokenRepository: fetches access token
    CommitEventService ->> GitLab: finds commits which are not in EventLog
    CommitEventService ->> EventLog: sends CREATION for all commits that are not in EventLog
    CommitEventService ->> EventLog: sends EVENTS_STATUS_CHANGE (to: AWAITING_DELETION) for all commits that are in EventLog but not in GitLab
    CommitEventService ->> EventLog: sends GLOBAL_COMMIT_SYNC_REQUEST if at least one AWAITING_DELETION or CREATION was found 
    deactivate CommitEventService
```

#### Global Commit Sync flow:

This flow traverses the whole commit history of a Project and find out:

1. if there are commits on GitLab that need to be created on the `Eventlog`
2. if there are commits that are not on GitLab that should be removed from the `EventLog`

This process is scheduled to be triggered at a minimum rate of once per week per project and at a maximum rate of once
per hour per project. The commit history traversal only begins when the number of commits on GitLab and on
the `EventLog` does not match and the most recent commit on GitLab is different from the most recent commit on
the `EventLog`.

```mermaid
sequenceDiagram
    participant EventLog
    participant CommmitEventService
    participant CommitEventService
    participant GitLab

    EventLog ->> CommmitEventService: GLOBAL_COMMIT_SYNC
    activate CommitEventService
    CommitEventService ->> GitLab: finds out the last commit ID and the total number of commits
    loop if the last commit ID or the total number of commits do not match with EventLog state find all the differences
    CommitEventService ->> TokenRepository: fetches access token
    CommitEventService ->> GitLab: get all commits
    CommitEventService ->> EventLog: get all commits
    CommitEventService ->> EventLog: sends CREATION for all commits that are not in EventLog
    CommitEventService ->> EventLog: sends EVENTS_STATUS_CHANGE (to: AWAITING_DELETION) for all commits that are in EventLog but not in GitLab
    end
    deactivate CommitEventService
```

#### Project provisioning flow

The assumption is the latest Commit Event for a Project in EventLog is in status 'NEW'

```mermaid
sequenceDiagram
    participant EventLog
    participant TriplesGenerator
    participant TokenRepository
    participant GitLab
    participant CLI
    participant TriplesStore

    EventLog ->> TriplesGenerator: sends AWAITING_GENERATION
    activate TriplesGenerator
    TriplesGenerator ->> TokenRepository: fetches access token
    TriplesGenerator ->> GitLab: clones the project
    TriplesGenerator ->> CLI: renku migrate
    TriplesGenerator ->> CLI: renku graph export
    TriplesGenerator ->> EventLog: sends EVENTS_STATUS_CHANGE (to: TRIPLES_GENERATED) with the graph as payload
    deactivate TriplesGenerator
    
    EventLog ->> TriplesGenerator: sends TRIPLES_GENERATED
    activate TriplesGenerator
    TriplesGenerator ->> TokenRepository: fetches access token
    TriplesGenerator ->> GitLab: calls several APIs in the Transformation process
    TriplesGenerator ->> TriplesStore: execute update queries and uploads project metadata
    TriplesGenerator ->> EventLog: sends EVENTS_STATUS_CHANGE (to: TRIPLES_STORE)
    deactivate TriplesGenerator    
```

#### Commit deletion flow

The assumption is that there was a `git reset hard` or `git rebase` done on the Project

```mermaid
sequenceDiagram
    participant EventLog
    participant TriplesGenerator
    participant TokenRepository
    participant GitLab
    participant TriplesStore

    EventLog ->> TriplesGenerator: sends CLEAN_UP_REQUEST
    activate TriplesGenerator
    TriplesGenerator ->> TokenRepository: fetches access token
    TriplesGenerator ->> TriplesStore: remove the data of a Project
    TriplesGenerator ->> EventLog: sends EVENTS_STATUS_CHANGE (to: NEW) of all the event of a single Project
    deactivate TriplesGenerator
    
    activate EventLog
    EventLog ->> EventLog: remove all events in status AWAITING_DELETION and DELETING
    loop if there are no events left for the Project
    EventLog ->> EventLog: remove the Project 
    EventLog ->> TokenRepository: remove the Project token
    EventLog ->> GitLab: remove the Project WebHook
    end
    EventLog ->> EventLog: change status of all Project events to NEW
    EventLog ->> TriplesGenerator: sends AWAITING_GENERATION
    deactivate EventLog    
```

#### The ADD_MIN_PROJECT_INFO event

The assumption is that there's no Commit Event in TRIPLES_STORE status for a Project

```mermaid
sequenceDiagram
    participant EventLog
    participant TriplesGenerator
    participant TokenRepository
    participant GitLab
    participant TriplesStore

    EventLog ->> TriplesGenerator: sends ADD_MIN_PROJECT_INFO
    activate TriplesGenerator
    TriplesGenerator ->> TokenRepository: fetches access token
    TriplesGenerator ->> GitLab: calls several APIs in the Transformation process
    TriplesGenerator ->> TriplesStore: execute update queries and uploads project metadata
    TriplesGenerator ->> EventLog: sends EVENTS_STATUS_CHANGE (to: TRIPLES_STORE)
    deactivate TriplesGenerator    
```

#### The MEMBER_SYNC event

This event is sent periodically to sync authorization data between GitLab and Triples Store

```mermaid
sequenceDiagram
    participant EventLog
    participant TriplesGenerator
    participant TokenRepository
    participant GitLab
    participant TriplesStore

    EventLog ->> TriplesGenerator: sends MEMBER_SYNC
    activate TriplesGenerator
    TriplesGenerator ->> TokenRepository: fetches access token
    TriplesGenerator ->> GitLab: calls the Project users and Project members APIs
    TriplesGenerator ->> TriplesStore: project members
    deactivate TriplesGenerator    
```

#### The PROJECT_SYNC event

This event is sent periodically to sync Project data between GitLab, EventLog and Triples Store

```mermaid
sequenceDiagram
    participant EventLog
    participant CommitEventService
    participant TriplesGenerator
    participant TokenRepository
    participant GitLab
    participant TriplesStore

    EventLog ->> EventLog: sends PROJECT_SYNC
    activate EventLog
      EventLog ->> TokenRepository: fetches access token
      EventLog ->> GitLab: calls the Project Details
      loop if there project path is NOT the same in EventLog and GitLab
        EventLog ->> CommitEventService: sends COMMIT_SYNC for the new path
        EventLog ->> TriplesGenerator: sends CLEAN_UP_REQUEST for the old path
      end
      EventLog ->> TriplesGenerator: sends SYNC_REPO_METADATA
      activate TriplesGenerator
        TriplesGenerator ->> GitLab: fetches project metadata
        TriplesGenerator ->> TriplesStore: fetches project metadata
        TriplesGenerator ->> TriplesStore: sends update queries if values needs updating (not for visibility changes)
        TriplesGenerator ->> EventLog: sends RedoProjectTransformation (only when visibility changes)
      deactivate TriplesGenerator
    deactivate EventLog    
```

#### The ZOMBIE_CHASING event

This event category detects Commit Events that got stale. 

```mermaid
sequenceDiagram
    participant EventLog
    participant TriplesGenerator

    loop finds out events that are marked as under processing but the process was interrupted
    activate EventLog    
    EventLog ->> TriplesGenerator: verifies if instance with given URL and identifier exist
    EventLog ->> EventLog: sends ZOMBIE_CHASING
    deactivate EventLog   
    EventLog ->> EventLog: sends EVENTS_STATUS_CHANGE (to: NEW | TRIPLES_GENERATED)
    end 
```

##### The removal (re-provisioning) of a project

Once an event is marked as AwaitingDeletion it is automatically picked up by our process and a CleanUp event is created. 
This event triggers the removal of the project in the Triple Store. The clean up of a project can be either the removal of the projects with all its events and entities (if the project was removed from GitLab) or the re-provisioning of the project (if there are events which are not AwaitingDeletion).

###### Removing Project Triples

The removal of project triples happens in two steps:
 - Updating links
 - Removing all entities

Updating links happens in order to not create island in our graph. An example would be with a hierarchy of forked projects:

`project1 <-- project2 <-- project3`

If we wanted to remove project2 we would have to re-link project3 to project1.

`project1 <-- project3`

The update of the links would also be applied to the Dataset entities which could be imported from other Datasets(similar to a fork for a project).

After the re-linking, the project and all its dependant entities can be removed. These entities will be removed only if they are not used in another project.
