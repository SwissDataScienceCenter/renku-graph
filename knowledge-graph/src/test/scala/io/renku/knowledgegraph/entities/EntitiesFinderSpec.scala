/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.knowledgegraph.entities

import Endpoint._
import Criteria.Filters._
import Criteria.{Filters, Sorting}
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.{accessTokens, sortingDirections}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities._
import io.renku.http.rest.SortBy
import io.renku.http.rest.paging.model._
import io.renku.http.rest.paging.{PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant, ZoneOffset}
import scala.util.Random

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities - no filters" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe
        allEntitiesFrom(project).sortBy(_.name.value)
    }
  }

  "findEntities - with query filter" should {

    "return entities which name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val person = personEntities
        .map(replacePersonName(to = sentenceContaining(query).generateAs(persons.Name)))
        .generateOne

      val loneProject = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(sentenceContaining(query).generateAs(projects.Name)))
        .modify(creatorLens.modify(_ => person.some))
        .generateOne

      val dsAndProject @ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            replaceDSName(to = sentenceContaining(query).generateAs(datasets.Name))
          )
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            planEntities().modify(replacePlanName(to = sentenceContaining(query).generateAs(plans.Name)))
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans.toList

      val notMatchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(loneProject, dsProject, planProject, notMatchingProject)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        loneProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        (plan -> planProject).to[model.Entity.Workflow],
        person.to[model.Entity.Person]
      ).sortBy(_.name.value)
    }

    "return entities which keywords matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(
          replaceProjectKeywords(to =
            Set(sentenceContaining(query).generateAs(projects.Keyword), projectKeywords.generateOne)
          )
        )
        .generateOne

      val dsAndProject @ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            replaceDSKeywords(to =
              List(sentenceContaining(query).generateAs(datasets.Keyword), datasetKeywords.generateOne)
            )
          )
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            planEntities().modify(
              replacePlanKeywords(to =
                List(sentenceContaining(query).generateAs(plans.Keyword), planKeywords.generateOne)
              )
            )
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans.toList

      loadToStore(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        (plan -> planProject).to[model.Entity.Workflow]
      ).sortBy(_.name.value)
    }

    "return entities which description matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(
          replaceProjectDesc(to = sentenceContaining(query).generateAs(projects.Description).some)
        )
        .generateOne

      val dsAndProject @ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSDesc(to = sentenceContaining(query).generateAs(datasets.Description).some))
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            planEntities().modify(replacePlanDesc(to = sentenceContaining(query).generateAs(plans.Description).some))
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans.toList

      loadToStore(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        (plan -> planProject).to[model.Entity.Workflow]
      ).sortBy(_.name.value)
    }

    "return project entities which namespace matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(path = projects.Path(s"$query/${relativePaths(maxSegments = 2).generateOne}")))
        .generateOne

      loadToStore(soleProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project]
      ).sortBy(_.name.value)
    }

    "return entities which creator name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val projectCreator = personEntities.generateOne.copy(name = sentenceContaining(query).generateAs(persons.Name))
      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => projectCreator.some))
        .generateOne

      val dsCreator = personEntities.generateOne.copy(name = sentenceContaining(query).generateAs(persons.Name))
      val dsAndProject @ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(creatorsLens.modify(_ => Set(dsCreator, personEntities.generateOne)))
          )
        )
        .generateOne

      loadToStore(soleProject, dsProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        projectCreator.to[model.Entity.Person],
        dsCreator.to[model.Entity.Person]
      ).sortBy(_.name.value)
    }
  }

  "findEntities - with entity type filter" should {

    "return only projects when 'project' type given" in new TestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Project.some)))
        .unsafeRunSync()
        .results shouldBe List(project.to[model.Entity.Project]).sortBy(_.name.value)
    }

    "return only datasets when 'dataset' type given" in new TestCase {
      val dsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Dataset.some)))
        .unsafeRunSync()
        .results shouldBe List(dsAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
    }

    "return only workflows when 'workflow' type given" in new TestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Workflow.some)))
        .unsafeRunSync()
        .results shouldBe project.plans.map(_ -> project).map(_.to[model.Entity.Workflow]).toList.sortBy(_.name.value)
    }

    "return only datasets when 'person' type given" in new TestCase {
      val person = personEntities.generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => person.some))
        .modify(removeMembers())
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Person.some)))
        .unsafeRunSync()
        .results shouldBe List(person.to[model.Entity.Person]).sortBy(_.name.value)
    }
  }

  "findEntities - with creator filter" should {

    "return entities with matching creator only" in new TestCase {
      val creator = personEntities.generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => creator.some))
        .generateOne

      val dsAndProject @ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ => Set(personEntities.generateOne, creator))
            )
          )
        )
        .generateOne

      loadToStore(soleProject, dsProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeCreator = creator.name.some)))
        .unsafeRunSync()
        .results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        creator.to[model.Entity.Person]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on creator" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeCreator = personNames.generateSome)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with visibility filter" should {

    "return entities with matching visibility only" in new TestCase {

      val publicProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val nonPublicProject = renkuProjectEntities(visibilityNonPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(publicProject, nonPublicProject)

      finder
        .findEntities(Criteria(Filters(maybeVisibility = projects.Visibility.Public.some)))
        .unsafeRunSync()
        .results shouldBe allEntitiesFrom(publicProject)
        .addAllPersonsFrom(nonPublicProject)
        .sortBy(_.name.value)
    }

    "return no entities when no match on visibility" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeVisibility = visibilityNonPublic.generateSome)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with date filter" should {

    "return entities with matching date only" in new TestCase {
      val date = Filters.Date(dateParams.generateOne.value.minusDays(1))
      val dateAsInstant = Instant
        .from(date.value.atStartOfDay(ZoneOffset.UTC))
        .plusSeconds(positiveInts(60 * 60 * 24 - 1).generateOne.value)

      val matchingDS ::~ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projects.DateCreated(dateAsInstant)))
        .withActivities(
          activityEntities(planEntities().modify(replacePlanDateCreated(to = plans.DateCreated(dateAsInstant)))),
          activityEntities(
            planEntities().modify(
              replacePlanDateCreated(to =
                timestampsNotInTheFuture(butYoungerThan = dateAsInstant.plus(Duration ofDays 1))
                  .generateAs(plans.DateCreated)
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify(
              provenanceLens[Dataset.Provenance.Internal].modify(_.copy(date = datasets.DateCreated(dateAsInstant)))
            )
        )
        .addDataset(
          datasetEntities(provenanceInternal).modify(
            provenanceLens[Dataset.Provenance.Internal].modify(
              _.copy(date =
                timestampsNotInTheFuture(butYoungerThan = dateAsInstant.plus(Duration ofDays 1))
                  .generateAs(datasets.DateCreated)
              )
            )
          )
        )
        .generateOne
      val plan :: _ = project.plans.toList

      loadToStore(project, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeDate = date.some)))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        (matchingDS -> project).to[model.Entity.Dataset],
        (plan       -> project).to[model.Entity.Workflow]
      ).sortBy(_.name.value)
    }

    "return entities with matching date only - case of DatePublished" in new TestCase {
      val date = dateParams.generateOne

      val matchingDS ::~ _ ::~ dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(_.copy(date = datasets.DatePublished(date.value)))
            )
        )
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(dsProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeDate = date.some)))
        .unsafeRunSync()
        .results shouldBe List(
        (matchingDS -> dsProject).to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on date" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeDate = dateParams.generateSome)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with sorting" should {

    "be sorting by Name if requested" in new TestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      val direction = sortingDirections.generateOne

      finder
        .findEntities(Criteria(sorting = Sorting.By(Sorting.ByName, direction)))
        .unsafeRunSync()
        .results shouldBe allEntitiesFrom(project).sortBy(_.name.value).use(direction)
    }

    "be sorting by Date if requested" in new TestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()), activityEntities(planEntities()))
        .withDatasets(datasetEntities(provenanceImportedExternal))
        .withDatasets(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(project)

      val direction = sortingDirections.generateOne

      val results = finder
        .findEntities(Criteria(sorting = Sorting.By(Sorting.ByDate, direction)))
        .unsafeRunSync()
        .results

      val expectedPersons = List.empty[model.Entity].addAllPersonsFrom(project).toSet

      val expectedSorted = List(project.to[model.Entity])
        .addAllDatasetsFrom(project)
        .addAllPlansFrom(project)
        .sortBy(_.dateAsInstant)
        .use(direction)

      val (sorted, persons) = if (direction == SortBy.Direction.Asc) {
        val (persons, sorted) = results splitAt expectedPersons.size
        sorted -> persons
      } else results splitAt expectedSorted.size

      sorted        shouldBe expectedSorted
      persons.toSet shouldBe expectedPersons
    }

    "be sorting by Matching Score if requested" in new TestCase {

      val query = nonBlankStrings(minLength = 3).generateOne

      val ds ::~ project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(to = projects.Name(query.value)))
        .withActivities(activityEntities(planEntities().modify(replacePlanName(to = plans.Name(s"smth $query")))))
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSName(to = sentenceContaining(query).generateAs(datasets.Name)))
        )
        .generateOne
      val plan :: Nil = project.plans.toList

      loadToStore(project)

      val direction = sortingDirections.generateOne

      finder
        .findEntities(
          Criteria(Filters(maybeQuery = Filters.Query(query.value).some),
                   Sorting.By(Sorting.ByMatchingScore, direction)
          )
        )
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        project.to[model.Entity.Project], // should have the highest score as its name is the query
        (plan -> project).to[model.Entity.Workflow], // should have higher score as its name is the query with a prefix
        (ds   -> project).to[model.Entity.Dataset]
      ).use(direction).reverse
    }
  }

  "findEntities - with paging" should {

    val project = renkuProjectEntities(visibilityPublic)
      .modify(removeCreator())
      .modify(removeMembers())
      .withDatasets(
        datasetEntities(provenanceNonModified).modify(provenanceLens.modify(removeCreators())),
        datasetEntities(provenanceNonModified).modify(provenanceLens.modify(removeCreators()))
      )
      .generateOne

    "return the only page" in new TestCase {

      loadToStore(project)

      val paging = PagingRequest(Page(1), PerPage(3))

      val results = finder.findEntities(Criteria(paging = paging)).unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results                  shouldBe allEntitiesFrom(project).sortBy(_.name.value)
    }

    "return the requested page with info if there are more" in new TestCase {

      loadToStore(project)

      val paging = PagingRequest(Page(Random.nextInt(3) + 1), PerPage(1))

      val results = finder
        .findEntities(Criteria(paging = paging))
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results shouldBe allEntitiesFrom(project).sortBy(_.name.value).get(paging.page.value - 1).toList
    }

    "return no results if non-existing page requested" in new TestCase {

      loadToStore(project)

      val paging = PagingRequest(Page(4), PerPage(1))

      val results = finder
        .findEntities(Criteria(paging = paging))
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results                  shouldBe Nil
    }
  }

  "findEntities - in case of a shared datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val originalDSAndProject @ originalDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      loadToStore(originalDSProject, importedDSProject)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = Filters.EntityType.Dataset.some)))
        .unsafeRunSync()
        .results shouldBe List(
        originalDSAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "de-duplicate datasets having equal sameAs - case of an Exported DS" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val importedDSAndProject1 @ importedDS1 ::~ project1WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject2 @ _ ::~ project2WithImportedDS = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val importedDSAndProject3 @ _ ::~ projectWithDSImportedFromProject = renkuProjectEntities(visibilityPublic)
        .importDataset(importedDS1)
        .generateOne

      loadToStore(project1WithImportedDS, project2WithImportedDS, projectWithDSImportedFromProject)

      val results = finder
        .findEntities(Criteria(Filters(maybeEntityType = Filters.EntityType.Dataset.some)))
        .unsafeRunSync()
        .results

      results should {
        be(List(importedDSAndProject1.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject2.to[model.Entity.Dataset])) or
          be(List(importedDSAndProject3.to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a modified datasets" should {

    "de-duplicate datasets having equal sameAs - case of an Internal DS" in new TestCase {
      val originalDS ::~ modifiedDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      loadToStore(originalDSProject, importedDSProject)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Dataset.some),
                   sorting = Sorting.By(Sorting.ByDate, SortBy.Direction.Asc)
          )
        )
        .unsafeRunSync()
        .results shouldBe List(
        (modifiedDS -> originalDSProject).to[model.Entity.Dataset],
        importedDSAndProject.to[model.Entity.Dataset]
      ).sortBy(_.dateAsInstant)
    }
  }

  "findEntities - in case of a invalidated datasets" should {

    "not return invalidated DS" in new TestCase {
      val originalDS ::~ _ ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .generateOne

      val importedDSAndProject @ _ ::~ importedDSProject = renkuProjectEntities(visibilityPublic)
        .importDataset(originalDS)
        .generateOne

      loadToStore(originalDSProject, importedDSProject)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = Filters.EntityType.Dataset.some)))
        .unsafeRunSync()
        .results shouldBe List(importedDSAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
    }
  }

  "findEntities - in case of a forks with datasets" should {

    "de-duplicate datasets when on forked projects" in new TestCase {
      val _ ::~ modifiedDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val original ::~ fork = originalDSProject.forkOnce()

      loadToStore(original, fork)

      val results = finder
        .findEntities(Criteria(Filters(maybeEntityType = Filters.EntityType.Dataset.some)))
        .unsafeRunSync()
        .results

      results should {
        be(List((modifiedDS -> original).to[model.Entity.Dataset])) or
          be(List((modifiedDS -> fork).to[model.Entity.Dataset]))
      }
    }
  }

  "findEntities - in case of a dataset on forks with different visibility" should {

    "favour dataset on public projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val dsAndPublicProject @ _ ::~ publicProject = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original ::~ fork = {
        val original ::~ fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }

      loadToStore(original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Dataset.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndPublicProject.to[model.Entity.Dataset])
    }

    "favour dataset on internal projects over projects if exist" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndInternalProject @ _ ::~ internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val original ::~ fork = {
        val original ::~ fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }

      loadToStore(original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Dataset.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndInternalProject.to[model.Entity.Dataset])
    }

    "select dataset on private project if there's no project with broader visibility" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndProject @ _ ::~ privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .importDataset(externalDS)
        .generateOne

      loadToStore(privateProject)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Dataset.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List(dsAndProject.to[model.Entity.Dataset])
    }
  }

  "findEntities - in case of a forks with workflows" should {

    "de-duplicate workflows when on forked projects" in new TestCase {
      val original ::~ fork = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .forkOnce()
      val plan :: Nil = fork.plans.toList

      loadToStore(original, fork)

      val results = finder
        .findEntities(Criteria(Filters(maybeEntityType = Filters.EntityType.Workflow.some)))
        .unsafeRunSync()
        .results

      results should {
        be(List((plan -> original).to[model.Entity.Workflow])) or
          be(List((plan -> fork).to[model.Entity.Workflow]))
      }
    }
  }

  "findEntities - in case of a workflows on forks with different visibility" should {

    "favour workflows on public projects if exist" in new TestCase {

      val publicProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original ::~ fork = {
        val original ::~ fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }
      val plan :: Nil = publicProject.plans.toList

      loadToStore(original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Workflow.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> publicProject).to[model.Entity.Workflow])
    }

    "favour workflows on internal projects over private projects if exist" in new TestCase {

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(planEntities()))
        .generateOne

      val original ::~ fork = {
        val original ::~ fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }
      val plan :: Nil = internalProject.plans.toList

      loadToStore(original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Workflow.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> internalProject).to[model.Entity.Workflow])
    }

    "select workflows on private projects if there are no projects with broader visibility" in new TestCase {

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(planEntities()))
        .generateOne
      val plan :: Nil = privateProject.plans.toList

      loadToStore(privateProject)

      finder
        .findEntities(
          Criteria(filters = Filters(maybeEntityType = Filters.EntityType.Workflow.some),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> privateProject).to[model.Entity.Workflow])
    }
  }

  "findEntities - security" should {

    val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne

    val privateProject = renkuProjectEntities(fixed(Visibility.Private))
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(planEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val internalProject = renkuProjectEntities(fixed(Visibility.Internal))
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(planEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val publicProject = renkuProjectEntities(visibilityPublic)
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(planEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    "return public entities only if no auth user is given" in new TestCase {

      loadToStore(privateProject, internalProject, publicProject)

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllPersonsFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name.value)
    }

    "return public and internal entities only if auth user is given" in new TestCase {

      loadToStore(privateProject, internalProject, publicProject)

      finder
        .findEntities(
          Criteria(maybeUser = personEntities(personGitLabIds.toGeneratorOfSomes).generateSome.map(_.toAuthUser))
        )
        .unsafeRunSync()
        .results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllEntitiesFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name.value)
    }

    "return any visibility entities if the given auth user has access to them" in new TestCase {

      loadToStore(privateProject, internalProject, publicProject)

      finder.findEntities(Criteria(maybeUser = member.toAuthUser.some)).unsafeRunSync().results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllEntitiesFrom(internalProject)
        .addAllEntitiesFrom(privateProject)
        .sortBy(_.name.value)
    }
  }

  "findEntities - persons" should {

    "return a single person if there are multiple with the same name" in new TestCase {
      // person merging is a temporary solution until we start to return persons ids

      val query = nonBlankStrings(minLength = 3).generateOne

      val sharedName      = sentenceContaining(query).generateAs(persons.Name)
      val person1SameName = personEntities.map(_.copy(name = sharedName)).generateOne
      val person2SameName = personEntities.map(_.copy(name = sharedName)).generateOne
      val person3 = personEntities
        .map(_.copy(name = sentenceContaining(query).generateAs(persons.Name)))
        .generateOne

      loadToStore(person1SameName, person2SameName, person3, personEntities.generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore should {
        be(List(person1SameName, person3).map(_.to[model.Entity.Person]).sortBy(_.name.value)) or
          be(List(person2SameName, person3).map(_.to[model.Entity.Person]).sortBy(_.name.value))
      }
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val finder =
      new EntitiesFinderImpl[IO](rdfStoreConfig, new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]()))
  }

  private implicit class PagingResponseOps(response: PagingResponse[model.Entity]) {
    lazy val resultsWithSkippedMatchingScore: List[model.Entity] = response.results.skipMatchingScore
  }

  private implicit class ResultsOps(results: List[model.Entity]) {
    lazy val skipMatchingScore: List[model.Entity] = results.map {
      case proj:     model.Entity.Project  => proj.copy(matchingScore = model.MatchingScore.min)
      case ds:       model.Entity.Dataset  => ds.copy(matchingScore = model.MatchingScore.min)
      case workflow: model.Entity.Workflow => workflow.copy(matchingScore = model.MatchingScore.min)
      case person:   model.Entity.Person   => person.copy(matchingScore = model.MatchingScore.min)
    }

    lazy val use: SortBy.Direction => List[model.Entity] = {
      case SortBy.Direction.Asc  => results
      case SortBy.Direction.Desc => results.reverse
    }
  }

  private implicit class EntityOps(entity: model.Entity) {
    lazy val dateAsInstant: Instant = entity match {
      case proj:     model.Entity.Project  => proj.date.value
      case ds:       model.Entity.Dataset  => ds.date.instant
      case workflow: model.Entity.Workflow => workflow.date.value
      case person:   model.Entity.Person   => person.date.value
    }
  }

  private def allEntitiesFrom(project: RenkuProject): List[model.Entity] =
    List.empty[model.Entity].addAllEntitiesFrom(project)

  private implicit class EntitiesOps(entities: List[model.Entity]) {

    def addAllEntitiesFrom(project: RenkuProject): List[model.Entity] = {
      List(project.to[model.Entity.Project])
        .addAllDatasetsFrom(project)
        .addAllPlansFrom(project)
        .addAllPersonsFrom(project) ::: entities
    }.distinct

    def addAllDatasetsFrom(project: RenkuProject): List[model.Entity] = {
      project.datasets.map(_ -> project).map(_.to[model.Entity.Dataset]) ::: entities
    }.distinct

    def addAllPlansFrom(project: RenkuProject): List[model.Entity] = {
      project.plans.toList.map(_ -> project).map(_.to[model.Entity.Workflow]) ::: entities
    }.distinct

    def addAllPersonsFrom(project: RenkuProject): List[model.Entity] =
      addPersons((project.members ++ project.maybeCreator).toList)
        .addPersons(project.datasets.flatMap(_.provenance.creators.toList))
        .addPersons(project.activities.map(_.author))
        .distinct

    def addPersons(persons: List[Person]): List[model.Entity] = {
      persons.map(_.to[model.Entity.Person]) ::: entities
    }.distinct
  }

  implicit class PersonOps(person: Person) {
    lazy val toAuthUser: AuthUser = AuthUser(person.maybeGitLabId.get, accessTokens.generateOne)
  }
}
