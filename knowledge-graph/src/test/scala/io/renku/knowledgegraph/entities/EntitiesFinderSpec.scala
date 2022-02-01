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
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, projects, users}
import io.renku.http.rest.SortBy
import io.renku.http.rest.paging.model._
import io.renku.http.rest.paging.{PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.entities.model.Entity
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Instant, ZoneOffset}
import scala.util.Random

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities - no filters" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val matchingDsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe List(
        project.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  "findEntities - with query filter" should {

    "return entities which name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(name = sentenceContaining(query).map(_.value).generateAs(projects.Name)))
        .generateOne

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            identificationLens.modify(
              _.copy(name = sentenceContaining(query).map(_.value).generateAs(datasets.Name))
            )
          )
        )
        .generateOne

      loadToStore(matchingProject, matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities which keywords matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(
          _.copy(keywords =
            Set(sentenceContaining(query).map(_.value).generateAs(projects.Keyword), projectKeywords.generateOne)
          )
        )
        .generateOne

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            additionalInfoLens.modify(
              _.copy(keywords =
                List(sentenceContaining(query).map(_.value).generateAs(datasets.Keyword), datasetKeywords.generateOne)
              )
            )
          )
        )
        .generateOne

      loadToStore(matchingProject, matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities which description matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(maybeDescription = sentenceContaining(query).map(_.value).generateAs(projects.Description).some))
        .generateOne

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            additionalInfoLens.modify(
              _.copy(maybeDescription = sentenceContaining(query).map(_.value).generateAs(datasets.Description).some)
            )
          )
        )
        .generateOne

      loadToStore(matchingProject, matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return project entities which namespace matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(path = projects.Path(s"$query/${relativePaths(maxSegments = 2).generateOne}")))
        .generateOne

      loadToStore(matchingProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        matchingProject.to[model.Entity.Project]
      ).sortBy(_.name.value)
    }

    "return entities which creator name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings(minLength = 3).generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(
          creatorLens.modify(_ =>
            personEntities.generateOne.copy(name = sentenceContaining(query).map(_.value).generateAs(users.Name)).some
          )
        )
        .generateOne

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ =>
                Set(
                  personEntities.generateOne.copy(name = sentenceContaining(query).map(_.value).generateAs(users.Name)),
                  personEntities.generateOne
                )
              )
            )
          )
        )
        .generateOne

      loadToStore(matchingProject, matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  "findEntities - with entity type filter" should {

    "return only projects when 'project' type given" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Project.some)))
        .unsafeRunSync()
        .results shouldBe List(project.to[model.Entity.Project]).sortBy(_.name.value)
    }

    "return only datasets when 'dataset' type given" in new TestCase {
      val dsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeEntityType = EntityType.Dataset.some)))
        .unsafeRunSync()
        .results shouldBe List(dsAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
    }
  }

  "findEntities - with creator filter" should {

    "return entities with matching creator only" in new TestCase {
      val creator = userNames.generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => personEntities.generateOne.copy(name = creator).some))
        .generateOne

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ => Set(personEntities.generateOne, personEntities.generateOne.copy(name = creator)))
            )
          )
        )
        .generateOne

      loadToStore(matchingProject, matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeCreator = creator.some)))
        .unsafeRunSync()
        .results shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on visibility" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Criteria(Filters(maybeCreator = userNames.generateSome)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with visibility filter" should {

    "return entities with matching visibility only" in new TestCase {

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(matchingDSProject, projectEntities(visibilityNonPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeVisibility = projects.Visibility.Public.some)))
        .unsafeRunSync()
        .results shouldBe List(
        matchingDSProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on creator" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
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
      val date = dateParams.generateOne
      val dateAsInstant = Instant
        .from(date.value.atStartOfDay(ZoneOffset.UTC))
        .plusSeconds(positiveInts(60 * 60 * 24 - 1).generateOne.value)

      val matchingDS ::~ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(dateCreated = projects.DateCreated(dateAsInstant)))
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify(
              provenanceLens[Dataset.Provenance.Internal].modify(_.copy(date = datasets.DateCreated(dateAsInstant)))
            )
        )
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeDate = date.some)))
        .unsafeRunSync()
        .results shouldBe List(
        matchingDSProject.to[model.Entity.Project],
        (matchingDS -> matchingDSProject).to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities with matching date only - case of DatePublished" in new TestCase {
      val date = dateParams.generateOne

      val matchingDS ::~ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(_.copy(date = datasets.DatePublished(date.value)))
            )
        )
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(matchingDSProject, projectEntities(visibilityPublic).generateOne)

      finder
        .findEntities(Criteria(Filters(maybeDate = date.some)))
        .unsafeRunSync()
        .results shouldBe List(
        (matchingDS -> matchingDSProject).to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on date" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
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
      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(matchingDSProject)

      val direction = sortingDirections.generateOne

      finder
        .findEntities(Criteria(sorting = Sorting.By(Sorting.ByName, direction)))
        .unsafeRunSync()
        .results shouldBe List(
        matchingDSProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value).use(direction)
    }

    "be sorting by Date if requested" in new TestCase {
      val externalDS ::~ internalDS ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceImportedExternal))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(project)

      val direction = sortingDirections.generateOne

      finder
        .findEntities(Criteria(sorting = Sorting.By(Sorting.ByDate, direction)))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        (externalDS -> project).to[model.Entity.Dataset],
        (internalDS -> project).to[model.Entity.Dataset]
      ).sortBy(_.dateAsInstant).use(direction)
    }

    "be sorting by Matching Score if requested" in new TestCase {

      val query = nonBlankStrings(minLength = 3).generateOne

      val ds ::~ project = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(name = projects.Name(query.value)))
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            identificationLens.modify(
              _.copy(name = sentenceContaining(query).map(_.value).generateAs(datasets.Name))
            )
          )
        )
        .generateOne

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
        project.to[model.Entity.Project], // should have higher score as its name is the query
        (ds -> project).to[model.Entity.Dataset]
      ).use(direction).reverse
    }
  }

  "findEntities - with paging" should {

    val ds1 ::~ ds2 ::~ project = renkuProjectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceNonModified))
      .addDataset(datasetEntities(provenanceNonModified))
      .generateOne

    "return the only page" in new TestCase {

      loadToStore(project)

      val paging = PagingRequest(Page(1), PerPage(3))

      val results = finder
        .findEntities(Criteria(paging = paging))
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results shouldBe List(
        project.to[Entity.Project],
        (ds1 -> project).to[Entity.Dataset],
        (ds2 -> project).to[Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return the requested page with info if there are more" in new TestCase {

      loadToStore(project)

      val paging = PagingRequest(Page(Random.nextInt(3) + 1), PerPage(1))

      val results = finder
        .findEntities(Criteria(paging = paging))
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results shouldBe List(
        project.to[Entity.Project],
        (ds1 -> project).to[Entity.Dataset],
        (ds2 -> project).to[Entity.Dataset]
      ).sortBy(_.name.value).get(paging.page.value - 1).toList
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
        .findEntities(Criteria())
        .unsafeRunSync()
        .results shouldBe List(
        originalDSProject.to[model.Entity.Project],
        importedDSProject.to[model.Entity.Project],
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

      val results = finder.findEntities(Criteria()).unsafeRunSync().results

      val expectedProjects = List(project1WithImportedDS, project2WithImportedDS, projectWithDSImportedFromProject)
        .map(_.to[model.Entity.Project])

      results should {
        be((importedDSAndProject1.to[model.Entity.Dataset] :: expectedProjects).sortBy(_.name.value)) or
          be((importedDSAndProject2.to[model.Entity.Dataset] :: expectedProjects).sortBy(_.name.value)) or
          be((importedDSAndProject3.to[model.Entity.Dataset] :: expectedProjects).sortBy(_.name.value))
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
        .findEntities(Criteria(sorting = Sorting.By(Sorting.ByDate, SortBy.Direction.Asc)))
        .unsafeRunSync()
        .results shouldBe List(
        originalDSProject.to[model.Entity.Project],
        importedDSProject.to[model.Entity.Project],
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
        .findEntities(Criteria())
        .unsafeRunSync()
        .results shouldBe List(
        originalDSProject.to[model.Entity.Project],
        importedDSProject.to[model.Entity.Project],
        importedDSAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  "findEntities - in case of a forks with datasets" should {

    "de-duplicate datasets when on forked projects" in new TestCase {
      val _ ::~ modifiedDS ::~ originalDSProject = renkuProjectEntities(visibilityPublic)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val original ::~ fork = originalDSProject.forkOnce()

      loadToStore(original, fork)

      val results = finder.findEntities(Criteria()).unsafeRunSync().results

      val expectedProjects = List(original, fork).map(_.to[model.Entity.Project])
      results should {
        be(((modifiedDS -> original).to[model.Entity.Dataset] :: expectedProjects).sortBy(_.name.value)) or
          be(((modifiedDS -> fork).to[model.Entity.Dataset] :: expectedProjects).sortBy(_.name.value))
      }
    }
  }

  "findEntities - in case of a dataset on forks with different visibility" should {

    "favour dataset on public project projects if exists" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val dsAndPublicProject @ _ ::~ publicProject = renkuProjectEntities(visibilityPublic)
        .importDataset(externalDS)
        .generateOne

      val member = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne
      val original ::~ fork = {
        val original ::~ fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }

      loadToStore(original, fork)

      finder.findEntities(Criteria(maybeUser = member.toAuthUser.some)).unsafeRunSync().results shouldBe List(
        original.to[model.Entity.Project],
        fork.to[model.Entity.Project],
        dsAndPublicProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "favour dataset on internal project projects if exists" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndInternalProject @ _ ::~ internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(_.copy(members = Set(member)))
        .importDataset(externalDS)
        .generateOne

      val original ::~ fork = {
        val original ::~ fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }

      loadToStore(original, fork)

      finder.findEntities(Criteria(maybeUser = member.toAuthUser.some)).unsafeRunSync().results shouldBe List(
        original.to[model.Entity.Project],
        fork.to[model.Entity.Project],
        dsAndInternalProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "select dataset on private project if there's no project with broader visibility" in new TestCase {
      val externalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne

      val member = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne
      val dsAndProject @ _ ::~ privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(_.copy(members = Set(member)))
        .importDataset(externalDS)
        .generateOne

      loadToStore(privateProject)

      finder.findEntities(Criteria(maybeUser = member.toAuthUser.some)).unsafeRunSync().results shouldBe List(
        privateProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  "findEntities - security" should {

    "not return non-public entities if no user who is a member is given" in new TestCase {

      val _ ::~ nonPublicProject = renkuProjectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      val dsAndProject @ _ ::~ publicProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(nonPublicProject, publicProject)

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe List(
        publicProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "not return non-public entities if the given user has no access to them" in new TestCase {

      val _ ::~ nonPublicProject = renkuProjectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      val dsAndProject @ _ ::~ publicProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(nonPublicProject, publicProject)

      finder
        .findEntities(
          Criteria(maybeUser = personEntities(userGitLabIds.toGeneratorOfSomes).generateSome.map(_.toAuthUser))
        )
        .unsafeRunSync()
        .results shouldBe List(
        publicProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return non-public entities if the given user has access to them" in new TestCase {
      val member = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne

      val dsAndProject @ _ ::~ project = renkuProjectEntities(visibilityNonPublic)
        .modify(_.copy(members = Set(member)))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder.findEntities(Criteria(maybeUser = member.toAuthUser.some)).unsafeRunSync().results shouldBe List(
        project.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val finder               = new EntitiesFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }

  private implicit class PagingResponseOps(response: PagingResponse[model.Entity]) {
    lazy val resultsWithSkippedMatchingScore: List[model.Entity] = response.results.skipMatchingScore
  }

  private implicit class ResultsOps(results: List[model.Entity]) {
    lazy val skipMatchingScore: List[model.Entity] = results.map {
      case proj: model.Entity.Project => proj.copy(matchingScore = model.MatchingScore.min)
      case ds:   model.Entity.Dataset => ds.copy(matchingScore = model.MatchingScore.min)
    }

    lazy val use: SortBy.Direction => List[model.Entity] = {
      case SortBy.Direction.Asc  => results
      case SortBy.Direction.Desc => results.reverse
    }
  }

  private implicit class EntityOps(entity: model.Entity) {
    lazy val dateAsInstant: Instant = entity match {
      case proj: model.Entity.Project => proj.date.value
      case ds:   model.Entity.Dataset => ds.date.instant
    }
  }

  implicit class PersonOps(person: Person) {
    lazy val toAuthUser: AuthUser = AuthUser(person.maybeGitLabId.get, accessTokens.generateOne)
  }
}
