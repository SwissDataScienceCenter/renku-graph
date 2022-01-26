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
import Filters._
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, projects, users}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Instant, ZoneOffset}

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities - no filters" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val matchingDsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder.findEntities(Filters()).unsafeRunSync() shouldBe List(
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
        .findEntities(Filters(maybeQuery = Query(query.value).some))
        .unsafeRunSync()
        .skipMatchingScore shouldBe List(
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
        .findEntities(Filters(maybeQuery = Query(query.value).some))
        .unsafeRunSync()
        .skipMatchingScore shouldBe List(
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
        .findEntities(Filters(maybeQuery = Query(query.value).some))
        .unsafeRunSync()
        .skipMatchingScore shouldBe List(
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
        .findEntities(Filters(maybeQuery = Query(query.value).some))
        .unsafeRunSync()
        .skipMatchingScore shouldBe List(
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
        .findEntities(Filters(maybeQuery = Query(query.value).some))
        .unsafeRunSync()
        .skipMatchingScore shouldBe List(
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
        .findEntities(Filters(maybeEntityType = EntityType.Project.some))
        .unsafeRunSync() shouldBe List(project.to[model.Entity.Project]).sortBy(_.name.value)
    }

    "return only datasets when 'dataset' type given" in new TestCase {
      val dsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Filters(maybeEntityType = EntityType.Dataset.some))
        .unsafeRunSync() shouldBe List(dsAndProject.to[model.Entity.Dataset]).sortBy(_.name.value)
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
        .findEntities(Filters(maybeCreator = creator.some))
        .unsafeRunSync() shouldBe List(
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
        .findEntities(Filters(maybeCreator = userNames.generateSome))
        .unsafeRunSync() shouldBe Nil
    }
  }

  "findEntities - with visibility filter" should {

    "return entities with matching visibility only" in new TestCase {

      val matchingDsAndProject @ _ ::~ matchingDSProject = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(matchingDSProject, projectEntities(visibilityNonPublic).generateOne)

      finder
        .findEntities(Filters(maybeVisibility = projects.Visibility.Public.some))
        .unsafeRunSync() shouldBe List(
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
        .findEntities(Filters(maybeVisibility = visibilityNonPublic.generateSome))
        .unsafeRunSync() shouldBe Nil
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
        .findEntities(Filters(maybeDate = date.some))
        .unsafeRunSync() shouldBe List(
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
        .findEntities(Filters(maybeDate = date.some))
        .unsafeRunSync() shouldBe List(
        (matchingDS -> matchingDSProject).to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return no entities when no match on date" in new TestCase {
      val _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder
        .findEntities(Filters(maybeDate = dateParams.generateSome))
        .unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val finder               = new EntitiesFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }

  private implicit class ResultsOps(results: List[model.Entity]) {
    lazy val skipMatchingScore: List[model.Entity] = results.map {
      case proj: model.Entity.Project => proj.copy(matchingScore = model.MatchingScore.min)
      case ds:   model.Entity.Dataset => ds.copy(matchingScore = model.MatchingScore.min)
    }
  }
}
