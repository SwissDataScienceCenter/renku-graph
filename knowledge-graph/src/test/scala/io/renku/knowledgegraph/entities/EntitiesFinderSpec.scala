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

import cats.effect.IO
import cats.syntax.all._
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

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val matchingDsAndProject @ _ ::~ project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      loadToStore(project)

      finder.findEntities(queryParam = None).unsafeRunSync() shouldBe List(
        project.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities which name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings().generateOne

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

      finder.findEntities(queryParam = Endpoint.QueryParam(query.value).some).unsafeRunSync() shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities which keywords matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings().generateOne

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

      finder.findEntities(queryParam = Endpoint.QueryParam(query.value).some).unsafeRunSync() shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return entities which description matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings().generateOne

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

      finder.findEntities(queryParam = Endpoint.QueryParam(query.value).some).unsafeRunSync() shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }

    "return project entities which namespace matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings().generateOne

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(path = projects.Path(s"$query/${relativePaths(maxSegments = 2).generateOne}")))
        .generateOne

      loadToStore(matchingProject, projectEntities(visibilityPublic).generateOne)

      finder.findEntities(queryParam = Endpoint.QueryParam(query.value).some).unsafeRunSync() shouldBe List(
        matchingProject.to[model.Entity.Project]
      ).sortBy(_.name.value)
    }

    "return entities which creator name matches the given query, sorted by name" in new TestCase {
      val query = nonBlankStrings().generateOne

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

      finder.findEntities(queryParam = Endpoint.QueryParam(query.value).some).unsafeRunSync() shouldBe List(
        matchingProject.to[model.Entity.Project],
        matchingDsAndProject.to[model.Entity.Dataset]
      ).sortBy(_.name.value)
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val finder               = new EntitiesFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }
}
