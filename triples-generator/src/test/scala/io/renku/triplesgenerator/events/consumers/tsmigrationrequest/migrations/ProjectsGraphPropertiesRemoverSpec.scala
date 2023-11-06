/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RegisteredUpdateQueryMigration

class ProjectsGraphPropertiesRemoverSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with AsyncMockFactory {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

    ProjectsGraphImagesRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
    ProjectsGraphImagesRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "remove old keywords and images properties " +
    "from all projects in the Projects graph" in {

      val project1 = anyRenkuProjectEntities
        .modify(replaceProjectKeywords(Set.empty))
        .modify(replaceImages(imageUris.generateList(min = 1)))
        .generateOne
        .to[entities.Project]
      val project2 = anyRenkuProjectEntities
        .modify(replaceProjectKeywords(projectKeywords.generateSet(min = 1)))
        .modify(replaceImages(Nil))
        .generateOne
        .to[entities.Project]

      for {
        _ <- provisionProjects(project1, project2).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaKeywords(project1.resourceId, project1)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaImages(project1.resourceId, project1)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaKeywords(project2.resourceId, project2)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaImages(project2.resourceId, project2)).assertNoException

        _ <- fetchKeywords(project1.resourceId).asserting(_ shouldBe project1.keywords)
        _ <- fetchImages(project1.resourceId).asserting(_ shouldBe project1.images.map(_.uri))
        _ <- fetchKeywords(project2.resourceId).asserting(_ shouldBe project2.keywords)
        _ <- fetchImages(project2.resourceId).asserting(_ shouldBe project2.images.map(_.uri))

        _ <- runUpdate(projectsDataset, ProjectsGraphKeywordsRemover.query).assertNoException
        _ <- runUpdate(projectsDataset, ProjectsGraphImagesRemover.query).assertNoException

        _ <- fetchKeywords(project1.resourceId).asserting(_ shouldBe Set.empty)
        _ <- fetchImages(project1.resourceId).asserting(_ shouldBe Nil)
        _ <- fetchKeywords(project2.resourceId).asserting(_ shouldBe Set.empty)
        _ <- fetchImages(project2.resourceId).asserting(_ shouldBe Nil)
      } yield Succeeded
    }

  private def fetchKeywords(projectId: projects.ResourceId): IO[Set[projects.Keyword]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test project keywords",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?keys
                 |WHERE {
                 |   GRAPH ${GraphClass.Projects.id} {
                 |     ${projectId.asEntityId} schema:keywords ?keys.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("keys").map(projects.Keyword)).toSet)

  private def fetchImages(projectId: projects.ResourceId): IO[List[images.ImageUri]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test project images",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?url ?pos
                 |WHERE {
                 |   GRAPH ${GraphClass.Projects.id} {
                 |     ${projectId.asEntityId} schema:image ?imgId.
                 |     ?imgId schema:contentUrl ?url.
                 |     ?imgId schema:position ?pos.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.flatMap(row => (row.get("url").map(images.ImageUri(_)) -> row.get("pos").map(_.toInt)).mapN(_ -> _))
        .sortBy(_._2)
        .map(_._1)
    )

  private def insertSchemaKeywords(projectId: projects.ResourceId, project: entities.Project) =
    project.keywords.toList.map { keyword =>
      SparqlQuery.ofUnsafe(
        "test insert project keyword",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Projects.id} {
                 |    ${projectId.asEntityId} schema:keywords ${keyword.asObject}.
                 |  }
                 |}
                 |""".stripMargin
      )
    }

  private def insertSchemaImages(projectId: projects.ResourceId, project: entities.Project) =
    project.images.map { image =>
      SparqlQuery.ofUnsafe(
        "test insert project image",
        Prefixes of (rdf -> "rdf", renku -> "renku", schema -> "schema"),
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Projects.id} {
                 |    ${projectId.asEntityId} schema:image ${image.resourceId.asEntityId}.
                 |    ${image.resourceId.asEntityId} rdf:type schema:ImageObject.
                 |    ${image.resourceId.asEntityId} schema:contentUrl ${image.uri.asObject}.
                 |    ${image.resourceId.asEntityId} schema:position ${image.position.asObject}.
                 |  }
                 |}
                 |""".stripMargin
      )
    }

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
