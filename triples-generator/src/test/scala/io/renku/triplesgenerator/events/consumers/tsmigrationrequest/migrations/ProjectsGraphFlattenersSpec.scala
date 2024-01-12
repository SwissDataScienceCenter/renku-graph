/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.{TestSearchInfoDatasets, concatSeparator}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.TestMetricsRegistry
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RegisteredUpdateQueryMigration

class ProjectsGraphFlattenersSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

    ProjectsGraphKeywordsFlattener[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
    ProjectsGraphImagesFlattener[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "add new keywordsConcat and imagesConcat properties to all projects in the Project graph" in projectsDSConfig
    .use { implicit pcc =>
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
        _ <- runUpdates(insertSchemaKeywords(project1.resourceId, project1)).assertNoException
        _ <- runUpdates(insertSchemaImages(project1.resourceId, project1)).assertNoException
        _ <- runUpdates(insertSchemaKeywords(project2.resourceId, project2)).assertNoException
        _ <- runUpdates(insertSchemaImages(project2.resourceId, project2)).assertNoException

        _ <- fetchKeywords(project1.resourceId).asserting(_ shouldBe project1.keywords)
        _ <- fetchImages(project1.resourceId).asserting(_ shouldBe project1.images.map(_.uri))
        _ <- fetchKeywords(project2.resourceId).asserting(_ shouldBe project2.keywords)
        _ <- fetchImages(project2.resourceId).asserting(_ shouldBe project2.images.map(_.uri))

        _ <- runUpdate(deleteKeywords(project1.resourceId)).assertNoException
        _ <- runUpdate(deleteImages(project1.resourceId)).assertNoException
        _ <- runUpdate(deleteKeywords(project2.resourceId)).assertNoException
        _ <- runUpdate(deleteImages(project2.resourceId)).assertNoException

        _ <- fetchKeywords(project1.resourceId).asserting(_ shouldBe Set.empty)
        _ <- fetchImages(project1.resourceId).asserting(_ shouldBe Nil)
        _ <- fetchKeywords(project2.resourceId).asserting(_ shouldBe Set.empty)
        _ <- fetchImages(project2.resourceId).asserting(_ shouldBe Nil)

        _ <- runUpdate(ProjectsGraphKeywordsFlattener.query).assertNoException
        _ <- runUpdate(ProjectsGraphImagesFlattener.query).assertNoException

        _ <- fetchKeywords(project1.resourceId).asserting(_ shouldBe project1.keywords)
        _ <- fetchImages(project1.resourceId).asserting(_ shouldBe project1.images.map(_.uri))
        _ <- fetchKeywords(project2.resourceId).asserting(_ shouldBe project2.keywords)
        _ <- fetchImages(project2.resourceId).asserting(_ shouldBe project2.images.map(_.uri))
      } yield Succeeded
    }

  private def fetchKeywords(projectId: projects.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[projects.Keyword]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test project keywords",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?keys
                 |WHERE {
                 |   GRAPH ${GraphClass.Projects.id} {
                 |     ${projectId.asEntityId} renku:keywordsConcat ?keys.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.headOption
        .flatMap(_.get("keys").map(_.split(concatSeparator).toSet.map(projects.Keyword)))
        .getOrElse(Set.empty[projects.Keyword])
    )

  private def fetchImages(projectId: projects.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[List[images.ImageUri]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test project images",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?imgs
                 |WHERE {
                 |   GRAPH ${GraphClass.Projects.id} {
                 |     ${projectId.asEntityId} renku:imagesConcat ?imgs.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.headOption
        .flatMap(
          _.get("imgs").map(
            _.split(concatSeparator).toList
              .map { case s"$pos:$uri" =>
                pos.toInt -> images.ImageUri(uri)
              }
              .sortBy(_._1)
              .map(_._2)
          )
        )
        .getOrElse(List.empty[images.ImageUri])
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

  private def deleteKeywords(projectId: projects.ResourceId) =
    SparqlQuery.ofUnsafe(
      "test delete keywords",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id renku:keywordsConcat ?keys.
               |  }
               |}
               |WHERE {
               |   BIND (${projectId.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Projects.id} {
               |     ?id renku:keywordsConcat ?keys.
               |   }
               |}
               |""".stripMargin
    )

  private def deleteImages(projectId: projects.ResourceId) =
    SparqlQuery.ofUnsafe(
      "test delete images",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id renku:imagesConcat ?imgs.
               |  }
               |}
               |WHERE {
               |   BIND (${projectId.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Projects.id} {
               |     ?id renku:imagesConcat ?imgs.
               |   }
               |}
               |""".stripMargin
    )

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
