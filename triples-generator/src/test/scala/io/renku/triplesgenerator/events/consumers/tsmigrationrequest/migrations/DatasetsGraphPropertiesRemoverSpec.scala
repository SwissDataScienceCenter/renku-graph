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
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
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

class DatasetsGraphPropertiesRemoverSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

    DatasetsGraphKeywordsRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
    DatasetsGraphImagesRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "remove old keywords and images properties " +
    "from all datasets in the Datasets graph" in projectsDSConfig.use { implicit pcc =>
      val ds1 -> project1 = anyRenkuProjectEntities
        .addDataset {
          datasetEntities(provenanceInternal)
            .modify(replaceDSKeywords(List.empty))
            .modify(replaceDSImages(imageUris.generateList(min = 1)))
        }
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])
      val ds1TopSameAs = ds1.provenance.topmostSameAs

      val ds2 -> project2 = anyRenkuProjectEntities
        .addDataset {
          datasetEntities(provenanceInternal)
            .modify(replaceDSKeywords(datasetKeywords.generateList(min = 1)))
            .modify(replaceDSImages(List.empty))
        }
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])
      val ds2TopSameAs = ds2.provenance.topmostSameAs

      for {
        _ <- provisionProjects(project1, project2).assertNoException
        _ <- runUpdates(insertSchemaKeywords(ds1TopSameAs, ds1)).assertNoException
        _ <- runUpdates(insertSchemaImages(ds1TopSameAs, ds1)).assertNoException
        _ <- runUpdates(insertSchemaKeywords(ds2TopSameAs, ds2)).assertNoException
        _ <- runUpdates(insertSchemaImages(ds2TopSameAs, ds2)).assertNoException

        _ <- fetchKeywords(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.keywords)
        _ <- fetchImages(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.images.map(_.uri))
        _ <- fetchKeywords(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.keywords)
        _ <- fetchImages(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.images.map(_.uri))

        _ <- runUpdate(DatasetsGraphKeywordsRemover.query).assertNoException
        _ <- runUpdate(DatasetsGraphImagesRemover.query).assertNoException

        _ <- fetchKeywords(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchImages(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchKeywords(ds2TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchImages(ds2TopSameAs).asserting(_ shouldBe Nil)
      } yield Succeeded
    }

  private def fetchKeywords(topSameAs: datasets.TopmostSameAs)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[List[datasets.Keyword]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test ds keywords",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?keys
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} schema:keywords ?keys.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("keys").map(datasets.Keyword)))

  private def fetchImages(topSameAs: datasets.TopmostSameAs)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[List[images.ImageUri]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test ds images",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?url ?pos
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} schema:image ?imgId.
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

  private def insertSchemaKeywords(topSameAs: datasets.TopmostSameAs, ds: entities.Dataset[_]) =
    ds.additionalInfo.keywords.map { keyword =>
      SparqlQuery.ofUnsafe(
        "test insert ds keyword",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Datasets.id} {
                 |    ${topSameAs.asEntityId} schema:keywords ${keyword.asObject}.
                 |  }
                 |}
                 |""".stripMargin
      )
    }

  private def insertSchemaImages(topSameAs: datasets.TopmostSameAs, ds: entities.Dataset[_]) =
    ds.additionalInfo.images.map { image =>
      SparqlQuery.ofUnsafe(
        "test insert ds image",
        Prefixes of (rdf -> "rdf", renku -> "renku", schema -> "schema"),
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Datasets.id} {
                 |    ${topSameAs.asEntityId} schema:image ${image.resourceId.asEntityId}.
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
