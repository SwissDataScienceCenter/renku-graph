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
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.{SearchInfoDatasets, concatSeparator}
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

class DatasetsGraphFlattenersSpec
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

    DatasetsGraphKeywordsFlattener[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
    DatasetsGraphImagesFlattener[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
    DatasetsGraphCreatorsFlattener[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "add new keywordsConcat, imagesConcat, creatorsNamesConcat and projectsVisibilitiesConcat properties " +
    "to all datasets in the Datasets graph" in {

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
        _ <- runUpdates(projectsDataset, insertSchemaKeywords(ds1TopSameAs, ds1)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaImages(ds1TopSameAs, ds1)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaKeywords(ds2TopSameAs, ds2)).assertNoException
        _ <- runUpdates(projectsDataset, insertSchemaImages(ds2TopSameAs, ds2)).assertNoException

        _ <- fetchKeywords(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.keywords)
        _ <- fetchImages(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.images.map(_.uri))
        _ <- fetchCreatorsNames(ds1TopSameAs).asserting(_ shouldBe ds1.provenance.creators.map(_.name).toList)
        _ <- fetchSlugsVisibs(ds1TopSameAs).asserting(_ shouldBe List(project1.slug -> project1.visibility))
        _ <- fetchKeywords(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.keywords)
        _ <- fetchImages(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.images.map(_.uri))
        _ <- fetchCreatorsNames(ds2TopSameAs).asserting(_ shouldBe ds2.provenance.creators.map(_.name).toList)
        _ <- fetchSlugsVisibs(ds2TopSameAs).asserting(_ shouldBe List(project2.slug -> project2.visibility))

        _ <- runUpdate(projectsDataset, deleteKeywords(ds1TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteImages(ds1TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteCreatorsNames(ds1TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteSlugsVisibs(ds1TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteKeywords(ds2TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteImages(ds2TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteCreatorsNames(ds2TopSameAs)).assertNoException
        _ <- runUpdate(projectsDataset, deleteSlugsVisibs(ds2TopSameAs)).assertNoException

        _ <- fetchKeywords(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchImages(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchCreatorsNames(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchSlugsVisibs(ds1TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchKeywords(ds2TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchImages(ds2TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchCreatorsNames(ds2TopSameAs).asserting(_ shouldBe Nil)
        _ <- fetchSlugsVisibs(ds2TopSameAs).asserting(_ shouldBe Nil)

        _ <- runUpdate(projectsDataset, DatasetsGraphKeywordsFlattener.query).assertNoException
        _ <- runUpdate(projectsDataset, DatasetsGraphImagesFlattener.query).assertNoException
        _ <- runUpdate(projectsDataset, DatasetsGraphCreatorsFlattener.query).assertNoException
        _ <- runUpdate(projectsDataset, DatasetsGraphSlugsVisibilitiesFlattener.query).assertNoException

        _ <- fetchKeywords(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.keywords)
        _ <- fetchImages(ds1TopSameAs).asserting(_ shouldBe ds1.additionalInfo.images.map(_.uri))
        _ <- fetchCreatorsNames(ds1TopSameAs).asserting(_ shouldBe ds1.provenance.creators.map(_.name).toList)
        _ <- fetchSlugsVisibs(ds1TopSameAs).asserting(_ shouldBe List(project1.slug -> project1.visibility))
        _ <- fetchKeywords(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.keywords)
        _ <- fetchImages(ds2TopSameAs).asserting(_ shouldBe ds2.additionalInfo.images.map(_.uri))
        _ <- fetchCreatorsNames(ds2TopSameAs).asserting(_ shouldBe ds2.provenance.creators.map(_.name).toList)
        _ <- fetchSlugsVisibs(ds2TopSameAs).asserting(_ shouldBe List(project2.slug -> project2.visibility))
      } yield Succeeded
    }

  private def fetchKeywords(topSameAs: datasets.TopmostSameAs): IO[List[datasets.Keyword]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test ds keywords",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?keys
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} renku:keywordsConcat ?keys.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.headOption
        .flatMap(_.get("keys").map(_.split(concatSeparator).toList.map(datasets.Keyword)))
        .getOrElse(List.empty[datasets.Keyword])
    )

  private def fetchImages(topSameAs: datasets.TopmostSameAs): IO[List[images.ImageUri]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test ds images",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?imgs
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} renku:imagesConcat ?imgs.
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

  private def fetchCreatorsNames(topSameAs: datasets.TopmostSameAs): IO[List[persons.Name]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test ds creators names",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?creators
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} renku:creatorsNamesConcat ?creators.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.headOption
        .flatMap(_.get("creators").map(_.split(concatSeparator).toList.map(persons.Name)))
        .getOrElse(List.empty[persons.Name])
    )

  private def fetchSlugsVisibs(topSameAs: datasets.TopmostSameAs): IO[List[(projects.Slug, projects.Visibility)]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test ds slugs visibilities",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?slugsVisibs
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ${topSameAs.asEntityId} renku:projectsVisibilitiesConcat ?slugsVisibs.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(
      _.headOption
        .flatMap(
          _.get("slugsVisibs").map(
            _.split(concatSeparator).toList
              .map { case s"$slug:$visibility" =>
                projects.Slug(slug) -> projects.Visibility.from(visibility).fold(throw _, identity)
              }
              .sortBy(_._1)
          )
        )
        .getOrElse(List.empty[(projects.Slug, projects.Visibility)])
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

  private def deleteKeywords(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "test delete ds keywords",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    ?id renku:keywordsConcat ?keys.
               |  }
               |}
               |WHERE {
               |   BIND (${topSameAs.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Datasets.id} {
               |     ?id renku:keywordsConcat ?keys.
               |   }
               |}
               |""".stripMargin
    )

  private def deleteImages(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "test delete ds images",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    ?id renku:imagesConcat ?imgs.
               |  }
               |}
               |WHERE {
               |   BIND (${topSameAs.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Datasets.id} {
               |     ?id renku:imagesConcat ?imgs.
               |   }
               |}
               |""".stripMargin
    )

  private def deleteCreatorsNames(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "test delete ds creators names",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    ?id renku:creatorsNamesConcat ?creators.
               |  }
               |}
               |WHERE {
               |   BIND (${topSameAs.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Datasets.id} {
               |     ?id renku:creatorsNamesConcat ?creators.
               |   }
               |}
               |""".stripMargin
    )

  private def deleteSlugsVisibs(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "test delete ds slugs visibilities",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    ?id renku:projectsVisibilitiesConcat ?slugsVisibs.
               |  }
               |}
               |WHERE {
               |   BIND (${topSameAs.asEntityId} AS ?id)
               |   GRAPH ${GraphClass.Datasets.id} {
               |     ?id renku:projectsVisibilitiesConcat ?slugsVisibs.
               |   }
               |}
               |""".stripMargin
    )

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
