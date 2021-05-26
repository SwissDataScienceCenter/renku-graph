/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Identifier, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.entities.Dataset._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQuery}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators.entityIds
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DescendantsUpdaterSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "prepareUpdates" should {

    "add update queries for all datasets which topmostSameAs or topmostDerivedFrom points to the given datasetId" in new TestCase {

      val dataset1                   = datasetEntities(datasetProvenanceImportedExternal()).generateOne
      val dataset1Id                 = datasetIdentifiers.generateOne
      val dataset1TopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne

      val dataset2Id                 = datasetIdentifiers.generateOne
      val dataset2TopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
      val dataset3Id                 = datasetIdentifiers.generateOne
      val dataset3TopmostSameAs      = datasetTopmostSameAs.generateOne
      val dataset4Id                 = datasetIdentifiers.generateOne
      val dataset4TopmostSameAs      = datasetTopmostSameAs.generateOne
      val dataset5Id                 = datasetIdentifiers.generateOne
      val dataset5TopmostSameAs      = datasetTopmostSameAs.generateOne
      val dataset5TopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne

      loadToStore(
        nonModifiedDataSetCommit()()(datasetIdentifier = dataset1Id,
                                     overrideTopmostSameAs = topmostData.topmostSameAs.some,
                                     overrideTopmostDerivedFrom = dataset1TopmostDerivedFrom.some
        ),
        nonModifiedDataSetCommit()()(datasetIdentifier = dataset2Id,
                                     overrideTopmostSameAs = topmostData.topmostSameAs.some,
                                     overrideTopmostDerivedFrom = dataset2TopmostDerivedFrom.some
        ),
        modifiedDataSetCommit()()(
          datasetIdentifier = dataset3Id,
          overrideTopmostSameAs = dataset3TopmostSameAs.some,
          overrideTopmostDerivedFrom = TopmostDerivedFrom(topmostData.datasetId).some
        ),
        modifiedDataSetCommit()()(
          datasetIdentifier = dataset4Id,
          overrideTopmostSameAs = dataset4TopmostSameAs.some,
          overrideTopmostDerivedFrom = TopmostDerivedFrom(topmostData.datasetId).some
        ),
        modifiedDataSetCommit()()(datasetIdentifier = dataset5Id,
                                  overrideTopmostSameAs = dataset5TopmostSameAs.some,
                                  overrideTopmostDerivedFrom = dataset5TopmostDerivedFrom.some
        )
      )

      val updatedTriples = updater.prepareUpdates[IO](curatedTriples, topmostData)

      updatedTriples.triples shouldBe curatedTriples.triples

      updatedTriples.updatesGroups
        .map(updateGroup => updateGroup.generateUpdates().value)
        .sequence
        .map(_.foldLeft(List.empty[SparqlQuery]) {
          case (allQueries, Right(currentQueries)) => allQueries ++ currentQueries
          case (_, Left(error))                    => throw error
        })
        .flatMap(_.runAll)
        .unsafeRunSync()

      findTopmostData(dataset1Id) shouldBe topmostData.topmostSameAs -> dataset1TopmostDerivedFrom
      findTopmostData(dataset2Id) shouldBe topmostData.topmostSameAs -> dataset2TopmostDerivedFrom
      findTopmostData(dataset3Id) shouldBe dataset3TopmostSameAs     -> topmostData.topmostDerivedFrom
      findTopmostData(dataset4Id) shouldBe dataset4TopmostSameAs     -> topmostData.topmostDerivedFrom
      findTopmostData(dataset5Id) shouldBe dataset5TopmostSameAs     -> dataset5TopmostDerivedFrom
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects[IO].generateOne.copy(updatesGroups = Nil)
    val topmostData    = topmostDatas.generateOne

    val updater = new DescendantsUpdater()
  }

  private lazy val topmostDatas = for {
    datasetId   <- entityIds
    sameAs      <- datasetTopmostSameAs
    derivedFrom <- datasetTopmostDerivedFroms
  } yield TopmostData(datasetId, sameAs, derivedFrom)

  private def findTopmostData(id: Identifier): (TopmostSameAs, TopmostDerivedFrom) =
    runQuery(s"""|SELECT ?topmostSameAs ?topmostDerivedFrom
                 |WHERE {
                 |  ?dsId a schema:Dataset;
                 |        schema:identifier '$id';
                 |        renku:topmostSameAs ?topmostSameAs;
                 |        renku:topmostDerivedFrom ?topmostDerivedFrom.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => TopmostSameAs(row("topmostSameAs")) -> TopmostDerivedFrom(row("topmostDerivedFrom"))) match {
      case row +: Nil => row
      case _          => fail(s"No or more than one record for dataset with $id id")
    }
}
