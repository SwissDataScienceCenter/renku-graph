/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, Identifier, SameAs}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators.entityIds
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DescendantsUpdaterSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "prepareUpdates" should {

    "add update queries for all datasets which topmostSameAs or topmostDerivedFrom points to the given datasetId" in new TestCase {

      val dataset1Id          = datasetIdentifiers.generateOne
      val dataset1DerivedFrom = datasetDerivedFroms.generateOne
      val dataset2Id          = datasetIdentifiers.generateOne
      val dataset2DerivedFrom = datasetDerivedFroms.generateOne
      val dataset3Id          = datasetIdentifiers.generateOne
      val dataset3SameAs      = datasetSameAs.generateOne
      val dataset4Id          = datasetIdentifiers.generateOne
      val dataset4SameAs      = datasetSameAs.generateOne
      val dataset5Id          = datasetIdentifiers.generateOne
      val dataset5SameAs      = datasetSameAs.generateOne
      val dataset5DerivedFrom = datasetDerivedFroms.generateOne
      loadToStore(
        nonModifiedDataSetCommit()()(datasetIdentifier          = dataset1Id,
                                     overrideTopmostSameAs      = SameAs(topmostData.datasetId).some,
                                     overrideTopmostDerivedFrom = dataset1DerivedFrom.some),
        nonModifiedDataSetCommit()()(datasetIdentifier          = dataset2Id,
                                     overrideTopmostSameAs      = SameAs(topmostData.datasetId).some,
                                     overrideTopmostDerivedFrom = dataset2DerivedFrom.some),
        modifiedDataSetCommit()()(datasetIdentifier             = dataset3Id,
                                  overrideTopmostSameAs         = dataset3SameAs.some,
                                  overrideTopmostDerivedFrom    = DerivedFrom(topmostData.datasetId).some),
        modifiedDataSetCommit()()(datasetIdentifier             = dataset4Id,
                                  overrideTopmostSameAs         = dataset4SameAs.some,
                                  overrideTopmostDerivedFrom    = DerivedFrom(topmostData.datasetId).some),
        modifiedDataSetCommit()()(datasetIdentifier             = dataset5Id,
                                  overrideTopmostSameAs         = dataset5SameAs.some,
                                  overrideTopmostDerivedFrom    = dataset5DerivedFrom.some)
      )

      val updatedTriples = updater.prepareUpdates[IO](curatedTriples, topmostData)

      updatedTriples.triples shouldBe curatedTriples.triples

      updatedTriples.updates
        .map(update => update().fold(err => throw err, runUpdate).unsafeRunSync())
        .sequence
        .unsafeRunSync()
      findTopmostData(dataset1Id) shouldBe topmostData.sameAs -> dataset1DerivedFrom
      findTopmostData(dataset2Id) shouldBe topmostData.sameAs -> dataset2DerivedFrom
      findTopmostData(dataset3Id) shouldBe dataset3SameAs     -> topmostData.derivedFrom
      findTopmostData(dataset4Id) shouldBe dataset4SameAs     -> topmostData.derivedFrom
      findTopmostData(dataset5Id) shouldBe dataset5SameAs     -> dataset5DerivedFrom
    }
  }

  private trait TestCase {
    implicit val context = MonadError[IO, Throwable]
    val curatedTriples: CuratedTriples[IO] = curatedTriplesObjects[IO].generateOne.copy(updates = Nil)
    val topmostData = topmostDatas.generateOne

    val updater = new DescendantsUpdater()
  }

  private lazy val topmostDatas = for {
    datasetId   <- entityIds
    sameAs      <- datasetSameAs
    derivedFrom <- datasetDerivedFroms
  } yield TopmostData(datasetId, sameAs, derivedFrom)

  private def findTopmostData(id: Identifier): (SameAs, DerivedFrom) =
    runQuery(s"""|SELECT ?topmostSameAs ?topmostDerivedFrom
                 |WHERE {
                 |  ?dsId rdf:type schema:Dataset;
                 |        schema:identifier '$id';
                 |        renku:topmostSameAs/schema:url ?topmostSameAs;
                 |        renku:topmostDerivedFrom ?topmostDerivedFrom.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => SameAs(row("topmostSameAs")) -> DerivedFrom(row("topmostDerivedFrom"))) match {
      case row +: Nil => row
      case _          => fail(s"No or more than one record for dataset with $id id")
    }
}
