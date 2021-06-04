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
import ch.datascience.rdfstore.entities.Dataset._
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQuery}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators.entityIds
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DescendantsUpdaterSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "prepareUpdates" should {

    "add update queries for all datasets which topmostSameAs or topmostDerivedFrom points to the given dataset id" in new TestCase {

      val ds1 = {
        val orig = datasetEntities(datasetProvenanceImportedInternalAncestorInternal).generateOne
        orig.copy(
          provenance = orig.provenance.copy(topmostSameAs = TopmostSameAs(topmostData.datasetId))
        )
      }
      val ds2 = {
        val orig = datasetEntities(datasetProvenanceImportedInternalAncestorInternal).generateOne
        orig.copy(
          provenance = orig.provenance.copy(topmostSameAs = TopmostSameAs(topmostData.datasetId))
        )
      }
      val ds3 = {
        val orig = datasetEntities(datasetProvenanceModified).generateOne
        orig.copy(
          provenance = orig.provenance.copy(topmostDerivedFrom = TopmostDerivedFrom(topmostData.datasetId))
        )
      }
      val ds4 = {
        val orig = datasetEntities(datasetProvenanceModified).generateOne
        orig.copy(
          provenance = orig.provenance.copy(topmostDerivedFrom = TopmostDerivedFrom(topmostData.datasetId))
        )
      }
      val ds5 = datasetEntities(datasetProvenanceImportedExternal).generateOne

      loadToStore(ds1, ds2, ds3, ds4, ds5)

      val updatedTriples = updater.prepareUpdates[IO](curatedTriples, topmostData)

      assume(updatedTriples.triples === curatedTriples.triples)

      updatedTriples.updatesGroups
        .map(updateGroup => updateGroup.generateUpdates().value)
        .sequence
        .map(_.foldLeft(List.empty[SparqlQuery]) {
          case (allQueries, Right(currentQueries)) => allQueries ++ currentQueries
          case (_, Left(error))                    => throw error
        })
        .flatMap(_.runAll)
        .unsafeRunSync()

      findTopmostData(ds1.identifier) shouldBe topmostData.topmostSameAs    -> ds1.provenance.topmostDerivedFrom
      findTopmostData(ds2.identifier) shouldBe topmostData.topmostSameAs    -> ds2.provenance.topmostDerivedFrom
      findTopmostData(ds3.identifier) shouldBe ds3.provenance.topmostSameAs -> topmostData.topmostDerivedFrom
      findTopmostData(ds4.identifier) shouldBe ds4.provenance.topmostSameAs -> topmostData.topmostDerivedFrom
      findTopmostData(ds5.identifier) shouldBe ds5.provenance.topmostSameAs -> ds5.provenance.topmostDerivedFrom
    }
  }

  private trait TestCase {
    val curatedTriples: CuratedTriples[IO] = curatedTriplesObjects[IO].generateOne.copy(updatesGroups = Nil)
    val topmostData = topmostDatas.generateOne

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
