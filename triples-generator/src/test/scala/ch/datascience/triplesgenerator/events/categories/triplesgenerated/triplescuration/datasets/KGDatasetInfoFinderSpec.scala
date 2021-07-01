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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs, TopmostDerivedFrom}
import ch.datascience.graph.model.testentities.Dataset._
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGDatasetInfoFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "findTopmostSameAs" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val sameAs = datasetInternalSameAs.generateOne
      kgDatasetInfoFinder.findTopmostSameAs(sameAs).unsafeRunSync() shouldBe Option.empty[SameAs]
    }

    "return the dataset's topmostSameAs if this dataset has one" in new TestCase {
      val dataset = datasetEntities(ofAnyProvenance).generateOne

      loadToStore(dataset.asJsonLD)

      val sameAs = SameAs(dataset.entityId)

      kgDatasetInfoFinder.findTopmostSameAs(sameAs).unsafeRunSync() shouldBe Some(
        dataset.provenance.topmostSameAs
      )
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in new TestCase {
      val dataset = datasetEntities(ofAnyProvenance).generateOne

      loadToStore(dataset.asJsonLD)

      removeTopmostSameAs(dataset.entityId)

      val sameAs = SameAs(dataset.entityId)

      kgDatasetInfoFinder.findTopmostSameAs(sameAs).unsafeRunSync() shouldBe None
    }
  }

  "findTopmostDerivedFrom" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val derivedFrom = datasetDerivedFroms.generateOne
      kgDatasetInfoFinder.findTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe Option.empty[TopmostDerivedFrom]
    }

    "return the dataset's topmostDerivedFrom if this dataset has one" in new TestCase {
      val dataset = datasetEntities(ofAnyProvenance).generateOne

      loadToStore(dataset.asJsonLD)

      val derivedFrom = DerivedFrom(dataset.entityId)

      kgDatasetInfoFinder.findTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe Some(
        dataset.provenance.topmostDerivedFrom
      )
    }

    "return None if there's a dataset with the given id but it has no topmostDerivedFrom" in new TestCase {
      val dataset = datasetEntities(ofAnyProvenance).generateOne

      loadToStore(dataset.asJsonLD)

      removeTopmostDerivedFrom(dataset.entityId)

      val derivedFrom = DerivedFrom(dataset.entityId)

      kgDatasetInfoFinder.findTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val kgDatasetInfoFinder  = new KGDatasetInfoFinderImpl(rdfStoreConfig, logger, timeRecorder)
  }

  private def removeTopmostSameAs(datasetId: EntityId): Unit = runUpdate {
    SparqlQuery.of(
      name = "topmostSameAs removal",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      body = s"""|DELETE { <$datasetId> renku:topmostSameAs ?sameAs }
                 |WHERE {
                 |  <$datasetId> a schema:Dataset;
                 |               renku:topmostSameAs ?sameAs.
                 |}
                 |""".stripMargin
    )
  }.unsafeRunSync()

  private def removeTopmostDerivedFrom(datasetId: EntityId): Unit = runUpdate {
    SparqlQuery.of(
      name = "topmostDerivedFrom removal",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      body = s"""|DELETE { <$datasetId> renku:topmostDerivedFrom ?derivedFrom }
                 |WHERE {
                 |  <$datasetId> a schema:Dataset;
                 |               renku:topmostDerivedFrom ?derivedFrom.
                 |}
                 |""".stripMargin
    )
  }.unsafeRunSync()
}
