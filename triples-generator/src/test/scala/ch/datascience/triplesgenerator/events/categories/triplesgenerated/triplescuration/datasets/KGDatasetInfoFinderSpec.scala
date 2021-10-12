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
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs, TopmostDerivedFrom}
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities.Dataset._
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGDatasetInfoFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "findTopmostSameAs" should {
    "return None if there is no dataset with that id" in new TestCase {
      kgDatasetInfoFinder.findTopmostSameAs(datasetResourceIds.generateOne).unsafeRunSync() shouldBe None
    }

    "return topmostSameAs for the given id" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      loadToStore(dataset)

      kgDatasetInfoFinder
        .findTopmostSameAs(dataset.resourceId)
        .unsafeRunSync() shouldBe dataset.provenance.topmostSameAs.some
    }
  }

  "findTopmostDerivedFrom" should {
    "return None if there is no dataset with that id" in new TestCase {
      kgDatasetInfoFinder.findTopmostDerivedFrom(datasetResourceIds.generateOne).unsafeRunSync() shouldBe None
    }

    "return topmostSameAs for the given id" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      loadToStore(dataset)

      kgDatasetInfoFinder
        .findTopmostDerivedFrom(dataset.resourceId)
        .unsafeRunSync() shouldBe dataset.provenance.topmostDerivedFrom.some
    }
  }

  "findParentTopmostSameAs" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val sameAs = datasetInternalSameAs.generateOne
      kgDatasetInfoFinder.findParentTopmostSameAs(sameAs).unsafeRunSync() shouldBe Option.empty[SameAs]
    }

    "return the dataset's topmostSameAs if this dataset has one" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      loadToStore(dataset)

      val sameAs = SameAs(dataset.entityId)

      kgDatasetInfoFinder.findParentTopmostSameAs(sameAs).unsafeRunSync() shouldBe Some(
        dataset.provenance.topmostSameAs
      )
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      loadToStore(dataset)

      removeTopmostSameAs(dataset.entityId)

      val sameAs = SameAs(dataset.entityId)

      kgDatasetInfoFinder.findParentTopmostSameAs(sameAs).unsafeRunSync() shouldBe None
    }
  }

  "findParentTopmostDerivedFrom" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val derivedFrom = datasetDerivedFroms.generateOne
      kgDatasetInfoFinder.findParentTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe Option
        .empty[TopmostDerivedFrom]
    }

    "return the dataset's topmostDerivedFrom if this dataset has one" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      loadToStore(dataset)

      val derivedFrom = DerivedFrom(dataset.entityId)

      kgDatasetInfoFinder.findParentTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe Some(
        dataset.provenance.topmostDerivedFrom
      )
    }

    "return None if there's a dataset with the given id but it has no topmostDerivedFrom" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      loadToStore(dataset)

      removeTopmostDerivedFrom(dataset.entityId)

      val derivedFrom = DerivedFrom(dataset.entityId)

      kgDatasetInfoFinder.findParentTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe None
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
