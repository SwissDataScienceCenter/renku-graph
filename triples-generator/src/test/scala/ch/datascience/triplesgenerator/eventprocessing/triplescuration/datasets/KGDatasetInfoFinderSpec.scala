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

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore._
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles._
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGDatasetInfoFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "findTopmostSameAs" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne
      kgDatasetInfoFinder.findTopmostSameAs(sameAs).unsafeRunSync() shouldBe Option.empty[SameAs]
    }

    "return the dataset's topmostSameAs if this dataset has one" in new TestCase {
      val activity = randomDataSetActivity
      loadToStore(JsonLDTriples(activity.asJsonLD.toJson))

      val parentDataset = activity.entity[DataSet]
      val topmostSameAs = SameAs(parentDataset.entityId)

      kgDatasetInfoFinder.findTopmostSameAs(topmostSameAs).unsafeRunSync() shouldBe Some(parentDataset.topmostSameAs)
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in new TestCase {
      val activity = randomDataSetActivity
      loadToStore(JsonLDTriples(activity.asJsonLD.toJson))

      val parentDataset = activity.entity[DataSet]
      val topmostSameAs = SameAs(parentDataset.entityId)

      removeTopmostSameAs(parentDataset.entityId)

      kgDatasetInfoFinder.findTopmostSameAs(topmostSameAs).unsafeRunSync() shouldBe None
    }
  }

  "findTopmostDerivedFrom" should {

    "return None if there's no dataset with the given id" in new TestCase {
      val derivedFrom = datasetDerivedFroms.generateOne
      kgDatasetInfoFinder.findTopmostDerivedFrom(derivedFrom).unsafeRunSync() shouldBe Option.empty[DerivedFrom]
    }

    "return the dataset's topmostDerivedFrom if this dataset has one" in new TestCase {
      val activity = randomDataSetActivity
      loadToStore(JsonLDTriples(activity.asJsonLD.toJson))

      val parentDataset      = activity.entity[DataSet]
      val topmostDerivedFrom = DerivedFrom(parentDataset.entityId)

      kgDatasetInfoFinder.findTopmostDerivedFrom(topmostDerivedFrom).unsafeRunSync() shouldBe Some(
        parentDataset.topmostDerivedFrom
      )
    }

    "return None if there's a dataset with the given id but it has no topmostDerivedFrom" in new TestCase {
      val activity = randomDataSetActivity
      loadToStore(JsonLDTriples(activity.asJsonLD.toJson))

      val parentDataset      = activity.entity[DataSet]
      val topmostDerivedFrom = DerivedFrom(parentDataset.entityId)

      removeTopmostDerivedFrom(parentDataset.entityId)

      kgDatasetInfoFinder.findTopmostDerivedFrom(topmostDerivedFrom).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val kgDatasetInfoFinder  = new KGDatasetInfoFinderImpl(rdfStoreConfig, logger, timeRecorder)
  }

  private def removeTopmostSameAs(datasetId: EntityId): Unit =
    runUpdate {
      SparqlQuery(
        name = "topmostSameAs removal",
        prefixes = Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
          "PREFIX schema: <http://schema.org/>"
        ),
        body = s"""|DELETE { <$datasetId> renku:topmostSameAs ?sameAs }
                   |WHERE {
                   |  <$datasetId> rdf:type schema:Dataset;
                   |        renku:topmostSameAs ?sameAs.
                   |}
                   |""".stripMargin
      )
    }.unsafeRunSync()

  private def removeTopmostDerivedFrom(datasetId: EntityId): Unit =
    runUpdate {
      SparqlQuery(
        name = "topmostDerivedFrom removal",
        prefixes = Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
          "PREFIX schema: <http://schema.org/>"
        ),
        body = s"""|DELETE { <$datasetId> renku:topmostDerivedFrom ?derivedFrom }
                   |WHERE {
                   |  <$datasetId> rdf:type schema:Dataset;
                   |        renku:topmostDerivedFrom ?derivedFrom.
                   |}
                   |""".stripMargin
      )
    }.unsafeRunSync()
}
