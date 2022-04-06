/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetIdentifiers
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling.QueryBasedMigration
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DeDuplicateOriginalIdentifiersSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  "query" should {

    "find projects having dataset with two or more originalIdentifiers" in {
      val project1 = {
        val (_ ::~ modifiedDS, proj: RenkuProject.WithoutParent) = renkuProjectEntities(anyVisibility)
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .generateOne

        val breakingModificationId = datasetIdentifiers.generateOne
        val breakingModification = modifiedDS.copy(provenance =
          modifiedDS.provenance.copy(
            initialVersion = datasets.InitialVersion(breakingModificationId),
            topmostDerivedFrom =
              datasets.TopmostDerivedFrom(datasets.DerivedFrom(Dataset.entityId(breakingModificationId)))
          )
        )
        proj.copy(datasets = breakingModification :: proj.datasets)
      }

      val _ ::~ _ ::~ project2 = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(project1.asJsonLD)
      loadToStore(project2.asJsonLD)

      runQuery(DeDuplicateOriginalIdentifiers.query.toString)
        .unsafeRunSync()
        .map(row => projects.Path(row("path")))
        .toSet shouldBe Set(project1.path)
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      DeDuplicateOriginalIdentifiers[IO].unsafeRunSync().getClass shouldBe classOf[QueryBasedMigration[IO]]
    }
  }
}
