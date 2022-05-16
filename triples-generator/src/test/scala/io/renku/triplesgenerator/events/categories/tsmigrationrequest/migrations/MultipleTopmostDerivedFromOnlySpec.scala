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

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.datasetTopmostDerivedFroms
import cats.effect.IO
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling.QueryBasedMigration
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MultipleTopmostDerivedFromOnlySpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  // there are three cases:
  // * multiple originalIdentifier only (looks like the right one can be matched from wasDerivedFrom)
  // * multiple schema:sameAs only (looks like the right one can be matched from topmostSameAs)
  // * multiple dateCreated only (maybe try to schedule re-provisioning? maybe we need to do an update query on transformation?)

  "query" should {

    "find projects with multiple topmostDerivedFrom" in {
      val (_, correctProject) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val (_ ::~ modifiedDSProject1, brokenProject1) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val (_ ::~ modifiedDSProject2, brokenProject2) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(correctProject, brokenProject1, brokenProject2)

      val illegalTopmost1 = datasetTopmostDerivedFroms.generateOne
      insertTriple(modifiedDSProject1.entityId, "renku:topmostDerivedFrom", illegalTopmost1.showAs[RdfResource])
      val illegalTopmost2 = datasetTopmostDerivedFroms.generateOne
      insertTriple(modifiedDSProject2.entityId, "renku:topmostDerivedFrom", illegalTopmost2.showAs[RdfResource])

      runQuery(MultipleTopmostDerivedFromOnly.query.toString)
        .unsafeRunSync()
        .map(row => projects.Path(row("path")))
        .toSet shouldBe Set(brokenProject1.path, brokenProject2.path)
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      MultipleTopmostDerivedFromOnly[IO].unsafeRunSync().getClass shouldBe classOf[QueryBasedMigration[IO]]
    }
  }
}
