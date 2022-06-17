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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations

import cats.effect.IO
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets
import io.renku.graph.model.datasets.TopmostDerivedFrom
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MultipleTopmostDerivedFromsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryRdfStore
    with MockFactory {

  "run" should {

    "find DS records with multiple renku:topmostDerivedFrom and remove the additional ones" in new TestCase {
      satisfyMocks

      val (_ ::~ correctDS, correctProject) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val (brokenDS, brokenProject) = {
        val (_ ::~ okDS, brokenProject) = renkuProjectEntities(anyVisibility)
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .generateOne
        brokenProject.addDataset(okDS.createModification())
      }

      loadToStore(correctProject, brokenProject)

      val illegalTopmost = TopmostDerivedFrom(brokenDS.provenance.derivedFrom)
      insertTriple(brokenDS.entityId, "renku:topmostDerivedFrom", illegalTopmost.showAs[RdfResource])

      findTopmostDerivedFroms(correctDS.identification.identifier) shouldBe Set(correctDS.provenance.topmostDerivedFrom)
      findTopmostDerivedFroms(brokenDS.identification.identifier) shouldBe Set(
        brokenDS.provenance.topmostDerivedFrom,
        illegalTopmost
      )

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      findTopmostDerivedFroms(correctDS.identification.identifier) shouldBe Set(correctDS.provenance.topmostDerivedFrom)
      findTopmostDerivedFroms(brokenDS.identification.identifier)  shouldBe Set(brokenDS.provenance.topmostDerivedFrom)
    }
  }

  "apply" should {
    "return an RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  private trait TestCase {
    implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recordsFinder     = RecordsFinder[IO](renkuStoreConfig)
    val updateRunner      = UpdateQueryRunner[IO](renkuStoreConfig)
    val migration         = new MultipleTopmostDerivedFroms[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findTopmostDerivedFroms(id: datasets.Identifier): Set[datasets.TopmostDerivedFrom] =
    runQuery(s"""|SELECT ?top 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |      schema:identifier '$id';
                 |      renku:topmostDerivedFrom ?top
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.TopmostDerivedFrom(row("top")))
      .toSet
}
