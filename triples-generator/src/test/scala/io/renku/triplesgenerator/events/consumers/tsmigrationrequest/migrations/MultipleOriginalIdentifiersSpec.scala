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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.datasets.OriginalIdentifier
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MultipleOriginalIdentifiersSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset
    with MockFactory {

  "run" should {

    "find DS records with multiple renku:originalIdentifier and remove the additional ones" in new TestCase {
      satisfyMocks

      val (_ ::~ correctDS, correctProject) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      val (_ ::~ brokenDS, brokenProject) = renkuProjectEntities(anyVisibility)
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      upload(to = renkuDataset, correctProject, brokenProject)

      insert(to = renkuDataset,
             Triple(brokenDS.entityId, renku / "originalIdentifier", brokenDS.identification.identifier)
      )

      findOriginalIdentifiers(brokenDS.identification.identifier) shouldBe Set(
        brokenDS.provenance.originalIdentifier,
        OriginalIdentifier(brokenDS.identification.identifier)
      )
      findOriginalIdentifiers(correctDS.identification.identifier) shouldBe Set(correctDS.provenance.originalIdentifier)

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      findOriginalIdentifiers(brokenDS.identification.identifier)  shouldBe Set(brokenDS.provenance.originalIdentifier)
      findOriginalIdentifiers(correctDS.identification.identifier) shouldBe Set(correctDS.provenance.originalIdentifier)
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
    val recordsFinder     = RecordsFinder[IO](renkuDSConnectionInfo)
    val updateRunner      = UpdateQueryRunner[IO](renkuDSConnectionInfo)
    val migration         = new MultipleOriginalIdentifiers[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findOriginalIdentifiers(id: datasets.Identifier): Set[datasets.OriginalIdentifier] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds originalIdentifier",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?originalId
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      renku:originalIdentifier ?originalId
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.OriginalIdentifier(row("originalId")))
      .toSet
}
