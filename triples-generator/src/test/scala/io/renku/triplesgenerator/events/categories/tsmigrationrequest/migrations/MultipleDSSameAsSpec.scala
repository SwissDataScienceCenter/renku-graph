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
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.datasetSameAs
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MultipleDSSameAsSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore with MockFactory {

  "run" should {

    "find DS records with multiple schema:sameAs and remove the additional ones" in new TestCase {
      satisfyMocks

      val (ds, dsProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val (importedDS, importedDSProject) = renkuProjectEntities(anyVisibility)
        .importDataset(ds)
        .generateOne

      loadToStore(dsProject, importedDSProject)

      val additionalSameAs       = datasetSameAs.generateOne
      val additionalSameAsJsonLD = additionalSameAs.asJsonLD
      loadToStore(additionalSameAsJsonLD)
      val additionalSameAsId = additionalSameAsJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
      insertTriple(importedDS.entityId, "schema:sameAs", show"<$additionalSameAsId>")

      findSameAs(importedDS.identification.identifier) shouldBe Set(importedDS.provenance.sameAs, additionalSameAs)
      findSameAs(ds.identification.identifier)         shouldBe Set.empty

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      findSameAs(importedDS.identification.identifier) shouldBe Set(importedDS.provenance.sameAs)
      findSameAs(ds.identification.identifier)         shouldBe Set.empty
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  private trait TestCase {
    implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recordsFinder     = RecordsFinder[IO](rdfStoreConfig)
    val updateRunner      = UpdateQueryRunner[IO](rdfStoreConfig)
    val migration         = new MultipleDSSameAs[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findSameAs(id: datasets.Identifier): Set[datasets.SameAs] =
    runQuery(s"""|SELECT ?sameAs
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |      schema:identifier '$id';
                 |      schema:sameAs/schema:url ?sameAs
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.SameAs(row("sameAs")))
      .toSet
}
