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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.datasetDescriptions
import eu.timepit.refined.auto._
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

class MultipleDSDescriptionsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset
    with MockFactory {

  "run" should {

    "find DS records with multiple schema:description and remove the additional ones" in {
      val (correctDS, correctProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal).modify(replaceDSDesc(datasetDescriptions.generateSome)))
        .generateOne

      val brokenDSDesc1 = datasetDescriptions.generateOne
      val (brokenDS, brokenProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal).modify(replaceDSDesc(brokenDSDesc1.some)))
        .generateOne

      upload(to = renkuDataset, correctProject, brokenProject)

      val brokenDSDesc2 = datasetDescriptions.generateOne
      insert(to = renkuDataset, Triple(brokenDS.entityId, schema / "description", brokenDSDesc2))

      findDescriptions(brokenDS.identification.identifier) shouldBe Set(brokenDSDesc1, brokenDSDesc2)

      runUpdate(on = renkuDataset, MultipleDSDescriptions.query).unsafeRunSync() shouldBe ()

      findDescriptions(brokenDS.identification.identifier)    should (be(Set(brokenDSDesc1)) or be(Set(brokenDSDesc2)))
      findDescriptions(correctDS.identification.identifier) shouldBe correctDS.additionalInfo.maybeDescription.toSet
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      MultipleDSDescriptions[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findDescriptions(id: datasets.Identifier): Set[datasets.Description] = runSelect(
    on = renkuDataset,
    SparqlQuery.of(
      "find DS description",
      Prefixes of schema -> "schema",
      s"""|SELECT ?desc 
          |WHERE { 
          |  ?id a schema:Dataset;
          |      schema:identifier '$id';
          |      schema:description ?desc
          |}""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => datasets.Description(row("desc")))
    .toSet
}
