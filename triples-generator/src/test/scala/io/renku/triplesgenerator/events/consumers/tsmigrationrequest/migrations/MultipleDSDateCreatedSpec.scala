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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetCreatedDates
import io.renku.graph.model._
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

import java.time.Instant

class MultipleDSDateCreatedSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset
    with MockFactory {

  "query" should {

    "find datasets having multiple schema:dateCreated and remove all except the oldest one" in {

      val (validDS, validDSProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      val (brokenDS, brokenDSProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      upload(to = renkuDataset, validDSProject, brokenDSProject)

      val anotherDateCreated = datasetCreatedDates(min = brokenDS.provenance.date.instant).generateOne
      insert(to = renkuDataset, Triple(brokenDS.resourceId, schema / "dateCreated", anotherDateCreated))

      findDateCreated(validDS.identification.identifier)  shouldBe Set(validDS.provenance.date)
      findDateCreated(brokenDS.identification.identifier) shouldBe Set(brokenDS.provenance.date, anotherDateCreated)

      runUpdate(on = renkuDataset, MultipleDSDateCreated.query).unsafeRunSync() shouldBe ()

      findDateCreated(validDS.identification.identifier)  shouldBe Set(validDS.provenance.date)
      findDateCreated(brokenDS.identification.identifier) shouldBe Set(brokenDS.provenance.date)
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      MultipleDSDateCreated[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findDateCreated(id: datasets.Identifier): Set[datasets.DateCreated] = runSelect(
    on = renkuDataset,
    SparqlQuery.of(
      "find DS date created",
      Prefixes of schema -> "schema",
      s"""|SELECT ?date 
          |WHERE { 
          |  ?id a schema:Dataset;
          |      schema:identifier '$id';
          |      schema:dateCreated ?date
          |}""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => datasets.DateCreated(Instant.parse(row("date"))))
    .toSet
}
