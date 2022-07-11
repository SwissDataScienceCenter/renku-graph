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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.datasetSameAs
import eu.timepit.refined.auto._
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MultipleDSSameAsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset
    with MockFactory {

  "query" should {

    "find DS records with multiple schema:sameAs and remove the ones that do not match the topmostSameAs" in {

      val (ds, dsProject) = renkuProjectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val (importedDS, importedDSProject) = renkuProjectEntities(anyVisibility)
        .importDataset(ds)
        .generateOne

      upload(to = renkuDataset, dsProject, importedDSProject)

      val additionalSameAsId = datasetSameAs.generateOne.entityId
      insert(to = renkuDataset, Triple.edge(importedDS.entityId, schema / "sameAs", additionalSameAsId))

      findSameAsIds(importedDS.identification.identifier) shouldBe Set(importedDS.provenance.sameAs.entityId,
                                                                       additionalSameAsId
      )
      findSameAsIds(ds.identification.identifier) shouldBe Set.empty

      runUpdate(on = renkuDataset, MultipleDSSameAs.query).unsafeRunSync() shouldBe ()

      findSameAsIds(importedDS.identification.identifier) shouldBe Set(importedDS.provenance.sameAs.entityId)
      findSameAsIds(ds.identification.identifier)         shouldBe Set.empty
    }
  }

  "apply" should {
    "return an UpdateQueryMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      MultipleDSSameAs[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findSameAsIds(id: datasets.Identifier): Set[EntityId] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?sameAs
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      schema:sameAs ?sameAs
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => EntityId.of(row("sameAs")))
      .toSet

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
