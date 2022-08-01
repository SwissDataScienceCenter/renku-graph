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
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetTopmostSameAs
import io.renku.graph.model.datasets
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.UpdateQueryMigration

class MultipleTopmostSameAsOnInternalDSSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset {

  "query" should {

    "find all Internal datasets having multiple topmostSameAs " +
      "while one of them points to the DS it belongs to (it's correct) " +
      "and remove all the excessive ones" in {
        val (ds, dsProject) = renkuProjectEntities(anyVisibility)
          .addDataset(datasetEntities(provenanceInternal))
          .generateOne

        upload(to = renkuDataset, dsProject)

        val illegalTopmost1 = datasetTopmostSameAs.generateOne
        insert(to = renkuDataset, Triple.edge(ds.entityId, renku / "topmostSameAs", illegalTopmost1))
        val illegalTopmost2 = datasetTopmostSameAs.generateOne
        insert(to = renkuDataset, Triple.edge(ds.entityId, renku / "topmostSameAs", illegalTopmost2))

        findTopmostSameAs(ds.identification.identifier) shouldBe Set(illegalTopmost1,
                                                                     illegalTopmost2,
                                                                     ds.provenance.topmostSameAs
        )

        runUpdate(on = renkuDataset, MultipleTopmostSameAsOnInternalDS.query).unsafeRunSync() shouldBe ()

        findTopmostSameAs(ds.identification.identifier) shouldBe Set(TopmostSameAs(ds.entityId))
      }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      MultipleTopmostSameAsOnInternalDS[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findTopmostSameAs(id: datasets.Identifier): Set[datasets.TopmostSameAs] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch topmostSameAs",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?topmostSameAs
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      renku:topmostSameAs ?topmostSameAs
            |}""".stripMargin
      )
    )
      .unsafeRunSync()
      .map(row => datasets.TopmostSameAs(row("topmostSameAs")))
      .toSet
}
