/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectsgraph

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class MigrationStartTimeFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset {

  "findMigrationStartDate" should {

    "create a new migration start date if there's no one in the TS " +
      "and keep returning the same date each time called" in new TestCase {

        findStartTime shouldBe None

        val startTime = finder.findMigrationStartDate.unsafeRunSync()

        findStartTime shouldBe Some(startTime)

        startTime compareTo Instant.now() should be <= 0

        Thread.sleep(1000)

        finder.findMigrationStartDate.unsafeRunSync() shouldBe startTime

        findStartTime shouldBe Some(startTime)
      }
  }

  private trait TestCase {
    private implicit val renkuUrl:     RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new MigrationStartTimeFinderImpl[IO](TSClient(migrationsDSConnectionInfo))

    def findStartTime =
      runSelect(
        on = migrationsDataset,
        SparqlQuery.of(
          "test V10 start time",
          Prefixes.of(renku -> "renku"),
          s"""|SELECT ?time
              |WHERE {
              |  ${ProvisionProjectsGraph.name.asEntityId.asSparql.sparql} renku:startTime ?time
              |}
              |""".stripMargin
        )
      ).unsafeRunSync().headOption.flatMap(_.get("time").map(Instant.parse))
  }
}
