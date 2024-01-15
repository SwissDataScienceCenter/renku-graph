/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Temporal}
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.duration._

class MigrationStartTimeFinderSpec extends AsyncWordSpec with AsyncIOSpec with GraphJenaSpec with should.Matchers {

  "findMigrationStartDate" should {

    "create a new migration start date if there's no one in the TS " +
      "and keep returning the same date each time called" in migrationsDSConfig.use { implicit mcc =>
        for {
          _ <- findStartTime.asserting(_ shouldBe None)

          startTime <- finder.findMigrationStartDate

          _ <- findStartTime.asserting(_ shouldBe Some(startTime))

          _ = startTime compareTo Instant.now() should be <= 0

          _ <- Temporal[IO].sleep(1000 millis)

          _ <- finder.findMigrationStartDate.asserting(_ shouldBe startTime)

          _ <- findStartTime.asserting(_ shouldBe Some(startTime))
        } yield Succeeded
      }
  }

  private implicit val renkuUrl:    RenkuUrl   = renkuUrls.generateOne
  private implicit lazy val logger: Logger[IO] = TestLogger[IO]()
  private def finder(implicit mcc: MigrationsConnectionConfig) = new MigrationStartTimeFinderImpl[IO](tsClient)

  private def findStartTime(implicit mcc: MigrationsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "test V10 start time",
        Prefixes.of(renku -> "renku"),
        s"""|SELECT ?time
            |WHERE {
            |  ${MigrationToV10.name.asEntityId.asSparql.sparql} renku:startTime ?time
            |}
            |""".stripMargin
      )
    ).map(_.headOption.flatMap(_.get("time").map(Instant.parse)))
}
