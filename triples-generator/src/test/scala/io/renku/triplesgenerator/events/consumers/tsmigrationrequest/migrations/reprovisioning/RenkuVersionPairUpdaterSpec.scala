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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import Schemas.renku
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.graph.model.versions.{CliVersion, RenkuVersionPair, SchemaVersion}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.generators.VersionGenerators._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.Succeeded
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class RenkuVersionPairUpdaterSpec extends AsyncWordSpec with AsyncIOSpec with GraphJenaSpec with Matchers {

  "update" should {
    "create a renku:VersionPair with the given version pair" in migrationsDSConfig.use { implicit mcc =>
      for {
        _ <- findPairInDb.asserting(_ shouldBe Set.empty)

        _ <- renkuVersionPairUpdater.update(currentRenkuVersionPair)

        _ <- findPairInDb.asserting(_ shouldBe Set(currentRenkuVersionPair))

        _ <- renkuVersionPairUpdater.update(newVersionCompatibilityPairs)

        _ <- findPairInDb.asserting(_ shouldBe Set(newVersionCompatibilityPairs))
      } yield Succeeded
    }
  }

  val currentRenkuVersionPair      = renkuVersionPairs.generateOne
  val newVersionCompatibilityPairs = renkuVersionPairs.generateOne

  private implicit val renkuUrl:    RenkuUrl       = renkuUrls.generateOne
  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def renkuVersionPairUpdater(implicit mcc: MigrationsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new RenkuVersionPairUpdaterImpl[IO](mcc)
  }

  private def findPairInDb(implicit mcc: MigrationsConnectionConfig): IO[Set[RenkuVersionPair]] =
    runSelect(
      SparqlQuery.of(
        "fetch version pair info",
        Prefixes of renku -> "renku",
        s"""|SELECT DISTINCT ?schemaVersion ?cliVersion
            |WHERE {
            |   ?id a renku:VersionPair;
            |         renku:schemaVersion ?schemaVersion ;
            |         renku:cliVersion ?cliVersion.
            |}
            |""".stripMargin
      )
    ).map(_.map(row => RenkuVersionPair(CliVersion(row("cliVersion")), SchemaVersion(row("schemaVersion")))).toSet)
}
