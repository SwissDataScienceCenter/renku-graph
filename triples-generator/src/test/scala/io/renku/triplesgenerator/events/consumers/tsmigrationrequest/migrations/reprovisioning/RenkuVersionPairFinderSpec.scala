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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest._
import matchers._
import org.scalatest.wordspec.AsyncWordSpec

class RenkuVersionPairFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with should.Matchers
    with BeforeAndAfterEach {

  private implicit lazy val renkuUrl: RenkuUrl = renkuUrls.generateOne

  "find" should {

    "return the Version pair in the triples store" in migrationsDSConfig.use { implicit mcc =>
      for {
        _ <- uploadToMigrations(currentVersionPair)

        _ <- versionPairFinder.find().asserting(_ shouldBe currentVersionPair.some)

        _ <- logger.loggedOnlyF(
               Warn(s"re-provisioning - version pair find finished${executionTimeRecorder.executionTimeInfo}")
             )
      } yield Succeeded
    }

    "return None if there are no Version pair in the triple store" in migrationsDSConfig.use { implicit mcc =>
      versionPairFinder.find().asserting(_ shouldBe None) >>
        logger.loggedOnlyF(
          Warn(s"re-provisioning - version pair find finished${executionTimeRecorder.executionTimeInfo}")
        )
    }

    "return an IllegalStateException if there are multiple Version Pairs" in migrationsDSConfig.use { implicit mcc =>
      uploadToMigrations(currentVersionPair, renkuVersionPairs.generateOne) >>
        versionPairFinder
          .find()
          .assertThrowsError[IllegalStateException](_.getMessage should startWith("Too many Version pairs found:"))
    }
  }

  private lazy val currentVersionPair = renkuVersionPairs.generateOne

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
  private implicit val tr: SparqlQueryTimeRecorder[IO] = new SparqlQueryTimeRecorder[IO](executionTimeRecorder)
  private def versionPairFinder(implicit mcc: MigrationsConnectionConfig) = new RenkuVersionPairFinderImpl[IO](mcc)

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    logger.reset()
  }
}
