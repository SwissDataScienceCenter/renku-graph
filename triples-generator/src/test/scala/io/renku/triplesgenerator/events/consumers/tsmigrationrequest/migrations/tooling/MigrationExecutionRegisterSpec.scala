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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations
package tooling

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class MigrationExecutionRegisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with should.Matchers {

  private val migrationName  = migrationNames.generateOne
  private val serviceVersion = serviceVersions.generateOne

  "MigrationExecutionRegister" should {

    "return true if asked for migration having execution registered" in migrationsDSConfig.use { implicit mcc =>
      register.registerExecution(migrationName).assertNoException >>
        register.findExecution(migrationName).asserting(_ shouldBe serviceVersion.some)
    }

    "return false if asked for migration that wasn't executed" in migrationsDSConfig.use { implicit mcc =>
      register.findExecution(migrationName).asserting(_ shouldBe None)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def register(implicit mcc: MigrationsConnectionConfig) = {
    implicit val renkuUrl: RenkuUrl                    = renkuUrls.generateOne
    implicit val tr:       SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new MigrationExecutionRegisterImpl[IO](serviceVersion, mcc)
  }
}
