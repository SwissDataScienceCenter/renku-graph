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
package tooling

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder, TriplesStoreConfig}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MigrationExecutionRegisterSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "MigrationExecutionRegister" should {

    "return true if asked for migration having execution registered" in new TestCase {
      register.registerExecution(migrationName).unsafeRunSync() shouldBe ()

      register.findExecution(migrationName).unsafeRunSync() shouldBe serviceVersion.some
    }

    "return false if asked for migration that wasn't executed" in new TestCase {
      register.findExecution(migrationName).unsafeRunSync() shouldBe None
    }
  }

  override lazy val storeConfig: TriplesStoreConfig = migrationsStoreConfig

  private trait TestCase {
    val migrationName  = migrationNames.generateOne
    val serviceVersion = serviceVersions.generateOne

    private implicit val renkuUrl:     RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val register = new MigrationExecutionRegisterImpl[IO](serviceVersion, migrationsStoreConfig)
  }
}
