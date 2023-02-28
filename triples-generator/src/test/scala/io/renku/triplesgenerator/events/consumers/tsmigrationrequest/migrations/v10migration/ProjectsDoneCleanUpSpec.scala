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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.MigrationExecutionRegisterImpl
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectsDoneCleanUpSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset {

  "cleanUp" should {

    "remove all and only the v10 migration's renku:migrated values" in new TestCase {

      executionRegister.registerExecution(MigrationToV10.name).unsafeRunSync()

      val paths = projectPaths.generateNonEmptyList().toList

      paths.map(projectDonePersister.noteDone).sequence.unsafeRunSync()

      migratedProjectsChecker.filterNotMigrated(paths).unsafeRunSync() shouldBe Nil

      cleaner.cleanUp().unsafeRunSync() shouldBe ()

      migratedProjectsChecker.filterNotMigrated(paths).unsafeRunSync() shouldBe paths

      executionRegister.findExecution(MigrationToV10.name).unsafeRunSync() shouldBe a[Some[_]]
    }
  }

  private trait TestCase {
    implicit val renkuUrl:       RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val tr:     SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](migrationsDSConnectionInfo)
    val cleaner          = new ProjectsDoneCleanUpImpl[IO](tsClient)

    val projectDonePersister    = new ProjectDonePersisterImpl[IO](tsClient)
    val migratedProjectsChecker = new MigratedProjectsCheckerImpl[IO](tsClient)

    val executionRegister =
      new MigrationExecutionRegisterImpl[IO](serviceVersions.generateOne, migrationsDSConnectionInfo)
  }
}
