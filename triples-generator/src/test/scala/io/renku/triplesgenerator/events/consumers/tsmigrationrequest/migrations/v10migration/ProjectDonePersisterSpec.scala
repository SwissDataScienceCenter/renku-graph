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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectDonePersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset {

  "noteDone" should {

    "persist the (MigrationToV10, renku:migrated, path) triple" in new TestCase {

      val path = projectPaths.generateOne

      migratedProjectsChecker.filterNotMigrated(List(path)).unsafeRunSync() shouldBe List(path)

      persister.noteDone(path).unsafeRunSync()

      migratedProjectsChecker.filterNotMigrated(List(path)).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    implicit val renkuUrl:       RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val tr:     SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](migrationsDSConnectionInfo)
    val persister        = new ProjectDonePersisterImpl[IO](tsClient)

    val migratedProjectsChecker = new MigratedProjectsCheckerImpl[IO](tsClient)
  }
}
