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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class ProjectDonePersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset
    with OptionValues {

  "noteDone" should {

    "remove the (ProvisionProjectsGraph, renku:toBeMigrated, path) triple" in new TestCase {

      val paths       = projectPaths.generateList(min = 2)
      val insertQuery = BacklogCreator.asToBeMigratedInserts.apply(paths).value

      runUpdate(on = migrationsDataset, insertQuery).unsafeRunSync()

      progressFinder.findProgressInfo.unsafeRunSync() shouldBe s"${paths.size} left from ${paths.size}"

      donePersister.noteDone(Random.shuffle(paths).head).unsafeRunSync()

      progressFinder.findProgressInfo.unsafeRunSync() shouldBe s"${paths.size - 1} left from ${paths.size}"
    }
  }

  private trait TestCase {
    implicit val renkuUrl:       RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val tr:     SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](migrationsDSConnectionInfo)
    val progressFinder   = new ProgressFinderImpl[IO](tsClient)
    val donePersister    = new ProjectDonePersisterImpl[IO](tsClient)
  }
}