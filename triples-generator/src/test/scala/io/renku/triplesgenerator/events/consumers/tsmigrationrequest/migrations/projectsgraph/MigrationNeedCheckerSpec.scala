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
package migrations.projectsgraph

import cats.effect.IO
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class MigrationNeedCheckerSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with MockFactory {

  "checkMigrationNeeded" should {

    "return Yes if there are projects without a counterpart in the Projects graph" in new TestCase {

      val projectInTwoGraphs = anyProjectEntities.generateOne
      val projectInOneGraph  = anyProjectEntities.generateOne

      provisionTestProject(projectInTwoGraphs).unsafeRunSync()

      upload(to = projectsDataset, projectInOneGraph)

      checker.checkMigrationNeeded.unsafeRunSync() shouldBe a[ConditionedMigration.MigrationRequired.Yes]
    }

    "return No if there are only projects in both graphs" in new TestCase {

      val project = anyProjectEntities.generateOne

      provisionTestProject(project).unsafeRunSync()

      checker.checkMigrationNeeded.unsafeRunSync() shouldBe a[ConditionedMigration.MigrationRequired.No]
    }
  }

  private implicit val logger:    TestLogger[IO] = TestLogger[IO]()
  implicit override val ioLogger: Logger[IO]     = logger

  private trait TestCase {
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val checker = new MigrationNeedCheckerImpl[IO](TSClient[IO](projectsDSConnectionInfo))
  }
}
