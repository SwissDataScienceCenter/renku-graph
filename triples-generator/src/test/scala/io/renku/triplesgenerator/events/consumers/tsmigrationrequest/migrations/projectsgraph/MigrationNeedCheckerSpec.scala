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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesstore._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class MigrationNeedCheckerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  "checkMigrationNeeded" should {

    "return Yes if there are projects without a counterpart in the Projects graph" in projectsDSConfig.use {
      implicit pcc =>
        val projectInTwoGraphs = anyProjectEntities.generateOne
        val projectInOneGraph  = anyProjectEntities.generateOne

        for {
          _ <- provisionTestProject(projectInTwoGraphs)

          _ <- uploadToProjects(projectInOneGraph)

          _ <- checker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.Yes])
        } yield Succeeded
    }

    "return No if there are only projects in both graphs" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne

      provisionTestProject(project) >>
        checker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.No])
    }
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()

  private def checker(implicit pcc: ProjectsConnectionConfig) = new MigrationNeedCheckerImpl[IO](tsClient)
}
