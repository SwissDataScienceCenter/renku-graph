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

package io.renku.triplesgenerator.events.consumers.minprojectinfo

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesstore._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectExistenceCheckerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers {

  "checkProjectExists" should {

    "return false if project does not exist in the TS; true otherwise" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne

      for {
        _ <- checker.checkProjectExists(project.resourceId).asserting(_ shouldBe false)

        _ <- uploadToProjects(project)

        _ <- checker.checkProjectExists(project.resourceId).asserting(_ shouldBe true)
      } yield Succeeded
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private def checker(implicit pcc: ProjectsConnectionConfig) = new ProjectExistenceCheckerImpl[IO](tsClient)
}
