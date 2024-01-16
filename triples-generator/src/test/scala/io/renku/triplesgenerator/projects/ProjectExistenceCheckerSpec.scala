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

package io.renku.triplesgenerator.projects

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

class ProjectExistenceCheckerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with OptionValues {

  it should "return true if project exists in the TS" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities.generateOne.to[entities.Project]

    provisionProject(project).assertNoException >>
      checker.checkExists(project.slug).asserting(_ shouldBe true)
  }

  it should "return false if project does not exist in the TS" in projectsDSConfig.use { implicit pcc =>
    checker.checkExists(projectSlugs.generateOne).asserting(_ shouldBe false)
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()
  private def checker(implicit pcc: ProjectsConnectionConfig) = new ProjectExistenceCheckerImpl[IO](tsClient)
}
