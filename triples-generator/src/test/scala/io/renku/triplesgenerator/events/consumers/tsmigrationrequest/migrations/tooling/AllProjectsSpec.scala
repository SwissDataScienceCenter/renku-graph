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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.Project
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.AllProjects.ProjectMetadata
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class AllProjectsSpec extends AsyncWordSpec with AsyncIOSpec with GraphJenaSpec with should.Matchers {

  implicit val renkuUrl:  RenkuUrl     = EntitiesGenerators.renkuUrl
  implicit val gitlabUrl: GitLabApiUrl = EntitiesGenerators.gitLabApiUrl
  implicit val logger:    Logger[IO]   = TestLogger[IO]()
  implicit lazy val timeRecorder: SparqlQueryTimeRecorder[IO] =
    new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())

  private def allProjects(implicit pcc: ProjectsConnectionConfig): AllProjects[IO] = AllProjects[IO](pcc)

  "find" should {
    "find all projects" in projectsDSConfig.use { implicit pcc =>
      val projectGen = EntitiesGenerators.anyProjectEntities
      val projects   = projectGen.generateFixedSizeList(32)

      for {
        _       <- uploadToProjects(projects.map(a => a: Project): _*)
        results <- allProjects.findAll(10).compile.toList
      } yield results shouldBe projects.map(p => ProjectMetadata(p.slug)).sortBy(_.slug.value)
    }
  }
}
