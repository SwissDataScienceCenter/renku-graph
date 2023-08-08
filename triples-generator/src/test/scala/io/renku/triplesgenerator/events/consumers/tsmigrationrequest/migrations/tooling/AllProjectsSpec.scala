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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.Project
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.AllProjects.ProjectMetadata
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class AllProjectsSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with should.Matchers {

  implicit val renkuUrl:  RenkuUrl     = EntitiesGenerators.renkuUrl
  implicit val gitlabUrl: GitLabApiUrl = EntitiesGenerators.gitLabApiUrl
  implicit val logger:    Logger[IO]   = TestLogger[IO]()
  implicit lazy val timeRecorder: SparqlQueryTimeRecorder[IO] =
    new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())

  def allProjects: AllProjects[IO] = AllProjects[IO](projectsDSConnectionInfo)

  "find" should {
    "find all projects" in {
      val projectGen = EntitiesGenerators.anyProjectEntities
      val projects   = projectGen.generateFixedSizeList(32)

      upload(to = projectsDataset, projects.map(a => a: Project): _*)
      val result = allProjects.findAll(10).compile.toList.unsafeRunSync()

      result shouldBe projects.map(p => ProjectMetadata(p.slug)).sortBy(_.slug.value)
    }
  }
}
