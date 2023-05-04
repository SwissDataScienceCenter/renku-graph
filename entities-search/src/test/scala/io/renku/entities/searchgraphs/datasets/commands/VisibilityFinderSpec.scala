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

package io.renku.entities.searchgraphs.datasets.commands

import cats.effect.IO
import io.renku.entities.searchgraphs.datasets.commands.VisibilityFinderImpl
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class VisibilityFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with IOSpec {

  "findVisibility" should {

    "return Project's visibility for Project existing in the TS" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      upload(to = projectsDataset, project)

      finder.findVisibility(project.resourceId).unsafeRunSync() shouldBe Some(project.visibility)
    }

    "return None if project does not exist" in new TestCase {
      finder.findVisibility(projectResourceIds.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new VisibilityFinderImpl[IO](projectsDSConnectionInfo)
  }
}
