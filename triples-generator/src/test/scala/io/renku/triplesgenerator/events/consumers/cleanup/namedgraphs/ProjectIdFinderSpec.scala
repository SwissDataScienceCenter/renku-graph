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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectIdFinderSpec extends AsyncWordSpec with AsyncIOSpec with TriplesGeneratorJenaSpec with should.Matchers {

  "findProjectId" should {

    "return ProjectIdentification object found for the given Project Slug" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      uploadToProjects(project) >>
        finder.findProjectId(project.slug).asserting(_ shouldBe Some(project.identification))
    }

    "return None when there's no Project with the given Slug" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      uploadToProjects(project) >>
        finder.findProjectId(projectSlugs.generateOne).asserting(_ shouldBe None)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def finder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new ProjectIdFinderImpl[IO](pcc)
  }
}
