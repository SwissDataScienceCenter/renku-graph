/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.rest

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.projects.rest.Converters._
import io.renku.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "findProject" should {

    "return details of the project with the given path when there's no parent" in new TestCase {
      forAll(projectEntities(anyVisibility)) { project =>
        loadToStore(anyProjectEntities.generateOne, project)

        metadataFinder.findProject(project.path, authUsers.generateOption).unsafeRunSync() shouldBe
          project.to[KGProject].some
      }
    }

    "return details of the project with the given path if it has a parent project - public projects" in new TestCase {
      forAll(projectWithParentEntities(visibilityPublic)) { project =>
        loadToStore(project, project.parent)

        metadataFinder.findProject(project.path, authUsers.generateOption).unsafeRunSync() shouldBe
          project.to[KGProject].some
      }
    }

    "return details of the project with the given path if it has a parent project - non-public projects" in new TestCase {
      val user         = authUsers.generateOne
      val userAsMember = personEntities.generateOne.copy(maybeGitLabId = user.id.some)
      val project      = projectWithParentEntities(visibilityNonPublic).generateOne.copy(members = Set(userAsMember))

      val parent: Project = project.parent match {
        case p: ProjectWithParent    => p.copy(members = Set(userAsMember))
        case p: ProjectWithoutParent => p.copy(members = Set(userAsMember))
      }

      loadToStore(project, parent)

      metadataFinder.findProject(project.path, user.some).unsafeRunSync() shouldBe
        project.to[KGProject].some
    }

    "return details of the project with the given path without info about the parent " +
      "if the user has no rights to access the parent project" in new TestCase {
        val user = authUsers.generateOne
        val project = projectWithParentEntities(visibilityNonPublic).generateOne.copy(
          members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))
        )

        val parent: Project = project.parent match {
          case p: ProjectWithParent    => p.copy(members = Set.empty)
          case p: ProjectWithoutParent => p.copy(members = Set.empty)
        }

        loadToStore(project, parent)

        metadataFinder.findProject(project.path, Some(user)).unsafeRunSync() shouldBe Some {
          project.to[KGProject].copy(maybeParent = None)
        }
      }

    "return None if there's no project with the given path" in new TestCase {
      metadataFinder.findProject(projectPaths.generateOne, authUsers.generateOption).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val metadataFinder       = new KGProjectFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }
}
