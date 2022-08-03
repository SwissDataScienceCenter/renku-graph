/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projectdetails

import Converters._
import KGProjectFinder.KGProject
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQueryTimeRecorder}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with ScalaCheckPropertyChecks
    with IOSpec {

  "findProject" should {

    "return details of the project with the given path when there's no parent" in new TestCase {
      forAll(Gen.oneOf(renkuProjectEntities(anyVisibility), nonRenkuProjectEntities(anyVisibility))) { project =>
        upload(to = renkuDataset, anyProjectEntities.generateOne, project)

        kgProjectFinder.findProject(project.path, authUsers.generateOption).unsafeRunSync() shouldBe
          project.to[KGProject].some
      }
    }

    "return details of the project with the given path if it has a parent project - public projects" in new TestCase {
      forAll(
        Gen.oneOf(renkuProjectWithParentEntities(visibilityPublic), nonRenkuProjectWithParentEntities(visibilityPublic))
      ) { project =>
        upload(to = renkuDataset, project, project.parent)

        kgProjectFinder.findProject(project.path, authUsers.generateOption).unsafeRunSync() shouldBe
          project.to[KGProject].some
      }
    }

    "return details of the project with the given path if it has a parent project - non-public projects" in new TestCase {
      val user         = authUsers.generateOne
      val userAsMember = personEntities.generateOne.copy(maybeGitLabId = user.id.some)
      val project = Gen
        .oneOf(
          renkuProjectWithParentEntities(visibilityNonPublic).map(_.copy(members = Set(userAsMember))),
          nonRenkuProjectWithParentEntities(visibilityNonPublic).map(_.copy(members = Set(userAsMember)))
        )
        .generateOne

      val parent = replaceMembers(Set(userAsMember))(project.parent)

      upload(to = renkuDataset, project, parent)

      kgProjectFinder.findProject(project.path, user.some).unsafeRunSync() shouldBe
        project.to[KGProject].some
    }

    "return details of the project with the given path without info about the parent " +
      "if the user has no rights to access the parent project" in new TestCase {
        val user = authUsers.generateOne
        val project = Gen
          .oneOf(
            renkuProjectWithParentEntities(visibilityNonPublic).map(
              _.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some)))
            ),
            nonRenkuProjectWithParentEntities(visibilityNonPublic).map(
              _.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some)))
            )
          )
          .generateOne

        val parent = replaceMembers(Set.empty)(project.parent)

        upload(to = renkuDataset, project, parent)

        kgProjectFinder.findProject(project.path, Some(user)).unsafeRunSync() shouldBe Some {
          project.to[KGProject].copy(maybeParent = None)
        }
      }

    "return None if there's no project with the given path" in new TestCase {
      kgProjectFinder.findProject(projectPaths.generateOne, authUsers.generateOption).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val kgProjectFinder = new KGProjectFinderImpl[IO](renkuDSConnectionInfo)
  }

  private def replaceMembers(members: Set[Person]): Project => Project = {
    case p: RenkuProject.WithParent       => p.copy(members = members)
    case p: RenkuProject.WithoutParent    => p.copy(members = members)
    case p: NonRenkuProject.WithParent    => p.copy(members = members)
    case p: NonRenkuProject.WithoutParent => p.copy(members = members)
  }
}
