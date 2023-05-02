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

package io.renku.graph.http.server.security

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.AccessToken
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabPathRecordsFinderSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "apply" should {

    "return SecurityRecord with project visibility and all project members" in new TestCase {

      val visibility = projectVisibilities.generateOne
      givenFindingVisibility(returning = visibility.some.pure[IO])

      val members = personGitLabIds.generateSet()
      givenFindingMembers(returning = members.pure[IO])

      recordsFinder(projectPath, maybeAuthUser).unsafeRunSync() shouldBe List(
        Authorizer.SecurityRecord(visibility, projectPath, members)
      )
    }

    "return no Records if no visibility found" in new TestCase {

      givenFindingVisibility(returning = None.pure[IO])

      val members = personGitLabIds.generateSet()
      givenFindingMembers(returning = members.pure[IO])

      recordsFinder(projectPath, maybeAuthUser).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {

    val projectPath   = projectPaths.generateOne
    val maybeAuthUser = authUsers.generateOption

    implicit val visibilityFinder: VisibilityFinder[IO] = mock[VisibilityFinder[IO]]
    implicit val membersFinder:    MembersFinder[IO]    = mock[MembersFinder[IO]]
    val recordsFinder = new GitLabPathRecordsFinderImpl[IO](visibilityFinder, membersFinder)

    def givenFindingVisibility(returning: IO[Option[projects.Visibility]]) =
      (visibilityFinder
        .findVisibility(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAuthUser.map(_.accessToken))
        .returning(returning)

    def givenFindingMembers(returning: IO[Set[persons.GitLabId]]) =
      (membersFinder
        .findMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAuthUser.map(_.accessToken))
        .returning(returning)
  }
}
