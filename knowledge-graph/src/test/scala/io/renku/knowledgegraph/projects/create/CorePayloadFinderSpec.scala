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

package io.renku.knowledgegraph.projects.create

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.core.client.Generators.userInfos
import io.renku.core.client.{ProjectRepository, UserInfo, NewProject => CoreNewProject}
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.http.client.GitLabGenerators.gitLabUrls
import io.renku.http.client.UserAccessToken
import io.renku.knowledgegraph.gitlab.UserInfoFinder
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class CorePayloadFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with EitherValues
    with AsyncMockFactory {

  it should "find the namespace details, " +
    "find user info in GL and " +
    "merge the data into a CoreNewProject" in {

      val newProject = newProjects.generateOne
      val authUser   = authUsers.generateOne

      val enrichedNamespace = namespacesWithName(from = newProject.namespace).generateOne
      givenNamespaceFinding(newProject.namespace, authUser.accessToken, returning = enrichedNamespace.some.pure[IO])

      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])

      val glCreatedProject = glCreatedProjectsGen.generateOne

      corePayloadFinder
        .findCorePayload(newProject, glCreatedProject, authUser)
        .asserting(
          _ shouldBe CoreNewProject(
            ProjectRepository.of(glUrl),
            enrichedNamespace.name,
            glCreatedProject.slug.toPath.asName,
            newProject.maybeDescription,
            newProject.keywords,
            newProject.template,
            newProject.branch,
            userInfo
          )
        )
    }

  it should "fail finding namespace fails" in {

    val newProject = newProjects.generateOne
    val authUser   = authUsers.generateOne

    val failure = exceptions.generateOne
    givenNamespaceFinding(newProject.namespace, authUser.accessToken, returning = failure.raiseError[IO, Nothing])

    val userInfo = userInfos.generateOne
    givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])

    corePayloadFinder
      .findCorePayload(newProject, glCreatedProjectsGen.generateOne, authUser)
      .assertThrowsError[Exception](
        _ shouldBe CreationFailures.onFindingNamespace(newProject.namespace.identifier, failure)
      )
  }

  it should "fail if no namespace found" in {

    val newProject = newProjects.generateOne
    val authUser   = authUsers.generateOne

    givenNamespaceFinding(newProject.namespace, authUser.accessToken, returning = None.pure[IO])

    val userInfo = userInfos.generateOne
    givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])

    corePayloadFinder
      .findCorePayload(newProject, glCreatedProjectsGen.generateOne, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.noNamespaceFound(newProject.namespace))
  }

  it should "fail finding user info fails" in {

    val newProject = newProjects.generateOne
    val authUser   = authUsers.generateOne

    val enrichedNamespace = namespacesWithName(from = newProject.namespace).generateOne
    givenNamespaceFinding(newProject.namespace, authUser.accessToken, returning = enrichedNamespace.some.pure[IO])

    val failure = exceptions.generateOne
    givenUserInfoFinding(authUser.accessToken, returning = failure.raiseError[IO, Nothing])

    corePayloadFinder
      .findCorePayload(newProject, glCreatedProjectsGen.generateOne, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onFindingUserInfo(authUser.id, failure))
  }

  it should "fail if no user info found" in {

    val newProject = newProjects.generateOne
    val authUser   = authUsers.generateOne

    val enrichedNamespace = namespacesWithName(from = newProject.namespace).generateOne
    givenNamespaceFinding(newProject.namespace, authUser.accessToken, returning = enrichedNamespace.some.pure[IO])

    givenUserInfoFinding(authUser.accessToken, returning = None.pure[IO])

    corePayloadFinder
      .findCorePayload(newProject, glCreatedProjectsGen.generateOne, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.noUserInfoFound(authUser.id))
  }

  private lazy val glUrl             = gitLabUrls.generateOne
  private val namespaceFinder        = mock[NamespaceFinder[IO]]
  private val userInfoFinder         = mock[UserInfoFinder[IO]]
  private lazy val corePayloadFinder = new CorePayloadFinderImpl[IO](namespaceFinder, userInfoFinder, glUrl)

  private def givenNamespaceFinding(namespace:   Namespace,
                                    accessToken: UserAccessToken,
                                    returning:   IO[Option[Namespace.WithName]]
  ) = (namespaceFinder.findNamespace _)
    .expects(namespace.identifier, accessToken)
    .returning(returning)

  private def givenUserInfoFinding(at: UserAccessToken, returning: IO[Option[UserInfo]]) =
    (userInfoFinder.findUserInfo _)
      .expects(at)
      .returning(returning)
}
