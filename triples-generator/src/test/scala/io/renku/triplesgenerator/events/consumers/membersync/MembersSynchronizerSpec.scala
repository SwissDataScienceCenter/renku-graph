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

package io.renku.triplesgenerator.events.consumers.membersync

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.projectauth.ProjectAuthData
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.tokenrepository.api.TokenRepositoryClient
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class MembersSynchronizerSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with BeforeAndAfterEach {

  it should "pulls members and visibility from GitLab and sync the Auth data " +
    "when project visibility is found" in {

      val projectSlug = projectSlugs.generateOne

      val accessToken = accessTokens.generateOne
      givenAccessTokenFinding(projectSlug, returning = accessToken.some.pure[IO])

      val membersInGitLab = gitLabProjectMembers.generateSet()
      givenProjectMembersFinding(projectSlug, accessToken, returning = membersInGitLab.pure[IO])

      val visibility = projectVisibilities.generateOne
      givenProjectVisibilityFinding(projectSlug, accessToken, returning = visibility.some.pure[IO])

      givenAuthDataUpdating(projectSlug, membersInGitLab, visibility, returning = ().pure[IO])

      synchronizer.synchronizeMembers(projectSlug).assertNoException
    }

  it should "pulls members and visibility from GitLab and remove the Auth data " +
    "when project visibility is found" in {

      val projectSlug = projectSlugs.generateOne

      val accessToken = accessTokens.generateOne
      givenAccessTokenFinding(projectSlug, returning = accessToken.some.pure[IO])

      val membersInGitLab = gitLabProjectMembers.generateSet()
      givenProjectMembersFinding(projectSlug, accessToken, returning = membersInGitLab.pure[IO])

      givenProjectVisibilityFinding(projectSlug, accessToken, returning = None.pure[IO])

      givenAuthDataRemoval(projectSlug, returning = ().pure[IO])

      synchronizer.synchronizeMembers(projectSlug).assertNoException
    }

  it should "remove the Auth data if no access token is found" in {

    val projectSlug = projectSlugs.generateOne

    givenAccessTokenFinding(projectSlug, returning = None.pure[IO])

    givenAuthDataRemoval(projectSlug, returning = ().pure[IO])

    synchronizer.synchronizeMembers(projectSlug).assertNoException
  }

  it should "recover with log statement if collaborator fails" in {

    val projectSlug = projectSlugs.generateOne

    val accessToken = accessTokens.generateOne
    givenAccessTokenFinding(projectSlug, returning = accessToken.some.pure[IO])

    val exception = exceptions.generateOne
    givenProjectMembersFinding(projectSlug, accessToken, returning = exception.raiseError[IO, Nothing])

    synchronizer.synchronizeMembers(projectSlug).assertNoException >>
      logger.loggedOnlyF(
        Info(s"$categoryName: $projectSlug accepted"),
        Error(s"$categoryName: Members synchronized for project $projectSlug failed", exception)
      )
  }

  private implicit lazy val logger: TestLogger[IO]            = TestLogger[IO]()
  private implicit val trClient:    TokenRepositoryClient[IO] = mock[TokenRepositoryClient[IO]]
  private val glProjectMembersFinder    = mock[GLProjectMembersFinder[IO]]
  private val glProjectVisibilityFinder = mock[GLProjectVisibilityFinder[IO]]
  private val projectAuthSync           = mock[ProjectAuthSync[IO]]
  private lazy val synchronizer =
    new MembersSynchronizerImpl[IO](trClient, glProjectMembersFinder, glProjectVisibilityFinder, projectAuthSync)

  private def givenAccessTokenFinding(projectSlug: projects.Slug, returning: IO[Option[AccessToken]]) =
    (trClient
      .findAccessToken(_: projects.Slug))
      .expects(projectSlug)
      .returning(returning)

  private def givenProjectMembersFinding(projectSlug: projects.Slug,
                                         at:          AccessToken,
                                         returning:   IO[Set[GitLabProjectMember]]
  ) = (glProjectMembersFinder
    .findProjectMembers(_: projects.Slug)(_: AccessToken))
    .expects(projectSlug, at)
    .returning(returning)

  private def givenProjectVisibilityFinding(projectSlug: projects.Slug,
                                            at:          AccessToken,
                                            returning:   IO[Option[projects.Visibility]]
  ) = (glProjectVisibilityFinder
    .findVisibility(_: projects.Slug)(_: AccessToken))
    .expects(projectSlug, at)
    .returning(returning)

  private def givenAuthDataUpdating(projectSlug:     projects.Slug,
                                    membersInGitLab: Set[GitLabProjectMember],
                                    visibility:      projects.Visibility,
                                    returning:       IO[Unit]
  ) = (projectAuthSync.syncProject _)
    .expects(ProjectAuthData(projectSlug, membersInGitLab.map(_.toProjectAuthMember), visibility))
    .returning(returning)

  private def givenAuthDataRemoval(projectSlug: projects.Slug, returning: IO[Unit]) =
    (projectAuthSync.removeAuthData _)
      .expects(projectSlug)
      .returning(returning)

  protected override def beforeEach() = logger.reset()
}
