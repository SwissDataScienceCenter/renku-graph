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

package io.renku.triplesgenerator.events.consumers.membersync

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectSlugToPath
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
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

  it should "pulls members and visibility from GitLab and sync the members in the TS" in {

    val projectSlug = projectSlugs.generateOne

    val maybeAccessToken = accessTokens.generateOption
    givenAccessTokenFinding(projectSlug, returning = maybeAccessToken.pure[IO])

    val membersInGitLab = gitLabProjectMembers.generateSet()
    givenProjectMembersFinding(projectSlug, maybeAccessToken, returning = membersInGitLab.pure[IO])

    val maybeVisibility = projectVisibilities.generateOption
    givenProjectVisibilityFinding(projectSlug, maybeAccessToken, returning = maybeVisibility.pure[IO])

    val syncSummary = syncSummaries.generateOne
    givenMemberSynchro(projectSlug, membersInGitLab, maybeVisibility, returning = syncSummary.pure[IO])

    synchronizer.synchronizeMembers(projectSlug).assertNoException >>
      IO(logger.loggedOnly(Info(show"$categoryName: $projectSlug accepted"), infoMessage(projectSlug, syncSummary)))
  }

  it should "recover with log statement if collaborator fails" in {

    val projectSlug = projectSlugs.generateOne

    val maybeAccessToken = accessTokens.generateOption
    givenAccessTokenFinding(projectSlug, returning = maybeAccessToken.pure[IO])

    val exception = exceptions.generateOne
    givenProjectMembersFinding(projectSlug, maybeAccessToken, returning = exception.raiseError[IO, Nothing])

    synchronizer.synchronizeMembers(projectSlug).assertNoException >>
      IO(logger.logged(Error(s"$categoryName: Members synchronized for project $projectSlug failed", exception)))
  }

  private implicit lazy val logger:       TestLogger[IO]        = TestLogger[IO]()
  private implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
  private val glProjectMembersFinder    = mock[GLProjectMembersFinder[IO]]
  private val glProjectVisibilityFinder = mock[GLProjectVisibilityFinder[IO]]
  private val kgSynchronizer            = mock[KGSynchronizer[IO]]
  private val executionTimeRecorder     = TestExecutionTimeRecorder[IO](maybeHistogram = None)
  private lazy val synchronizer = new MembersSynchronizerImpl[IO](glProjectMembersFinder,
                                                                  glProjectVisibilityFinder,
                                                                  kgSynchronizer,
                                                                  executionTimeRecorder
  )

  private def infoMessage(projectSlug: projects.Slug, syncSummary: SyncSummary) = Info(
    s"$categoryName: members for project: $projectSlug synchronized in ${executionTimeRecorder.elapsedTime}ms: " +
      s"${syncSummary.membersAdded} member(s) added, ${syncSummary.membersRemoved} member(s) removed"
  )

  private def givenAccessTokenFinding(projectSlug: projects.Slug, returning: IO[Option[AccessToken]]) =
    (accessTokenFinder
      .findAccessToken(_: projects.Slug)(_: projects.Slug => String))
      .expects(projectSlug, projectSlugToPath)
      .returning(returning)

  private def givenProjectMembersFinding(projectSlug: projects.Slug,
                                         mat:         Option[AccessToken],
                                         returning:   IO[Set[GitLabProjectMember]]
  ) = (glProjectMembersFinder
    .findProjectMembers(_: projects.Slug)(_: Option[AccessToken]))
    .expects(projectSlug, mat)
    .returning(returning)

  private def givenProjectVisibilityFinding(projectSlug: projects.Slug,
                                            mat:         Option[AccessToken],
                                            returning:   IO[Option[projects.Visibility]]
  ) = (glProjectVisibilityFinder
    .findVisibility(_: projects.Slug)(_: Option[AccessToken]))
    .expects(projectSlug, mat)
    .returning(returning)

  private def givenMemberSynchro(projectSlug:     projects.Slug,
                                 membersInGL:     Set[GitLabProjectMember],
                                 maybeVisibility: Option[projects.Visibility],
                                 returning:       IO[SyncSummary]
  ) = (kgSynchronizer.syncMembers _)
    .expects(projectSlug, membersInGL, maybeVisibility)
    .returning(returning)

  protected override def beforeEach() = logger.reset()
}
