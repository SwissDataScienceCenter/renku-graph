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

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectPathToPath
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.membersync.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MembersSynchronizerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "synchronizeMembers" should {

    "pulls members from GitLab and sync the members in the TS" in new TestCase {

      val membersInGitLab = gitLabProjectMembers.generateSet()

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: projects.Path)(_: projects.Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[IO])

      (gitLabProjectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(membersInGitLab.pure[IO])

      val syncSummary = syncSummaries.generateOne
      (kgSynchronizer.syncMembers _)
        .expects(projectPath, membersInGitLab)
        .returning(syncSummary.pure[IO])

      synchronizer.synchronizeMembers(projectPath).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(show"$categoryName: $projectPath accepted"),
        infoMessage(syncSummary)
      )
    }

    "recover with log statement if collaborator fails" in new TestCase {

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: projects.Path)(_: projects.Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[IO])

      val exception = exceptions.generateOne
      (gitLabProjectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(exception.raiseError[IO, Set[GitLabProjectMember]])

      synchronizer.synchronizeMembers(projectPath).unsafeRunSync() shouldBe ()

      logger.logged(Error(s"$categoryName: Members synchronized for project $projectPath failed", exception))
    }
  }

  private trait TestCase {

    val projectPath = projectPaths.generateOne

    implicit val logger:            TestLogger[IO]        = TestLogger[IO]()
    implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
    val gitLabProjectMembersFinder = mock[GitLabProjectMembersFinder[IO]]
    val kgSynchronizer             = mock[KGSynchronizer[IO]]
    val executionTimeRecorder      = TestExecutionTimeRecorder[IO](maybeHistogram = None)
    val synchronizer =
      new MembersSynchronizerImpl[IO](gitLabProjectMembersFinder, kgSynchronizer, executionTimeRecorder)

    def infoMessage(syncSummary: SyncSummary) = Info(
      s"$categoryName: members for project: $projectPath synchronized in ${executionTimeRecorder.elapsedTime}ms: " +
        s"${syncSummary.membersAdded} member(s) added, ${syncSummary.membersRemoved} member(s) removed"
    )
  }
}
