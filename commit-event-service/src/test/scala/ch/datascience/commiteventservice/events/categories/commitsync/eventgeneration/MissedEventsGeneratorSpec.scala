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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration

import Generators.{commitInfos, projectInfos}
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.CommitToEventLog
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class MissedEventsGeneratorSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import IOAccessTokenFinder._

  "loadMissedEvents" should {

    "do nothing if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

        val commitSyncEvent = commitSyncEvents.generateOne

        givenLatestCommitAndLogEventMatch(commitSyncEvent)

        eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

        logger.logged(
          Info(s"${logMessageCommon(commitSyncEvent)} -> no new events found in ${executionTimeRecorder.elapsedTime}ms")
        )
      }

    "add missing events to the Event Log " +
      "when the latest eventId differs from the latest commit in GitLab" in new TestCase {
        val commitSyncEvent  = commitSyncEvents.generateOne
        val maybeAccessToken = Gen.option(accessTokens).generateOne

        givenAccessToken(commitSyncEvent.project.path, maybeAccessToken)
        val projectInfo = projectInfos.generateOne.copy(id = commitSyncEvent.project.id)

        val commitInfo = commitInfos.generateOne
        givenFetchLatestCommit(commitSyncEvent.project.id, maybeAccessToken)
          .returning(OptionT.some[IO](commitInfo))

        givenFindingProjectInfo(commitSyncEvent, maybeAccessToken)
          .returning(projectInfo.pure[IO])

        givenStoring(
          StartCommit(id = commitInfo.id, project = Project(projectInfo.id, projectInfo.path))
        ).returning(IO.unit)

        eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

        logger.logged(
          Info(s"${logMessageCommon(commitSyncEvent)} -> new events found in ${executionTimeRecorder.elapsedTime}ms")
        )
      }

    "do nothing if there are no commits in GitLab (e.g. project removed)" in new TestCase {
      val commitSyncEvent = commitSyncEvents.generateOne

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commitSyncEvent.project.path, maybeAccessToken1)
      givenFetchLatestCommit(commitSyncEvent.project.id, maybeAccessToken1)
        .returning(OptionT.none[IO, CommitInfo])

      eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

      logger.logged(
        Info(s"${logMessageCommon(commitSyncEvent)} -> no new events found in ${executionTimeRecorder.elapsedTime}ms")
      )
    }

    "not break processing if finding Access Token for one of the event(s) fails" in new TestCase {

      val commitSyncEvent = commitSyncEvents.generateOne

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(commitSyncEvent.project.path, projectPathToPath)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

      logger.logged(
        Error(
          s"${logMessageCommon(commitSyncEvent)} -> synchronization failed in ${executionTimeRecorder.elapsedTime}ms",
          exception
        )
      )
    }

    "not break processing if finding Project Info for one of the events fails" in new TestCase {
      val commitSyncEvent = commitSyncEvents.generateOne

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commitSyncEvent.project.path, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(commitSyncEvent.project.id, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val exception = exceptions.generateOne
      givenFindingProjectInfo(commitSyncEvent, maybeAccessToken1)
        .returning(exception.raiseError[IO, ProjectInfo])

      eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Error(
          s"${logMessageCommon(commitSyncEvent)} -> synchronization failed in ${executionTimeRecorder.elapsedTime}ms",
          exception
        )
      )
    }

    "not break processing if storing start Commit for one of the events fails" in new TestCase {
      val commitSyncEvent = commitSyncEvents.generateOne

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commitSyncEvent.project.path, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(commitSyncEvent.project.id, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val projectInfo1 = projectInfos.generateOne.copy(id = commitSyncEvent.project.id)
      givenFindingProjectInfo(commitSyncEvent, maybeAccessToken1)
        .returning(projectInfo1.pure[IO])
      val exception = exceptions.generateOne
      givenStoring(
        StartCommit(id = commitInfo1.id, project = Project(projectInfo1.id, projectInfo1.path))
      ).returning(IO.raiseError(exception))

      eventsGenerator.generateMissedEvents(commitSyncEvent).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Error(
          s"${logMessageCommon(commitSyncEvent)} -> synchronization failed in ${executionTimeRecorder.elapsedTime}ms",
          exception
        )
      )
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {

    val accessTokenFinder     = mock[AccessTokenFinder[IO]]
    val latestCommitFinder    = mock[LatestCommitFinder[IO]]
    val projectInfoFinder     = mock[ProjectInfoFinder[IO]]
    val commitToEventLog      = mock[CommitToEventLog[IO]]
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val eventsGenerator = new MissedEventsGeneratorImpl(
      accessTokenFinder,
      latestCommitFinder,
      projectInfoFinder,
      commitToEventLog,
      logger,
      executionTimeRecorder
    )

    def givenLatestCommitAndLogEventMatch(latestCommit: CommitSyncEvent): Unit = {
      val maybeAccessToken = Gen.option(accessTokens).generateOne
      givenAccessToken(latestCommit.project.path, maybeAccessToken)

      val commitInfo = commitInfos.generateOne.copy(id = latestCommit.id)
      givenFetchLatestCommit(latestCommit.project.id, maybeAccessToken)
        .returning(OptionT.some[IO](commitInfo))
      ()
    }

    def givenFetchLatestCommit(projectId: projects.Id, maybeAccessToken: Option[AccessToken]) =
      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)

    def givenAccessToken(projectPath: Path, maybeAccessToken: Option[AccessToken]) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[IO])

    def givenFindingProjectInfo(latestProjectCommit: CommitSyncEvent, maybeAccessToken: Option[AccessToken]) =
      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(latestProjectCommit.project.id, maybeAccessToken)

    def givenStoring(pushEvent: StartCommit) =
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(pushEvent)
  }
}
