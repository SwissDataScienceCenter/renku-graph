/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.missedevents

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info, Warn}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.webhookservice.commits.{CommitInfo, LatestCommitFinder}
import ch.datascience.webhookservice.eventprocessing.startcommit.IOCommitToEventLog
import ch.datascience.webhookservice.eventprocessing.{Project, StartCommit}
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.missedevents.LatestEventsFetcher.LatestProjectCommit
import ch.datascience.webhookservice.project.ProjectInfoFinder
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class IOMissedEventsLoaderSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import IOAccessTokenFinder._

  "loadMissedEvents" should {

    "do nothing when no eventIds found in the Event Log" in new TestCase {
      givenFetchLogLatestEvents
        .returning(context.pure(List.empty))

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, 0 skipped, 0 failed"
        )
      )
    }

    "do nothing if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

      val latestProjectsCommitsList = nonEmptyList(latestProjectsCommits).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      givenLatestCommitsAndLogEventsMatch(latestProjectsCommitsList: _*)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, ${latestProjectsCommitsList.size} skipped, 0 failed"
        )
      )
    }

    "add missing events to the Event Log " +
      "for projects with the latest eventIds different than the latest commits in GitLab" in new TestCase {

      val latestProjectsCommitsList @ commit1 +: commit2 +: commit3 +: Nil =
        nonEmptyList(latestProjectsCommits, minElements = 3, maxElements = 3).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      givenLatestCommitsAndLogEventsMatch(commit1, commit3)

      val maybeAccessToken2 = Gen.option(accessTokens).generateOne
      givenAccessToken(commit2.projectId, maybeAccessToken2)
      val commitInfo2 = commitInfos.generateOne
      givenFetchLatestCommit(commit2, maybeAccessToken2)
        .returning(OptionT.some[IO](commitInfo2))
      val projectInfo2 = projectInfos.generateOne.copy(id = commit2.projectId)
      givenFindingProjectInfo(commit2, maybeAccessToken2)
        .returning(context.pure(projectInfo2))

      givenStoring(
        StartCommit(id = commitInfo2.id, project = Project(projectInfo2.id, projectInfo2.path))
      ).returning(IO.unit)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 1 updates, 2 skipped, 0 failed"
        )
      )
    }

    "do nothing if the latest PushEvent does not exists" in new TestCase {
      val latestProjectsCommitsList @ commit1 +: commit2 +: Nil =
        nonEmptyList(latestProjectsCommits, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commit1.projectId, maybeAccessToken1)
      givenFetchLatestCommit(commit1, maybeAccessToken1)
        .returning(OptionT.none[IO, CommitInfo])

      givenLatestCommitsAndLogEventsMatch(commit2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, 2 skipped, 0 failed"
        )
      )
    }

    "not break processing if finding Access Token for one of the event(s) fails" in new TestCase {

      val latestProjectsCommitsList = nonEmptyList(latestProjectsCommits, minElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      val exception = exceptions.generateOne
      latestProjectsCommitsList.headOption.foreach { event =>
        (accessTokenFinder
          .findAccessToken(_: Id)(_: Id => String))
          .expects(event.projectId, projectIdToPath)
          .returning(context.raiseError(exception))
      }

      givenLatestCommitsAndLogEventsMatch(latestProjectsCommitsList.tail: _*)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      latestProjectsCommitsList.headOption.foreach { event =>
        logger.logged(Warn(s"Synchronizing Commits for project ${event.projectId} failed", exception))
      }
      logger.logged(
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, ${latestProjectsCommitsList.tail.size} skipped, 1 failed"
        )
      )
    }

    "not break processing if finding the latest Commit for one of the events fails" in new TestCase {

      val latestProjectsCommitsList @ commit1 +: commit2 +: Nil =
        nonEmptyList(latestProjectsCommits, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commit1.projectId, maybeAccessToken1)
      val exception = exceptions.generateOne
      givenFetchLatestCommit(commit1, maybeAccessToken1)
        .returning(OptionT.liftF(context.raiseError(exception)))

      givenLatestCommitsAndLogEventsMatch(commit2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${commit1.projectId} failed", exception),
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, 1 skipped, 1 failed"
        )
      )
    }

    "not break processing if finding Project Info for one of the events fails" in new TestCase {

      val latestProjectsCommitsList @ commit1 +: commit2 +: Nil =
        nonEmptyList(latestProjectsCommits, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commit1.projectId, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(commit1, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val exception = exceptions.generateOne
      givenFindingProjectInfo(commit1, maybeAccessToken1)
        .returning(context.raiseError(exception))

      givenLatestCommitsAndLogEventsMatch(commit2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${commit1.projectId} failed", exception),
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, 1 skipped, 1 failed"
        )
      )
    }

    "not break processing if storing start Commit for one of the events fails" in new TestCase {

      val latestProjectsCommitsList @ commit1 +: commit2 +: Nil =
        nonEmptyList(latestProjectsCommits, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestProjectsCommitsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(commit1.projectId, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(commit1, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val projectInfo1 = projectInfos.generateOne.copy(id = commit1.projectId)
      givenFindingProjectInfo(commit1, maybeAccessToken1)
        .returning(context.pure(projectInfo1))
      val exception = exceptions.generateOne
      givenStoring(
        StartCommit(id = commitInfo1.id, project = Project(projectInfo1.id, projectInfo1.path))
      ).returning(IO.raiseError(exception))

      givenLatestCommitsAndLogEventsMatch(commit2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${commit1.projectId} failed", exception),
        Info(
          s"Synchronized Commits with GitLab in ${executionTimeRecorder.elapsedTime}ms: 0 updates, 1 skipped, 1 failed"
        )
      )
    }

    "fail if finding latest events in the Event Log fails" in new TestCase {
      val exception = exceptions.generateOne
      givenFetchLogLatestEvents
        .returning(context.raiseError(exception))

      intercept[Exception] {
        eventsLoader.loadMissedEvents.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error("Synchronizing Commits with GitLab failed", exception))
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val latestEventsFinder    = mock[LatestEventsFetcher[IO]]
    val accessTokenFinder     = mock[AccessTokenFinder[IO]]
    val latestCommitFinder    = mock[LatestCommitFinder[IO]]
    val projectInfoFinder     = mock[ProjectInfoFinder[IO]]
    val commitToEventLog      = mock[IOCommitToEventLog]
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val eventsLoader = new IOMissedEventsLoader(
      latestEventsFinder,
      accessTokenFinder,
      latestCommitFinder,
      projectInfoFinder,
      commitToEventLog,
      logger,
      executionTimeRecorder
    )

    def givenFetchLogLatestEvents =
      (latestEventsFinder.fetchLatestEvents _)
        .expects()

    def givenLatestCommitsAndLogEventsMatch(latestCommits: LatestProjectCommit*): Unit =
      latestCommits foreach { latestCommit =>
        val maybeAccessToken = Gen.option(accessTokens).generateOne
        givenAccessToken(latestCommit.projectId, maybeAccessToken)

        val commitInfo = commitInfos.generateOne.copy(id = latestCommit.commitId)
        givenFetchLatestCommit(latestCommit, maybeAccessToken)
          .returning(OptionT.some[IO](commitInfo))
      }

    def givenFetchLatestCommit(latestCommit: LatestProjectCommit, maybeAccessToken: Option[AccessToken]) =
      (latestCommitFinder
        .findLatestCommit(_: Id, _: Option[AccessToken]))
        .expects(latestCommit.projectId, maybeAccessToken)

    def givenAccessToken(projectId: Id, maybeAccessToken: Option[AccessToken]) =
      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(projectId, projectIdToPath)
        .returning(context.pure(maybeAccessToken))

    def givenFindingProjectInfo(latestProjectCommit: LatestProjectCommit, maybeAccessToken: Option[AccessToken]) =
      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(latestProjectCommit.projectId, maybeAccessToken)

    def givenStoring(pushEvent: StartCommit) =
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(pushEvent)
  }

  private implicit val latestProjectsCommits: Gen[LatestProjectCommit] = for {
    commitId  <- commitIds
    projectId <- projectIds
  } yield LatestProjectCommit(commitId, projectId)
}
