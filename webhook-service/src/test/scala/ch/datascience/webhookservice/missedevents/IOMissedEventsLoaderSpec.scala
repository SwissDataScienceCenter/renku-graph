/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.control.Throttler
import ch.datascience.dbeventlog.commands.IOEventLogLatestEvents
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info, Warn}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.webhookservice.commits.{CommitInfo, LatestCommitFinder}
import ch.datascience.webhookservice.eventprocessing.StartCommit
import ch.datascience.webhookservice.eventprocessing.startcommit.IOCommitToEventLog
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.project.ProjectInfoFinder
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.global

class IOMissedEventsLoaderSpec extends WordSpec with MockFactory {

  "loadMissedEvents" should {

    "do nothing when no eventIds found in the Event Log" in new TestCase {
      givenFetchLogLatestEvents
        .returning(context.pure(List.empty))

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(Info("Synchronized Commits with GitLab in 10ms: 0 updates, 0 skipped, 0 failed"))
    }

    "do nothing if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

      val latestEventsList = nonEmptyList(commitEventIds).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      givenLatestCommitsAndLogEventsMatch(latestEventsList: _*)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(
        Info(s"Synchronized Commits with GitLab in 10ms: 0 updates, ${latestEventsList.size} skipped, 0 failed")
      )
    }

    "add missing events to the Event Log " +
      "for projects with the latest eventIds different than the latest commits in GitLab" in new TestCase {

      val latestEventsList @ event1 +: event2 +: event3 +: Nil =
        nonEmptyList(commitEventIds, minElements = 3, maxElements = 3).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      givenLatestCommitsAndLogEventsMatch(event1, event3)

      val maybeAccessToken2 = Gen.option(accessTokens).generateOne
      givenAccessToken(event2.projectId, maybeAccessToken2)
      val commitInfo2 = commitInfos.generateOne
      givenFetchLatestCommit(event2, maybeAccessToken2)
        .returning(OptionT.some[IO](commitInfo2))
      val projectInfo2 = projectInfos.generateOne.copy(id = event2.projectId)
      givenFindingProjectInfo(event2, maybeAccessToken2)
        .returning(context.pure(projectInfo2))

      givenStoring(
        StartCommit(id = commitInfo2.id, project = Project(projectInfo2.id, projectInfo2.path))
      ).returning(IO.unit)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(Info("Synchronized Commits with GitLab in 10ms: 1 updates, 2 skipped, 0 failed"))
    }

    "do nothing if the latest PushEvent does not exists" in new TestCase {
      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      givenFetchLatestCommit(event1, maybeAccessToken1)
        .returning(OptionT.none[IO, CommitInfo])

      givenLatestCommitsAndLogEventsMatch(event2)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.logged(Info("Synchronized Commits with GitLab in 10ms: 0 updates, 2 skipped, 0 failed"))
    }

    "not break processing if finding Access Token for one of the event(s) fails" in new TestCase {

      val latestEventsList = nonEmptyList(commitEventIds, minElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val exception = exceptions.generateOne
      latestEventsList.headOption.foreach { event =>
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(event.projectId)
          .returning(context.raiseError(exception))
      }

      givenLatestCommitsAndLogEventsMatch(latestEventsList.tail: _*)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      latestEventsList.headOption.foreach { event =>
        logger.logged(Warn(s"Synchronizing Commits for project ${event.projectId} failed", exception))
      }
      logger.logged(
        Info(s"Synchronized Commits with GitLab in 10ms: 0 updates, ${latestEventsList.tail.size} skipped, 1 failed")
      )
    }

    "not break processing if finding the latest Commit for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val exception = exceptions.generateOne
      givenFetchLatestCommit(event1, maybeAccessToken1)
        .returning(OptionT.liftF(context.raiseError(exception)))

      givenLatestCommitsAndLogEventsMatch(event2)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${event1.projectId} failed", exception),
        Info("Synchronized Commits with GitLab in 10ms: 0 updates, 1 skipped, 1 failed")
      )
    }

    "not break processing if finding Project Info for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(event1, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val exception = exceptions.generateOne
      givenFindingProjectInfo(event1, maybeAccessToken1)
        .returning(context.raiseError(exception))

      givenLatestCommitsAndLogEventsMatch(event2)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${event1.projectId} failed", exception),
        Info("Synchronized Commits with GitLab in 10ms: 0 updates, 1 skipped, 1 failed")
      )
    }

    "not break processing if storing start Commit for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne.toList
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val commitInfo1 = commitInfos.generateOne
      givenFetchLatestCommit(event1, maybeAccessToken1)
        .returning(OptionT.some[IO](commitInfo1))
      val projectInfo1 = projectInfos.generateOne.copy(id = event1.projectId)
      givenFindingProjectInfo(event1, maybeAccessToken1)
        .returning(context.pure(projectInfo1))
      val exception = exceptions.generateOne
      givenStoring(
        StartCommit(id = commitInfo1.id, project = Project(projectInfo1.id, projectInfo1.path))
      ).returning(IO.raiseError(exception))

      givenLatestCommitsAndLogEventsMatch(event2)

      givenThrottlerAccessed(latestEventsList.size)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Warn(s"Synchronizing Commits for project ${event1.projectId} failed", exception),
        Info("Synchronized Commits with GitLab in 10ms: 0 updates, 1 skipped, 1 failed")
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

    val eventLogLatestEvents  = mock[IOEventLogLatestEvents]
    val accessTokenFinder     = mock[AccessTokenFinder[IO]]
    val latestCommitFinder    = mock[LatestCommitFinder[IO]]
    val projectInfoFinder     = mock[ProjectInfoFinder[IO]]
    val commitToEventLog      = mock[IOCommitToEventLog]
    val logger                = TestLogger[IO]()
    val throttler             = mock[Throttler[IO, EventsSynchronization]]
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](expected = ElapsedTime(10))
    val eventsLoader = new IOMissedEventsLoader(
      eventLogLatestEvents,
      accessTokenFinder,
      latestCommitFinder,
      projectInfoFinder,
      commitToEventLog,
      throttler,
      logger,
      executionTimeRecorder
    )

    def givenFetchLogLatestEvents =
      (eventLogLatestEvents.findAllLatestEvents _)
        .expects()

    def givenLatestCommitsAndLogEventsMatch(latestEvents: CommitEventId*): Unit =
      latestEvents foreach { latestEvent =>
        val maybeAccessToken = Gen.option(accessTokens).generateOne
        givenAccessToken(latestEvent.projectId, maybeAccessToken)

        val commitInfo = commitInfos.generateOne.copy(id = latestEvent.id)
        givenFetchLatestCommit(latestEvent, maybeAccessToken)
          .returning(OptionT.some[IO](commitInfo))
      }

    def givenFetchLatestCommit(latestEvent: CommitEventId, maybeAccessToken: Option[AccessToken]) =
      (latestCommitFinder
        .findLatestCommit(_: ProjectId, _: Option[AccessToken]))
        .expects(latestEvent.projectId, maybeAccessToken)

    def givenAccessToken(projectId: ProjectId, maybeAccessToken: Option[AccessToken]) =
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(projectId)
        .returning(context.pure(maybeAccessToken))

    def givenFindingProjectInfo(latestEvent: CommitEventId, maybeAccessToken: Option[AccessToken]) =
      (projectInfoFinder
        .findProjectInfo(_: ProjectId, _: Option[AccessToken]))
        .expects(latestEvent.projectId, maybeAccessToken)

    def givenStoring(pushEvent: StartCommit) =
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(pushEvent)

    def givenThrottlerAccessed(times: Int) = {
      (throttler.acquire _).expects().returning(IO.unit).repeat(times to times)
      (throttler.release _).expects().returning(IO.unit).repeat(times to times)
    }
  }
}
