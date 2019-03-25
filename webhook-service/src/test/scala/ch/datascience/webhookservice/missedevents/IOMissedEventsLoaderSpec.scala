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
import cats.effect.{Clock, ContextShift, IO}
import ch.datascience.dbeventlog.commands.IOEventLogLatestEvents
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info, Warn}
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.IOPushEventSender
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.project.ProjectInfoFinder
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class IOMissedEventsLoaderSpec extends WordSpec with MockFactory {

  "loadMissedEvents" should {

    "do nothing when no eventIds found in the Event Log" in new TestCase {
      givenFetchLogLatestEvents
        .returning(context.pure(List.empty))

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.logged(Info("Synchronized events with GitLab in 10s: 0 updates, 0 skipped, 0 failed"))
    }

    "do nothing if the latest eventIds in the Event Log " +
      "matches the latest commits in GitLab for relevant projects" in new TestCase {

      val latestEventsList = nonEmptyList(commitEventIds).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      givenPushAndLogEventsMatch(latestEventsList: _*)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.logged(
        Info(s"Synchronized events with GitLab in 10s: 0 updates, ${latestEventsList.size} skipped, 0 failed")
      )
    }

    "add missing events to the Event Log " +
      "for projects with the latest eventIds different than the latest commits in GitLab" in new TestCase {

      val latestEventsList @ event1 +: event2 +: event3 +: Nil =
        nonEmptyList(commitEventIds, minElements = 3, maxElements = 3).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      givenPushAndLogEventsMatch(event1, event3)

      val maybeAccessToken2 = Gen.option(accessTokens).generateOne
      givenAccessToken(event2.projectId, maybeAccessToken2)
      val pushEventInfo2 = pushEventInfos.generateOne.copy(event2.projectId)
      givenFetchLatestPushEvent(event2, maybeAccessToken2)
        .returning(IO.pure(Some(pushEventInfo2)))
      val projectInfo2 = projectInfos.generateOne.copy(id = event2.projectId)
      givenFindingProjectInfo(event2, maybeAccessToken2)
        .returning(context.pure(projectInfo2))

      givenStoring(
        PushEvent(maybeCommitFrom = None,
                  commitTo        = pushEventInfo2.commitTo,
                  pushUser        = pushEventInfo2.pushUser,
                  project         = Project(projectInfo2.id, projectInfo2.path))
      ).returning(IO.unit)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.logged(Info("Synchronized events with GitLab in 10s: 1 updates, 2 skipped, 0 failed"))
    }

    "do nothing if the latest PushEvent does not exists" in new TestCase {
      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      givenFetchLatestPushEvent(event1, maybeAccessToken1)
        .returning(IO.pure(None))

      givenPushAndLogEventsMatch(event2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.logged(Info("Synchronized events with GitLab in 10s: 0 updates, 2 skipped, 0 failed"))
    }

    "not break processing if finding Access Token for one of the event(s) fails" in new TestCase {

      val latestEventsList = nonEmptyList(commitEventIds, minElements = 2).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val exception = exceptions.generateOne
      latestEventsList.headOption.foreach { event =>
        (accessTokenFinder
          .findAccessToken(_: ProjectId))
          .expects(event.projectId)
          .returning(context.raiseError(exception))
      }

      givenPushAndLogEventsMatch(latestEventsList.tail: _*)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      latestEventsList.headOption.foreach { event =>
        logger.logged(Warn(s"Synchronizing events for project ${event.projectId} failed", exception))
      }
      logger.logged(
        Info(s"Synchronized events with GitLab in 10s: 0 updates, ${latestEventsList.tail.size} skipped, 1 failed")
      )
    }

    "not break processing if finding the latest Push Event for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val exception = exceptions.generateOne
      givenFetchLatestPushEvent(event1, maybeAccessToken1)
        .returning(context.raiseError(exception))

      givenPushAndLogEventsMatch(event2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Warn(s"Synchronizing events for project ${event1.projectId} failed", exception),
        Info("Synchronized events with GitLab in 10s: 0 updates, 1 skipped, 1 failed")
      )
    }

    "not break processing if finding Project Info for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val pushEventInfo1 = pushEventInfos.generateOne.copy(event2.projectId)
      givenFetchLatestPushEvent(event1, maybeAccessToken1)
        .returning(IO.pure(Some(pushEventInfo1)))
      val exception = exceptions.generateOne
      givenFindingProjectInfo(event1, maybeAccessToken1)
        .returning(context.raiseError(exception))

      givenPushAndLogEventsMatch(event2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Warn(s"Synchronizing events for project ${event1.projectId} failed", exception),
        Info("Synchronized events with GitLab in 10s: 0 updates, 1 skipped, 1 failed")
      )
    }

    "not break processing if storing Push Event for one of the events fails" in new TestCase {

      val latestEventsList @ event1 +: event2 +: Nil =
        nonEmptyList(commitEventIds, minElements = 2, maxElements = 2).generateOne
      givenFetchLogLatestEvents
        .returning(context.pure(latestEventsList))

      val maybeAccessToken1 = Gen.option(accessTokens).generateOne
      givenAccessToken(event1.projectId, maybeAccessToken1)
      val pushEventInfo1 = pushEventInfos.generateOne.copy(event2.projectId)
      givenFetchLatestPushEvent(event1, maybeAccessToken1)
        .returning(IO.pure(Some(pushEventInfo1)))
      val projectInfo1 = projectInfos.generateOne.copy(id = event1.projectId)
      givenFindingProjectInfo(event1, maybeAccessToken1)
        .returning(context.pure(projectInfo1))
      val exception = exceptions.generateOne
      givenStoring(
        PushEvent(maybeCommitFrom = None,
                  commitTo        = pushEventInfo1.commitTo,
                  pushUser        = pushEventInfo1.pushUser,
                  project         = Project(projectInfo1.id, projectInfo1.path))
      ).returning(IO.raiseError(exception))

      givenPushAndLogEventsMatch(event2)

      eventsLoader.loadMissedEvents.unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Warn(s"Synchronizing events for project ${event1.projectId} failed", exception),
        Info("Synchronized events with GitLab in 10s: 0 updates, 1 skipped, 1 failed")
      )
    }

    "fail if finding latest events in the Event Log fails" in new TestCase {
      val exception = exceptions.generateOne
      givenFetchLogLatestEvents
        .returning(context.raiseError(exception))

      intercept[Exception] {
        eventsLoader.loadMissedEvents.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error("Synchronizing events with GitLab failed", exception))
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    private implicit val clock: Clock[IO] = mock[Clock[IO]]
    (clock.monotonic(_: TimeUnit)).expects(SECONDS).returning(context.pure(1000L))
    (clock.monotonic(_: TimeUnit)).expects(SECONDS).returning(context.pure(1010L)).noMoreThanOnce()

    val eventLogLatestEvents   = mock[IOEventLogLatestEvents]
    val accessTokenFinder      = mock[AccessTokenFinder[IO]]
    val latestPushEventFetcher = mock[LatestPushEventFetcher[IO]]
    val projectInfoFinder      = mock[ProjectInfoFinder[IO]]
    val pushEventSender        = mock[IOPushEventSender]
    val logger                 = TestLogger[IO]()
    val eventsLoader = new IOMissedEventsLoader(
      eventLogLatestEvents,
      accessTokenFinder,
      latestPushEventFetcher,
      projectInfoFinder,
      pushEventSender,
      logger,
    )

    def givenFetchLogLatestEvents =
      (eventLogLatestEvents.findAllLatestEvents _)
        .expects()

    def givenPushAndLogEventsMatch(latestEvents: CommitEventId*): Unit =
      latestEvents foreach { latestEvent =>
        val maybeAccessToken = Gen.option(accessTokens).generateOne
        givenAccessToken(latestEvent.projectId, maybeAccessToken)

        val pushEventInfo = pushEventInfos.generateOne.copy(latestEvent.projectId, commitTo = latestEvent.id)
        givenFetchLatestPushEvent(latestEvent, maybeAccessToken)
          .returning(context.pure(Some(pushEventInfo)))
      }

    def givenFetchLatestPushEvent(latestEvent: CommitEventId, maybeAccessToken: Option[AccessToken]) =
      (latestPushEventFetcher
        .fetchLatestPushEvent(_: ProjectId, _: Option[AccessToken]))
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

    def givenStoring(pushEvent: PushEvent) =
      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
  }
}
