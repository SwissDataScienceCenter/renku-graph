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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.eventprocessing.CommitEventsOrigin
import ch.datascience.webhookservice.eventprocessing.commitevent.{CommitEventSender, CommitEventSerializer, EventLog}
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class PushEventSenderSpec extends WordSpec with MockFactory {

  "storeCommitsInEventLog" should {

    "convert the given push event into commit events and store them in the event log" in new TestCase {
      val commitEventsStream = commitEventsFrom(commitEventsOrigin).generateOne

      (commitEventsFinder
        .findCommitEvents(_: CommitEventsOrigin))
        .expects(commitEventsOrigin)
        .returning(context.pure(commitEventsStream))

      val commitEvents = toList(commitEventsStream)
      commitEvents foreach { commitEvent =>
        (commitEventSender
          .send(_: CommitEvent))
          .expects(commitEvent)
          .returning(context.pure(()))
      }

      pushEventSender.storeCommitsInEventLog(commitEventsOrigin) shouldBe Success(())

      logger.loggedOnly(
        commitEvents.map(event => Info(successfulStoring(commitEventsOrigin, event))) :+
          Info(successfulStoring(commitEventsOrigin))
      )
    }

    "fail if finding commit events stream fails" in new TestCase {
      val exception = exceptions.generateOne
      (commitEventsFinder
        .findCommitEvents(_: CommitEventsOrigin))
        .expects(commitEventsOrigin)
        .returning(context.raiseError(exception))

      pushEventSender.storeCommitsInEventLog(commitEventsOrigin) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedStoring(commitEventsOrigin), exception))
    }

    "store all non failing events and log errors for these for which fetching fail" in new TestCase {
      val exception          = exceptions.generateOne
      val commitEventsStream = Failure(exception) #:: commitEventsFrom(commitEventsOrigin).generateOne
      (commitEventsFinder
        .findCommitEvents(_: CommitEventsOrigin))
        .expects(commitEventsOrigin)
        .returning(context.pure(commitEventsStream))

      val commitEvents = toList(commitEventsStream)
      commitEvents foreach { commitEvent =>
        (commitEventSender
          .send(_: CommitEvent))
          .expects(commitEvent)
          .returning(context.pure(()))
      }

      pushEventSender.storeCommitsInEventLog(commitEventsOrigin) shouldBe Success(())

      logger.loggedOnly(
        Error(failedFetching(commitEventsOrigin), exception) +:
          commitEvents.map(event => Info(successfulStoring(commitEventsOrigin, event))) :+
          Info(successfulStoring(commitEventsOrigin))
      )
    }

    "store all non failing events and log errors for these for which storing fail" in new TestCase {
      val commitEventsStream = commitEventsFrom(commitEventsOrigin).generateOne
      (commitEventsFinder
        .findCommitEvents(_: CommitEventsOrigin))
        .expects(commitEventsOrigin)
        .returning(context.pure(commitEventsStream))

      val failingEvent +: passingEvents = toList(commitEventsStream)

      val exception = exceptions.generateOne
      (commitEventSender
        .send(_: CommitEvent))
        .expects(failingEvent)
        .returning(context.raiseError(exception))
      passingEvents foreach { event =>
        (commitEventSender
          .send(_: CommitEvent))
          .expects(event)
          .returning(context.pure(()))
      }

      pushEventSender.storeCommitsInEventLog(commitEventsOrigin) shouldBe Success(())

      logger.loggedOnly(
        Error(failedStoring(commitEventsOrigin, failingEvent), exception) +:
          passingEvents.map(event => Info(successfulStoring(commitEventsOrigin, event))) :+
          Info(successfulStoring(commitEventsOrigin))
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitEventsOrigin = commitEventsOrigins.generateOne

    val commitEventSender  = mock[TestCommitEventSender]
    val commitEventsFinder = mock[TestCommitEventsFinder]
    val logger             = TestLogger[Try]()
    val pushEventSender    = new PushEventSender[Try](commitEventsFinder, commitEventSender, logger)
  }

  private class TestCommitEventSender(
      eventLog:              EventLog[Try],
      commitEventSerializer: CommitEventSerializer[Try]
  ) extends CommitEventSender[Try](eventLog, commitEventSerializer)

  private class TestCommitEventsFinder(
      commitInfoFinder: CommitInfoFinder[Try]
  ) extends CommitEventsFinder[Try](commitInfoFinder)

  private def commitEventsFrom(commitEventsOrigin: CommitEventsOrigin): Gen[Stream[Try[CommitEvent]]] =
    for {
      commitEvent <- commitEventFrom(commitEventsOrigin)
    } yield {
      val firstCommitEvent = commitEventFrom(commitEventsOrigin).generateOne

      commitEvent.parents.foldLeft(Stream(Try(firstCommitEvent))) { (commitEvents, parentId) =>
        Try(
          commitEventFrom(parentId,
                          commitEventsOrigin.pushUser,
                          commitEventsOrigin.project,
                          commitEventsOrigin.hookAccessToken).generateOne) #:: commitEvents
      }
    }

  private def commitEventFrom(commitEventsOrigin: CommitEventsOrigin): Gen[CommitEvent] =
    commitEventFrom(
      commitEventsOrigin.commitTo,
      commitEventsOrigin.pushUser,
      commitEventsOrigin.project,
      commitEventsOrigin.hookAccessToken
    )

  private def commitEventFrom(commitId:        CommitId,
                              pushUser:        PushUser,
                              project:         Project,
                              hookAccessToken: HookAccessToken): Gen[CommitEvent] =
    for {
      message       <- commitMessages
      committedDate <- committedDates
      author        <- users
      committer     <- users
      parentsIds    <- parentsIdsLists()
    } yield
      CommitEvent(
        id              = commitId,
        message         = message,
        committedDate   = committedDate,
        pushUser        = pushUser,
        author          = author,
        committer       = committer,
        parents         = parentsIds,
        project         = project,
        hookAccessToken = hookAccessToken
      )

  private def toList(eventsStream: Stream[Try[CommitEvent]]): List[CommitEvent] =
    eventsStream.toList.foldLeft(List.empty[CommitEvent]) {
      case (allEvents, Success(event)) => allEvents :+ event
      case (allEvents, Failure(_))     => allEvents
    }

  private def failedFetching(commitEventsOrigin: CommitEventsOrigin): String =
    s"PushEvent commitTo: ${commitEventsOrigin.commitTo}, project: ${commitEventsOrigin.project.id}: fetching one of the commit events failed"

  private def failedStoring(commitEventsOrigin: CommitEventsOrigin): String =
    s"PushEvent commitTo: ${commitEventsOrigin.commitTo}, project: ${commitEventsOrigin.project.id}: storing in event log failed"

  private def failedStoring(commitEventsOrigin: CommitEventsOrigin, commitEvent: CommitEvent): String =
    s"PushEvent commitTo: ${commitEventsOrigin.commitTo}, project: ${commitEventsOrigin.project.id}, CommitEvent id: ${commitEvent.id}: storing in event log failed"

  private def successfulStoring(commitEventsOrigin: CommitEventsOrigin, commitEvent: CommitEvent): String =
    s"PushEvent commitTo: ${commitEventsOrigin.commitTo}, project: ${commitEventsOrigin.project.id}, CommitEvent id: ${commitEvent.id}: stored in event log"

  private def successfulStoring(commitEventsOrigin: CommitEventsOrigin): String =
    s"PushEvent commitTo: ${commitEventsOrigin.commitTo}, project: ${commitEventsOrigin.project.id}: stored in event log"
}
