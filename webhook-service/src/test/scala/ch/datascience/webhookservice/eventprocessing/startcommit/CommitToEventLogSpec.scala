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

package ch.datascience.webhookservice.eventprocessing.startcommit

import java.time.{Clock, Instant, ZoneId}

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.webhookservice.eventprocessing.StartCommit
import ch.datascience.webhookservice.eventprocessing.commitevent._
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitToEventLog.SendingResult
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class CommitToEventLogSpec extends WordSpec with MockFactory {

  import IOAccessTokenFinder._

  "storeCommitsInEventLog" should {

    "convert the start Commit into commit events and store them in the event log" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context pure maybeAccessToken)

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents = commitEventsFrom(startCommit).generateOne
      (eventsFlowBuilder
        .transformEventsWith[SendingResult](_: CommitEvent => Try[SendingResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[SendingResult]] =>
          commitEvents.map(transform).sequence
        }

      commitEvents foreach { commitEvent =>
        (commitEventSender
          .send(_: CommitEvent))
          .expects(commitEvent)
          .returning(context.pure(()))
      }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.loggedOnly(
        Info(
          successfulStoring(startCommit, commitEvents = commitEvents.size, stored = commitEvents.size, failed = 0)
        )
      )
    }

    "do nothing (skip logging) if there are no events to send" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context pure maybeAccessToken)

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents = List.empty[CommitEvent]
      (eventsFlowBuilder
        .transformEventsWith[SendingResult](_: CommitEvent => Try[SendingResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[SendingResult]] =>
          commitEvents.map(transform).sequence
        }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.expectNoLogs()
    }

    "fail if finding access token fails" in new TestCase {

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(generalFailure(startCommit), exception))
    }

    "fail if finding commit events source fails" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context pure maybeAccessToken)

      val exception = exceptions.generateOne
      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(generalFailure(startCommit), exception))
    }

    "fail if finding commit events fails" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val exception = exceptions.generateOne
      (eventsFlowBuilder
        .transformEventsWith[SendingResult](_: CommitEvent => Try[SendingResult]))
        .expects(*)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedFinding(startCommit), exception))
    }

    "fail if transforming to commit events fails" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val exception = exceptions.generateOne
      (eventsFlowBuilder
        .transformEventsWith[SendingResult](_: CommitEvent => Try[SendingResult]))
        .expects(*)
        .onCall { _: Function1[CommitEvent, Try[SendingResult]] =>
          context raiseError exception
        }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedFinding(startCommit), exception))
    }

    "store all non failing events and log errors for these for which storing fails" in new TestCase {
      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(projectId, projectIdToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents @ failingEvent +: passingEvents = commitEventsFrom(startCommit).generateOne
      (eventsFlowBuilder
        .transformEventsWith[SendingResult](_: CommitEvent => Try[SendingResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[SendingResult]] =>
          commitEvents.map(transform).sequence
        }

      val exception = exceptions.generateOne
      (commitEventSender
        .send(_: CommitEvent))
        .expects(failingEvent)
        .returning(context raiseError exception)
      passingEvents foreach { event =>
        (commitEventSender
          .send(_: CommitEvent))
          .expects(event)
          .returning(context pure ((): Unit))
      }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.loggedOnly(
        Error(failedStoring(startCommit, failingEvent), exception),
        Info(successfulStoring(startCommit, commitEvents.size, stored = passingEvents.size, failed = 1))
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val startCommit = startCommits.generateOne
    val projectId   = startCommit.project.id
    val batchDate   = BatchDate(Instant.now)
    val clock       = Clock.fixed(batchDate.value, ZoneId.systemDefault)

    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val commitEventSender     = mock[TryCommitEventSender]
    val commitEventsSource    = mock[TryCommitEventsSourceBuilder]
    val eventsFlowBuilder     = mock[CommitEventsSourceBuilder.EventsFlowBuilder[Try]]
    val logger                = TestLogger[Try]()
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)
    val commitToEventLog = new CommitToEventLog[Try](
      accessTokenFinder,
      commitEventsSource,
      commitEventSender,
      logger,
      executionTimeRecorder,
      clock
    )

    def successfulStoring(startCommit: StartCommit, commitEvents: Int, stored: Int, failed: Int): String =
      s"Start Commit id: ${startCommit.id}, project: ${startCommit.project.id}: " +
        s"$commitEvents Commit Events generated, $stored stored in the Event Log, $failed failed in ${executionTimeRecorder.elapsedTime}ms"

    def failedFinding(startCommit: StartCommit): String =
      s"Start Commit id: ${startCommit.id}, project: ${startCommit.project.id}: " +
        "finding commit events failed"

    def generalFailure(startCommit: StartCommit): String =
      s"Start Commit id: ${startCommit.id}, project: ${startCommit.project.id}: " +
        "converting to commit events failed"

    def failedStoring(startCommit: StartCommit, commitEvent: CommitEvent): String =
      s"Start Commit id: ${startCommit.id}, project: ${startCommit.project.id}, CommitEvent id: ${commitEvent.id}: " +
        "storing in the event log failed"

    def commitEventsFrom(startCommit: StartCommit): Gen[List[CommitEvent]] =
      for {
        commitEvent <- commitEventFrom(startCommit)
      } yield commitEvent +: commitEvent.parents
        .map(commitEventFrom(_, startCommit.project).generateOne)

    def commitEventFrom(startCommit: StartCommit): Gen[CommitEvent] = commitEventFrom(
      startCommit.id,
      startCommit.project
    )

    def commitEventFrom(commitId: CommitId, project: Project): Gen[CommitEvent] =
      for {
        message       <- commitMessages
        committedDate <- committedDates
        author        <- users
        committer     <- users
        parentsIds    <- parentsIdsLists()
      } yield CommitEvent(
        id            = commitId,
        message       = message,
        committedDate = committedDate,
        author        = author,
        committer     = committer,
        parents       = parentsIds,
        project       = project,
        batchDate     = batchDate
      )
  }
}
