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
package historytraversal

import cats.MonadError
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.EventCreationResult._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.logging.TestExecutionTimeRecorder
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import scala.util._

class CommitToEventLogSpec extends AnyWordSpec with MockFactory with should.Matchers {

  import AccessTokenFinder._

  "storeCommitsInEventLog" should {

    "convert the Start Commit into commit events and store them in the Event Log if they do not exist yet" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context pure maybeAccessToken)

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents = commitEventsFrom(startCommit).generateOne
      (eventsFlowBuilder
        .transformEventsWith(_: CommitEvent => Try[EventCreationResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[EventCreationResult]] =>
          commitEvents.map(transform).sequence
        }

      val eventCreationResults = commitEvents map { commitEvent =>
        val doesExist = Random.nextBoolean()
        if (doesExist) {
          (eventDetailsFinder
            .checkIfExists(_: CommitId, _: projects.Id))
            .expects(commitEvent.id, commitEvent.project.id)
            .returning(doesExist.pure[Try])
          Existed
        } else {
          (eventDetailsFinder
            .checkIfExists(_: CommitId, _: projects.Id))
            .expects(commitEvent.id, commitEvent.project.id)
            .returning(doesExist.pure[Try])
          (commitEventSender
            .send(_: CommitEvent))
            .expects(commitEvent)
            .returning(().pure[Try])
          Created
        }
      }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.loggedOnly(
        Info(
          successfulStoring(startCommit,
                            created = eventCreationResults.count(_ == Created),
                            existed = eventCreationResults.count(_ == Existed),
                            failed = 0
          )
        )
      )
    }

    "do nothing (skip logging) if there are no events to send" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context pure maybeAccessToken)

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents = List.empty[CommitEvent]
      (eventsFlowBuilder
        .transformEventsWith(_: CommitEvent => Try[EventCreationResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[EventCreationResult]] =>
          commitEvents.map(transform).sequence
        }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.expectNoLogs()
    }

    "fail if finding access token fails" in new TestCase {

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(generalFailure(startCommit), exception))
    }

    "fail if building commit events source fails" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
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
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val exception = exceptions.generateOne
      (eventsFlowBuilder
        .transformEventsWith(_: CommitEvent => Try[EventCreationResult]))
        .expects(*)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedFinding(startCommit), exception))
    }

    "fail if transforming to commit events fails" in new TestCase {

      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val exception = exceptions.generateOne
      (eventsFlowBuilder
        .transformEventsWith(_: CommitEvent => Try[EventCreationResult]))
        .expects(*)
        .onCall { _: Function1[CommitEvent, Try[EventCreationResult]] =>
          context raiseError exception
        }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedFinding(startCommit), exception))
    }

    "store all non failing events and log errors for these which failed" in new TestCase {
      val maybeAccessToken = Gen.option(accessTokens).generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(context.pure(maybeAccessToken))

      (commitEventsSource
        .buildEventsSource(_: StartCommit, _: Option[AccessToken], _: Clock))
        .expects(startCommit, maybeAccessToken, clock)
        .returning(context pure eventsFlowBuilder)

      val commitEvents @ failingToSendEvent +: failingToCheckIfExistEvent +: passingEvents =
        commitEventsFrom(startCommit, minParents = 2).generateOne
      (eventsFlowBuilder
        .transformEventsWith(_: CommitEvent => Try[EventCreationResult]))
        .expects(*)
        .onCall { transform: Function1[CommitEvent, Try[EventCreationResult]] =>
          commitEvents.map(transform).sequence
        }

      val sendException = exceptions.generateOne
      (eventDetailsFinder
        .checkIfExists(_: CommitId, _: projects.Id))
        .expects(failingToSendEvent.id, failingToSendEvent.project.id)
        .returning(false.pure[Try])
      (commitEventSender
        .send(_: CommitEvent))
        .expects(failingToSendEvent)
        .returning(context raiseError sendException)

      val checkIfExistsException = exceptions.generateOne
      (eventDetailsFinder
        .checkIfExists(_: CommitId, _: projects.Id))
        .expects(failingToCheckIfExistEvent.id, failingToCheckIfExistEvent.project.id)
        .returning(context raiseError checkIfExistsException)
      passingEvents foreach { event =>
        (eventDetailsFinder
          .checkIfExists(_: CommitId, _: projects.Id))
          .expects(event.id, event.project.id)
          .returning(false.pure[Try])
        (commitEventSender
          .send(_: CommitEvent))
          .expects(event)
          .returning(().pure[Try])

      }

      commitToEventLog.storeCommitsInEventLog(startCommit) shouldBe Success(())

      logger.loggedOnly(
        Error(failedStoring(startCommit, failingToSendEvent), sendException),
        Error(failedEventFinding(startCommit, failingToCheckIfExistEvent), checkIfExistsException),
        Info(
          successfulStoring(startCommit,
                            created = passingEvents.size,
                            existed = 0,
                            failed = commitEvents.size - passingEvents.size
          )
        )
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val startCommit = startCommits.generateOne
    val projectPath = startCommit.project.path
    val batchDate   = BatchDate(Instant.now)
    val clock       = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val commitEventSender     = mock[CommitEventSender[Try]]
    val commitEventsSource    = mock[TryCommitEventsSourceBuilder]
    val eventDetailsFinder    = mock[EventDetailsFinder[Try]]
    val eventsFlowBuilder     = mock[CommitEventsSourceBuilder.EventsFlowBuilder[Try]]
    val logger                = TestLogger[Try]()
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger)
    val commitToEventLog = new CommitToEventLogImpl[Try](
      accessTokenFinder,
      commitEventsSource,
      commitEventSender,
      eventDetailsFinder,
      logger,
      executionTimeRecorder,
      clock
    )

    def successfulStoring(startCommit: StartCommit, created: Int, existed: Int, failed: Int): String =
      s"$categoryName: id = ${startCommit.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        s"events generation result: $created created, $existed existed, $failed failed in ${executionTimeRecorder.elapsedTime}ms"

    def failedFinding(startCommit: StartCommit): String =
      s"$categoryName: id = ${startCommit.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "finding commit events failed"

    def generalFailure(startCommit: StartCommit): String =
      s"$categoryName: id = ${startCommit.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "converting to commit events failed"

    def failedStoring(startCommit: StartCommit, commitEvent: CommitEvent): String =
      s"$categoryName: id = ${startCommit.id}, addedId = ${commitEvent.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "storing in the event log failed"

    def failedEventFinding(startCommit: StartCommit, commitEvent: CommitEvent): String =
      s"$categoryName: id = ${startCommit.id}, addedId = ${commitEvent.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "finding event in the event log failed"

    def commitEventsFrom(startCommit: StartCommit, minParents: Int Refined NonNegative = 0): Gen[List[CommitEvent]] =
      for {
        commitEvent <- commitEventFrom(startCommit, minParents)
      } yield commitEvent +: commitEvent.parents
        .map(commitEventFrom(_, startCommit.project).generateOne)

    def commitEventFrom(startCommit: StartCommit, minParents: Int Refined NonNegative): Gen[CommitEvent] =
      commitEventFrom(
        startCommit.id,
        startCommit.project,
        minParents
      )

    def commitEventFrom(commitId:   CommitId,
                        project:    Project,
                        minParents: Int Refined NonNegative = 0
    ): Gen[CommitEvent] = for {
      message       <- commitMessages
      committedDate <- committedDates
      author        <- authors
      committer     <- committers
      parentsIds    <- listOf(commitIds, minParents)
    } yield NewCommitEvent(
      id = commitId,
      message = message,
      committedDate = committedDate,
      author = author,
      committer = committer,
      parents = parentsIds,
      project = project,
      batchDate = batchDate
    )
  }
}
