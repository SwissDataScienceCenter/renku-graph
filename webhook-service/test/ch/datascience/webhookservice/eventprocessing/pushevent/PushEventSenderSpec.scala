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

import java.time.Instant

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.{CommitEvent, User}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.commitevent.{CommitEventSender, CommitEventSerializer, EventLog}
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class PushEventSenderSpec extends WordSpec with MockFactory {

  "storeCommitsInEventLog" should {

    "convert the given push event to commit events and store them in the event log" in new TestCase {
      val commitEvent = commitEventFrom(pushEvent)

      (commitEventsFinder
        .findCommitEvents(_: PushEvent))
        .expects(pushEvent)
        .returning(context.pure(commitEvent))

      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(()))

      pushEventSender.storeCommitsInEventLog(pushEvent) shouldBe Success(())

      logger.loggedOnly(Info, s"PushEvent for id: ${pushEvent.after}, project: ${pushEvent.project.id} stored")
    }

    "fail if finding commit events for the given push event fails" in new TestCase {
      val commitEvent = commitEventFrom(pushEvent)

      val exception = exceptions.generateOne
      (commitEventsFinder
        .findCommitEvents(_: PushEvent))
        .expects(pushEvent)
        .returning(context.raiseError(exception))

      pushEventSender.storeCommitsInEventLog(pushEvent) shouldBe Failure(exception)

      logger.loggedOnly(
        Error,
        s"Storing pushEvent for id: ${pushEvent.after}, project: ${pushEvent.project.id} failed",
        exception
      )
    }

    "fail if storing in the event log fails" in new TestCase {
      val commitEvent = commitEventFrom(pushEvent)

      (commitEventsFinder
        .findCommitEvents(_: PushEvent))
        .expects(pushEvent)
        .returning(context.pure(commitEvent))

      val exception = exceptions.generateOne
      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.raiseError(exception))

      pushEventSender.storeCommitsInEventLog(pushEvent) shouldBe Failure(exception)

      logger.loggedOnly(
        Error,
        s"Storing pushEvent for id: ${pushEvent.after}, project: ${pushEvent.project.id} failed",
        exception
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val pushEvent = pushEvents.generateOne

    val commitEventSender  = mock[TestCommitEventSender]
    val commitEventsFinder = mock[TestCommitEventsFinder]
    val logger             = TestLogger[Try]()
    val pushEventSender    = new PushEventSender[Try](commitEventsFinder, commitEventSender, logger)
  }

  private class TestCommitEventSender(
      eventLog:              EventLog[Try],
      commitEventSerializer: CommitEventSerializer[Try],
      logger:                Logger[Try]
  ) extends CommitEventSender[Try](eventLog, commitEventSerializer, logger)

  private class TestCommitEventsFinder extends CommitEventsFinder[Try]

  private def commitEventFrom(pushEvent: PushEvent) = CommitEvent(
    id        = pushEvent.after,
    message   = "",
    timestamp = Instant.EPOCH,
    pushUser  = pushEvent.pushUser,
    author    = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
    committer = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
    parents   = Seq(),
    project   = pushEvent.project,
    added     = Nil,
    modified  = Nil,
    removed   = Nil
  )
}
