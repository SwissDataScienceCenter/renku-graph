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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class CommitEventSenderSpec extends WordSpec with MockFactory {

  "send" should {

    "succeed when delivering the event to the storage was successful" in new TestCase {

      val serializedEvent = serialize(commitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(serializedEvent))

      (eventLog
        .append(_: String))
        .expects(serializedEvent)
        .returning(context.pure(()))

      eventSender.send(commitEvent)

      logger.loggedOnly(Info(s"Commit event id: ${commitEvent.id}, project: ${commitEvent.project.id} stored"))
    }

    "fail when event serialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.raiseError(exception))

      eventSender.send(commitEvent)

      logger.loggedOnly(
        Error(
          s"Storing commit event id: ${commitEvent.id}, project: ${commitEvent.project.id} failed",
          exception
        )
      )
    }

    "fail when delivering the event to the storage return an error" in new TestCase {

      val serializedEvent = serialize(commitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(serializedEvent))

      val exception = exceptions.generateOne
      (eventLog
        .append(_: String))
        .expects(serialize(commitEvent))
        .returning(context.raiseError(exception))

      eventSender.send(commitEvent)

      logger.loggedOnly(
        Error(
          s"Storing commit event id: ${commitEvent.id}, project: ${commitEvent.project.id} failed",
          exception
        )
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitEvent = commitEvents.generateOne

    class TestCommitEventSerializer extends CommitEventSerializer[Try]
    val eventSerializer = mock[TestCommitEventSerializer]
    val eventLog        = mock[EventLog[Try]]
    val logger          = TestLogger[Try]()
    val eventSender     = new CommitEventSender[Try](eventLog, eventSerializer, logger)
  }

  private def serialize(commitEvent: CommitEvent): String =
    s"""{id: "${commitEvent.id.toString}"}"""
}
