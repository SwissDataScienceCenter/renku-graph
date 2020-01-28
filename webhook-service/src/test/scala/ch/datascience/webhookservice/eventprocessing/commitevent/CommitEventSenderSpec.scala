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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.MonadError
import cats.effect.Bracket
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.commands.EventLogAdd
import ch.datascience.dbeventlog.{EventBody, EventLogDB}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class CommitEventSenderSpec extends WordSpec with MockFactory {

  "send" should {

    "succeed when delivering the event to the Event Log was successful" in new TestCase {

      val serializedEvent = serialize(commitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(serializedEvent))

      (eventAdd
        .storeNewEvent(_: CommitEvent, _: EventBody))
        .expects(commitEvent, EventBody(serializedEvent))
        .returning(context.unit)

      eventSender.send(commitEvent) shouldBe Success(())
    }

    "fail when event serialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.raiseError(exception))

      eventSender.send(commitEvent) shouldBe Failure(exception)
    }

    "fail when delivering the event to the Event Log fails" in new TestCase {

      val serializedEvent = serialize(commitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(serializedEvent))

      val exception = exceptions.generateOne
      (eventAdd
        .storeNewEvent(_: CommitEvent, _: EventBody))
        .expects(commitEvent, EventBody(serializedEvent))
        .returning(context.raiseError(exception))

      eventSender.send(commitEvent) shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitEvent = commitEvents.generateOne

    class TestCommitEventSerializer extends CommitEventSerializer[Try]
    class TestEventLogAdd(
        transactor: DbTransactor[Try, EventLogDB]
    )(implicit ME:  Bracket[Try, Throwable])
        extends EventLogAdd[Try](transactor)
    val eventSerializer = mock[TestCommitEventSerializer]
    val eventAdd        = mock[TestEventLogAdd]
    val eventSender     = new CommitEventSender[Try](eventSerializer, eventAdd)
  }

  private def serialize(commitEvent: CommitEvent): String =
    s"""{id: "${commitEvent.id.toString}"}"""
}
