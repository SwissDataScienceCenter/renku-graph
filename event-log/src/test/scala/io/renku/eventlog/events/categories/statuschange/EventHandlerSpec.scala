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

package io.renku.eventlog.events.categories
package statuschange

import cats.Show
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.events
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.EventPayload
import io.renku.eventlog.events.categories.statuschange.Generators._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventHandlerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    "decode a valid event and pass it to the events updater" in new TestCase {
      Seq(
        Gen
          .const(AllEventsToNew)
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None),
        toTriplesGeneratedEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> Some((event._1.payload -> event._1.schemaVersion).asJson.noSpaces)),
        toTripleStoreEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None),
        toFailureEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None),
        rollbackToNewEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None),
        rollbackToTriplesGeneratedEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None),
        toAwaitingDeletionEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(event => event -> None)
      ).map(_.generateOne) foreach { case ((event, eventAsString), maybePayload) =>
        handler.handle(events.EventRequestContent(event.asJson, maybePayload)).unsafeRunSync() shouldBe Accepted

        eventually {
          logger.loggedOnly(Info(s"$categoryName: $eventAsString -> Processed"),
                            Info(s"$categoryName: $eventAsString -> $Accepted")
          )
        }
        logger.reset()
      }
    }

    s"log an error if the events updater fails" in new TestCase {
      val exception = exceptions.generateOne
      val (event, eventAsString) =
        toTripleStoreEvents
          .map(stubUpdateStatuses(updateResult = exception.raiseError[IO, Unit]))
          .generateOne

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      eventually {
        logger.loggedOnly(Error(s"$categoryName: $eventAsString -> Failure", exception),
                          Info(s"$categoryName: $eventAsString -> $Accepted")
        )
      }
    }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)
  private trait TestCase {

    val logger        = TestLogger[IO]()
    val statusChanger = mock[StatusChanger[IO]]
    val queryExec     = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val handler       = new EventHandler[IO](categoryName, statusChanger, queryExec, logger)

    def stubUpdateStatuses[E <: StatusChangeEvent](updateResult: IO[Unit])(implicit show: Show[E]): E => (E, String) =
      event => {
        (statusChanger.updateStatuses[E](_: E)(_: DBUpdater[IO, E])).expects(event, *).returning(updateResult)
        (event, event.show)
      }
  }

  private implicit def eventEncoder[E <: StatusChangeEvent]: Encoder[E] = Encoder.instance[E] {
    case StatusChangeEvent.AllEventsToNew =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "newStatus":    "NEW"
    }"""
    case StatusChangeEvent.ToTriplesGenerated(eventId, path, processingTime, _, _) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_GENERATED",
      "processingTime": ${processingTime.value}
    }"""
    case StatusChangeEvent.ToTriplesStore(eventId, path, processingTime) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_STORE",
      "processingTime": ${processingTime.value}
    }"""
    case StatusChangeEvent.ToFailure(eventId, path, message, _, newStatus) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    ${newStatus.value},
      "message":      ${message.value}
    }"""
    case StatusChangeEvent.RollbackToNew(eventId, path) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "NEW"
    }"""
    case StatusChangeEvent.RollbackToTriplesGenerated(eventId, path) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_GENERATED"
    }"""
    case StatusChangeEvent.ToAwaitingDeletion(eventId, path) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "AWAITING_DELETION"
    }"""
  }

  private implicit lazy val payloadEncoder: Encoder[(EventPayload, SchemaVersion)] = Encoder.instance {
    case (payload, schemaVersion) => json"""{
      "payload": ${payload.value}, 
      "schemaVersion":${schemaVersion.value}
    }"""
  }
}
