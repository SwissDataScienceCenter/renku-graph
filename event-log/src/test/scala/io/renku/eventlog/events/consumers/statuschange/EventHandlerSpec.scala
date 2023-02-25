/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers
package statuschange

import cats.Show
import cats.effect.{Deferred, IO}
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.events.consumers.statuschange.Generators._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.json.JsonOps._
import io.renku.metrics.{MetricsRegistry, TestMetricsRegistry}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    "decode a valid event and pass it to the events updater" in new TestCase {
      Seq(
        "ToTriplesGenerated" -> toTriplesGeneratedEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(e => EventRequestContent.WithPayload[ZippedEventPayload](e.asJson, e.payload))),
        "ToTripleStore" -> toTripleStoreEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "ToFailure" -> toFailureEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "RollbackToNew" -> rollbackToNewEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "RollbackToTriplesGenerated" -> rollbackToTriplesGeneratedEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "ToAwaitingDeletion" -> toAwaitingDeletionEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "RollbackToAwaitingDeletion" -> rollbackToAwaitingDeletionEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "RedoProjectTransformation" -> redoProjectTransformationEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "ProjectEventToNew" -> projectEventsToNewEvents
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson))),
        "AllEventsToNew" -> Gen
          .const(AllEventsToNew)
          .map(stubUpdateStatuses(updateResult = ().pure[IO]))
          .map(toRequestContent(event => EventRequestContent.NoPayload(event.asJson)))
      ).map(_.bimap(identity, _.generateOne)) foreach {
        case (subcategory, (event, eventRequestContent, waitForUpdate, eventAsString)) =>
          withClue(s"$subcategory -> ") {
            handler
              .createHandlingDefinition(eventRequestContent)
              .unsafeRunSync()
              .process
              .value
              .unsafeRunSync() shouldBe Right(Accepted)

            waitForUpdate.get.unsafeRunSync()

            if (event.silent) logger.expectNoLogs()
            else logger.loggedOnly(Info(s"$categoryName: $eventAsString -> $Accepted"))
            logger.reset()
          }
      }
    }

    s"log an error if the events updater fails" in new TestCase {
      val exception = exceptions.generateOne
      val (event, _, eventAsString) = toTripleStoreEvents
        .map(stubUpdateStatuses(updateResult = exception.raiseError[IO, Unit]))
        .generateOne

      handler
        .createHandlingDefinition(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Right(Accepted)

      eventually {
        logger.loggedOnly(Error(s"$categoryName: $eventAsString -> Failure", exception),
                          Info(s"$categoryName: $eventAsString -> $Accepted")
        )
      }
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler
        .createHandlingDefinition(request)
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }
  }

  private trait TestCase {

    implicit val logger:                   TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  MetricsRegistry[IO]       = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    private val statusChanger       = mock[StatusChanger[IO]]
    private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
    private val eventsQueue         = mock[StatusChangeEventsQueue[IO]]
    val handler = new EventHandler[IO](categoryName, eventsQueue, statusChanger, deliveryInfoRemover)

    def stubUpdateStatuses[E <: StatusChangeEvent](
        updateResult: IO[Unit]
    )(implicit show: Show[E]): E => (E, Deferred[IO, Unit], String) =
      event => {
        val waitForUpdate = Deferred.unsafe[IO, Unit]
        (statusChanger
          .updateStatuses[E](_: E)(_: DBUpdater[IO, E]))
          .expects(event, *)
          .onCall(_ =>
            updateResult
              .flatTap(_ => waitForUpdate.complete(()))
              .recoverWith(_ => waitForUpdate.complete(()).void >> updateResult)
          )
        (event, waitForUpdate, event.show)
      }

    def toRequestContent[E <: StatusChangeEvent](
        f: E => EventRequestContent
    ): ((E, Deferred[IO, Unit], String)) => (E, EventRequestContent, Deferred[IO, Unit], String) = {
      case (event, waitForUpdate, eventShow) => (event, f(event), waitForUpdate, eventShow)
    }
  }

  private implicit def eventEncoder[E <: StatusChangeEvent]: Encoder[E] = Encoder.instance[E] {
    case StatusChangeEvent.AllEventsToNew => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "newStatus":    "NEW"
    }"""
    case StatusChangeEvent.ToTriplesGenerated(eventId, path, processingTime, _) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_GENERATED",
      "processingTime": ${processingTime.value}
    }"""
    case StatusChangeEvent.ToTriplesStore(eventId, path, processingTime) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_STORE",
      "processingTime": ${processingTime.value}
    }"""
    case StatusChangeEvent.ToFailure(eventId, path, message, _, newStatus, maybeExecutionDelay) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    ${newStatus.value},
      "message":      ${message.value}
    }""" addIfDefined ("executionDelay" -> maybeExecutionDelay)
    case StatusChangeEvent.RollbackToNew(eventId, path) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "NEW"
    }"""
    case StatusChangeEvent.RollbackToTriplesGenerated(eventId, path) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_GENERATED"
    }"""
    case StatusChangeEvent.ToAwaitingDeletion(eventId, path) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "AWAITING_DELETION"
    }"""
    case StatusChangeEvent.RollbackToAwaitingDeletion(Project(id, path)) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "project": {
        "id":         ${id.value},
        "path":       ${path.value}
      },
      "newStatus":    "AWAITING_DELETION"
    }"""
    case StatusChangeEvent.ProjectEventsToNew(project) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "project": {
        "id":         ${project.id.value},
        "path":       ${project.path.value}
      },
      "newStatus":    "NEW"
    }"""
    case StatusChangeEvent.RedoProjectTransformation(projectPath) => json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "project": {
        "path": ${projectPath.value}
      },
      "newStatus": "TRIPLES_GENERATED"
    }"""
  }
}
