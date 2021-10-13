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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.timestamps
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventProcessingTimes, zippedEventPayloads}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus, ZippedEventPayload}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToTriplesGenerated
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToTriplesGeneratedUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change statuses to TRIPLES_GENERATED of all events older than the current event" in new TestCase {
      val eventDate        = eventDates.generateOne
      val statusesToUpdate = Set(New, GeneratingTriples, GenerationRecoverableFailure, AwaitingDeletion)
      val eventsToUpdate   = statusesToUpdate.map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate)))
      val eventsToSkip = EventStatus.all
        .diff(statusesToUpdate)
        .map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate))) +
        addEvent(New, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      val event   = addEvent(GeneratingTriples, eventDate)
      val eventId = CompoundEventId(event._1, projectId)

      val statusChangeEvent =
        ToTriplesGenerated(eventId, projectPath, eventProcessingTimes.generateOne, zippedEventPayloads.generateOne)

      (deliveryInfoRemover.deleteDelivery _).expects(eventId).returning(Kleisli.pure(()))

      sessionResource
        .useK(dbUpdater updateDB statusChangeEvent)
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        projectPath,
        statusesToUpdate
          .map(_ -> -1)
          .toMap +
          (GeneratingTriples -> (-1 /* for the event */ - 1 /* for the old GeneratingTriples */ )) +
          (TriplesGenerated  -> (eventsToUpdate.size + 1 /* for the event */ - 1 /* for the AwaitingDeletion */ ))
      )

      findFullEvent(eventId)
        .map {
          case (_, status, _, Some(payload), processingTimes) =>
            status        shouldBe TriplesGenerated
            payload.value   should contain theSameElementsAs statusChangeEvent.payload.value
            processingTimes should contain(statusChangeEvent.processingTime)
          case _ => fail("No payload found for the main event")
        }
        .getOrElse(fail("No event found for the main event"))

      eventsToUpdate.map {
        case (eventId, AwaitingDeletion, _, _, _) => findFullEvent(CompoundEventId(eventId, projectId)) shouldBe None
        case (eventId, status, _, _, _) =>
          findFullEvent(CompoundEventId(eventId, projectId))
            .map { case (_, status, _, maybePayload, processingTimes) =>
              status          shouldBe TriplesGenerated
              processingTimes shouldBe Nil
              maybePayload    shouldBe None
            }
            .getOrElse(fail(s"No event found with old $status status"))
      }

      eventsToSkip.map { case (eventId, originalStatus, originalMessage, originalPayload, originalProcessingTimes) =>
        findFullEvent(CompoundEventId(eventId, projectId))
          .map { case (_, status, maybeMessage, maybePayload, processingTimes) =>
            status       shouldBe originalStatus
            maybeMessage shouldBe originalMessage
            (maybePayload, originalPayload).mapN { (actualPayload, toSkipPayload) =>
              actualPayload.value should contain theSameElementsAs toSkipPayload.value
            }
            processingTimes shouldBe originalProcessingTimes
          }
          .getOrElse(fail(s"No event found with old $originalStatus status"))
      }
    }

    "just clean the delivery info if the event is already in the TRIPLES_GENERATED" in new TestCase {
      val eventDate = eventDates.generateOne
      val (olderEventId, olderEventStatus, _, _, _) =
        addEvent(New, timestamps(max = eventDate.value).generateAs(EventDate))

      val (eventId, _, _, existingPayload, _) = addEvent(TriplesGenerated, eventDate)

      val statusChangeEvent = ToTriplesGenerated(CompoundEventId(eventId, projectId),
                                                 projectPath,
                                                 eventProcessingTimes.generateOne,
                                                 zippedEventPayloads.generateOne
      )

      (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

      sessionResource
        .useK(dbUpdater updateDB statusChangeEvent)
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty

      findFullEvent(statusChangeEvent.eventId)
        .map { case (_, status, _, maybePayload, processingTimes) =>
          status shouldBe TriplesGenerated
          (maybePayload, existingPayload).mapN { (actualPayload, payload) =>
            actualPayload.value should contain theSameElementsAs payload.value
          }
          processingTimes should not contain statusChangeEvent.processingTime
        }
        .getOrElse(fail("No event found for the main event"))

      findFullEvent(CompoundEventId(olderEventId, projectId))
        .map { case (_, status, _, maybePayload, processingTimes) =>
          status          shouldBe olderEventStatus
          processingTimes shouldBe Nil
          maybePayload    shouldBe None
        }
        .getOrElse(fail(s"No event found with old $olderEventStatus status"))
    }
  }

  "onRollback" should {
    "clean the delivery info for the event" in new TestCase {
      val event = ToTriplesGenerated(compoundEventIds.generateOne,
                                     projectPath,
                                     eventProcessingTimes.generateOne,
                                     zippedEventPayloads.generateOne
      )

      (deliveryInfoRemover.deleteDelivery _).expects(event.eventId).returning(Kleisli.pure(()))

      sessionResource
        .useK(dbUpdater onRollback event)
        .unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime         = mockFunction[Instant]
    val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
    val queriesExecTimes    = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater           = new ToTriplesGeneratedUpdater[IO](deliveryInfoRemover, queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status:    EventStatus,
                 eventDate: EventDate
    ): (EventId, EventStatus, Option[EventMessage], Option[ZippedEventPayload], List[EventProcessingTime]) =
      storeGeneratedEvent(status, eventDate, projectId, projectPath)

    def findFullEvent(eventId: CompoundEventId) = {
      val maybeEvent     = findEvent(eventId)
      val maybePayload   = findPayload(eventId).map(_._2)
      val processingTime = findProcessingTime(eventId)
      maybeEvent map { case (_, status, maybeMessage) =>
        (eventId.id, status, maybeMessage, maybePayload, processingTime.map(_._2))
      }
    }
  }
}
