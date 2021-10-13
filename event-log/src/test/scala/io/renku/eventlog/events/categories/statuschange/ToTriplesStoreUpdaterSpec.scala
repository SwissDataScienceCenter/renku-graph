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
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.timestamps
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus, ZippedEventPayload}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToTriplesStore
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToTriplesStoreUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all events in statuses before TRIPLES_STORE up to the current event with the status TRIPLES_STORE" in new TestCase {
      val eventDate = eventDates.generateOne

      val statusesToUpdate = Set(New,
                                 GeneratingTriples,
                                 GenerationRecoverableFailure,
                                 TriplesGenerated,
                                 TransformingTriples,
                                 TransformationRecoverableFailure
      )
      val eventsToUpdate = statusesToUpdate.map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate)))
      val eventsToSkip = EventStatus.all
        .diff(statusesToUpdate)
        .map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate))) +
        addEvent(TriplesGenerated, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      val event = addEvent(TransformingTriples, eventDate)

      val statusChangeEvent =
        ToTriplesStore(CompoundEventId(event._1, projectId), projectPath, eventProcessingTimes.generateOne)

      (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

      sessionResource.useK(dbUpdater updateDB statusChangeEvent).unsafeRunSync() shouldBe DBUpdateResults
        .ForProjects(
          projectPath,
          statusesToUpdate
            .map(_ -> -1)
            .toMap +
            (TransformingTriples -> (-1 /* for the event */ - 1 /* for the old TransformingTriples */ )) +
            (TriplesStore        -> (eventsToUpdate.size + 1 /* for the event */ ))
        )

      findFullEvent(CompoundEventId(event._1, projectId))
        .map { case (_, status, _, maybePayload, processingTimes) =>
          status        shouldBe TriplesStore
          maybePayload  shouldBe a[Some[_]]
          processingTimes should contain(statusChangeEvent.processingTime)
        }
        .getOrElse(fail("No event found for main event"))

      eventsToUpdate.map { case (eventId, status, _, originalPayload, originalProcessingTimes) =>
        findFullEvent(CompoundEventId(eventId, projectId))
          .map { case (_, status, maybeMessage, maybePayload, processingTimes) =>
            status          shouldBe TriplesStore
            maybeMessage    shouldBe None
            maybePayload    shouldBe originalPayload
            processingTimes shouldBe originalProcessingTimes
          }
          .getOrElse(fail(s"No event found with old $status status"))
      }

      eventsToSkip.map { case (eventId, originalStatus, originalMessage, originalPayload, originalProcessingTimes) =>
        findFullEvent(CompoundEventId(eventId, projectId))
          .map { case (_, status, maybeMessage, maybePayload, processingTimes) =>
            status          shouldBe originalStatus
            maybeMessage    shouldBe originalMessage
            maybePayload    shouldBe originalPayload
            processingTimes shouldBe originalProcessingTimes
          }
          .getOrElse(fail(s"No event found with old $originalStatus status"))
      }
    }
  }

  "onRollback" should {
    "clean the delivery info for the event" in new TestCase {
      val event = ToTriplesStore(compoundEventIds.generateOne, projectPath, eventProcessingTimes.generateOne)

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
    val dbUpdater           = new ToTriplesStoreUpdater[IO](deliveryInfoRemover, queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status:    EventStatus,
                 eventDate: EventDate
    ): (EventId, EventStatus, Option[EventMessage], Option[ZippedEventPayload], List[EventProcessingTime]) =
      storeGeneratedEvent(status, eventDate, projectId, projectPath)

    def findFullEvent(
        eventId: CompoundEventId
    ): Option[(EventId, EventStatus, Option[EventMessage], Option[ZippedEventPayload], List[EventProcessingTime])] = {
      val maybeEvent     = findEvent(eventId)
      val maybePayload   = findPayload(eventId).map(_._2)
      val processingTime = findProcessingTime(eventId)
      maybeEvent.map { case (_, status, maybeMessage) =>
        (eventId.id, status, maybeMessage, maybePayload, processingTime.map(_._2))
      }
    }
  }
}
