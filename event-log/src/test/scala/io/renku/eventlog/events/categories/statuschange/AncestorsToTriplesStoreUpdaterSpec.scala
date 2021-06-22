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

import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{positiveInts, timestamps, timestampsNotInTheFuture}
import ch.datascience.graph.model.EventsGenerators.{eventBodies, eventIds, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.EventStatus.FailureStatus
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, eventPayloads}
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AncestorsToTriplesStore
import io.renku.eventlog.{EventDate, ExecutionDate, InMemoryEventLogDbSpec, TypeSerializers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random
import cats.syntax.all._

class AncestorsToTriplesStoreUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all TRIPLES_GENERATED events up to the current event with the status TRIPLES_STORE" in new TestCase {
      val eventDate           = eventDates.generateOne
      val eventProcessingTime = eventProcessingTimes.generateOne

      val eventsBeforeNotToChange = EventStatus.all
        .filterNot(_ == EventStatus.TriplesGenerated)
        .map(status => addEvent(status, timestamps(max = eventDate.value).generateAs(EventDate)) -> status)

      val eventsBeforeToChange = (1 to positiveInts(max = 10).generateOne).map { _ =>
        addEvent(EventStatus.TriplesGenerated, timestamps(max = eventDate.value).generateAs(EventDate))
      }

      val eventId = addEvent(EventStatus.TransformingTriples, eventDate, eventProcessingTime.some)

      val newEventIdAfterEvent =
        addEvent(EventStatus.New, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      sessionResource
        .useK {
          dbUpdater.updateDB(
            AncestorsToTriplesStore(CompoundEventId(eventId, projectId), projectPath, eventProcessingTime)
          )
        }
        .unsafeRunSync() shouldBe DBUpdateResults.ForProject(
        projectPath,
        Map(EventStatus.TriplesGenerated -> eventsBeforeToChange.size)
      )

      eventsBeforeNotToChange.map { case (eventId, originalStatus) =>
        findFullEvent(CompoundEventId(eventId, projectId)) -> originalStatus
      } foreach {
        case (Some((_, EventStatus.TriplesStore, _, Some(_), processingTimes)), EventStatus.TriplesStore) =>
          processingTimes.nonEmpty shouldBe true
        case (Some((_, actualStatus: FailureStatus, Some(_), _, _)), originalStatus) =>
          actualStatus shouldBe originalStatus
        case (Some((_, actualStatus, _, _, _)), originalStatus) =>
          actualStatus shouldBe originalStatus
        case (None, originalStatus) => fail(s"Event not found with status $originalStatus")
      }

      eventsBeforeToChange.flatMap(eventId => findFullEvent(CompoundEventId(eventId, projectId))) foreach {
        case (_, EventStatus.TriplesStore, None, Some(_), processingTimes) =>
          processingTimes.nonEmpty shouldBe true
        case _ => fail("Events to change were not in TriplesStore status")
      }

      findEvent(CompoundEventId(newEventIdAfterEvent, projectId)).map(_._2) shouldBe Some(EventStatus.New)

      val Some((_, EventStatus.TriplesStore, _, maybePayload, processingTimes)) =
        findFullEvent(CompoundEventId(eventId, projectId))
      maybePayload                                  shouldBe a[Some[_]]
      processingTimes.contains(eventProcessingTime) shouldBe true
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new AncestorsToTriplesStoreUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status:                   EventStatus,
                 eventDate:                EventDate,
                 maybeEventProcessingTime: Option[EventProcessingTime] = None
    ): EventId = {
      val eventId = CompoundEventId(eventIds.generateOne, projectId)

      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        eventDate,
        eventBodies.generateOne,
        projectPath = projectPath,
        maybeMessage = status match {
          case _: EventStatus.FailureStatus => eventMessages.generateSome
          case _ => eventMessages.generateOption
        },
        maybeEventPayload = status match {
          case EventStatus.TransformingTriples | EventStatus.TriplesStore | EventStatus.TriplesGenerated =>
            eventPayloads.generateSome
          case EventStatus.AwaitingDeletion => eventPayloads.generateOption
          case _                            => eventPayloads.generateNone
        }
      )

      (maybeEventProcessingTime, status) match {
        case (Some(processingTime), _) => upsertProcessingTime(eventId, status, processingTime)
        case (_, EventStatus.TriplesGenerated | EventStatus.TriplesStore) =>
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        case (_, EventStatus.AwaitingDeletion) =>
          if (Random.nextBoolean()) {
            upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
          } else ()
        case _ => ()
      }

      eventId.id
    }

    def findFullEvent(eventId: CompoundEventId) = {
      val maybeEvent     = findEvent(eventId)
      val maybePayload   = findPayload(eventId).map(_._2)
      val processingTime = findProcessingTime(eventId)
      maybeEvent.map { case (_, status, maybeMessage) =>
        (eventId.id, status, maybeMessage, maybePayload, processingTime.map(_._2))
      }
    }

  }
}
