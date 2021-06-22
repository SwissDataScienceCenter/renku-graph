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
import ch.datascience.generators.Generators.{timestamps, timestampsNotInTheFuture}
import ch.datascience.graph.model.EventsGenerators.{eventBodies, eventIds, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, eventPayloads}
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToTriplesGenerated
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random

class ToTriplesGeneratedUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all events up to the current event with the status TRIPLES_GENERATED" in new TestCase {
      val eventDate        = eventDates.generateOne
      val statusesToUpdate = Set(New, GeneratingTriples, GenerationRecoverableFailure, AwaitingDeletion)
      val eventsToUpdate   = statusesToUpdate.map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate)))
      val eventsToSkip = EventStatus.all
        .diff(statusesToUpdate)
        .map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate))) +
        addEvent(New, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      val event = addEvent(GeneratingTriples, eventDate)

      val statusChangeEvent = ToTriplesGenerated(CompoundEventId(event._1, projectId),
                                                 projectPath,
                                                 eventProcessingTimes.generateOne,
                                                 eventPayloads.generateOne,
                                                 projectSchemaVersions.generateOne
      )

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

      findFullEvent(CompoundEventId(event._1, projectId))
        .map { case (_, status, _, maybePayload, processingTimes) =>
          status        shouldBe TriplesGenerated
          maybePayload  shouldBe Some(statusChangeEvent.payload)
          processingTimes should contain(statusChangeEvent.processingTime)
        }
        .getOrElse(fail("No event found for main event"))

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
            status          shouldBe originalStatus
            maybeMessage    shouldBe originalMessage
            maybePayload    shouldBe originalPayload
            processingTimes shouldBe originalProcessingTimes
          }
          .getOrElse(fail(s"No event found with old $originalStatus status"))
      }
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new ToTriplesGeneratedUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status:    EventStatus,
                 eventDate: EventDate
    ): (EventId, EventStatus, Option[EventMessage], Option[EventPayload], List[EventProcessingTime]) = {
      val eventId = CompoundEventId(eventIds.generateOne, projectId)
      val maybeMessage = status match {
        case _: EventStatus.FailureStatus => eventMessages.generateSome
        case _ => eventMessages.generateOption
      }
      val maybePayload = status match {
        case TriplesStore | TriplesGenerated => eventPayloads.generateSome
        case AwaitingDeletion                => eventPayloads.generateOption
        case _                               => eventPayloads.generateNone
      }
      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        eventDate,
        eventBodies.generateOne,
        projectPath = projectPath,
        maybeMessage = maybeMessage,
        maybeEventPayload = maybePayload
      )

      val processingTimes = status match {
        case TriplesGenerated | EventStatus.TriplesStore => List(eventProcessingTimes.generateOne)
        case AwaitingDeletion =>
          if (Random.nextBoolean()) List(eventProcessingTimes.generateOne)
          else Nil
        case _ => Nil
      }
      processingTimes.foreach(upsertProcessingTime(eventId, status, _))

      (eventId.id, status, maybeMessage, maybePayload, processingTimes)
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
