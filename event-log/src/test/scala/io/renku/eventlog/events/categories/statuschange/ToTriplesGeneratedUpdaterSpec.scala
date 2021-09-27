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
import ch.datascience.generators.Generators.timestamps
import ch.datascience.graph.model.EventsGenerators.eventProcessingTimes
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, eventPayloads}
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
        .getOrElse(fail("No event found for the main event"))

      eventsToUpdate.map {
        case (eventId, AwaitingDeletion, _, _, _, _) => findFullEvent(CompoundEventId(eventId, projectId)) shouldBe None
        case (eventId, status, _, _, _, _) =>
          findFullEvent(CompoundEventId(eventId, projectId))
            .map { case (_, status, _, maybePayload, processingTimes) =>
              status          shouldBe TriplesGenerated
              processingTimes shouldBe Nil
              maybePayload    shouldBe None
            }
            .getOrElse(fail(s"No event found with old $status status"))
      }

      eventsToSkip.map { case (eventId, originalStatus, originalMessage, originalPayload, _, originalProcessingTimes) =>
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

    "do nothing if the event is already in the TRIPLES_GENERATED" in new TestCase {
      val eventDate = eventDates.generateOne
      val (olderEventId, olderEventStatus, _, _, _, _) =
        addEvent(New, timestamps(max = eventDate.value).generateAs(EventDate))

      val (eventId, _, _, existingPayload, Some(schemaVersion), _) = addEvent(TriplesGenerated, eventDate)

      val statusChangeEvent = ToTriplesGenerated(CompoundEventId(eventId, projectId),
                                                 projectPath,
                                                 eventProcessingTimes.generateOne,
                                                 eventPayloads.generateOne,
                                                 schemaVersion
      )

      sessionResource
        .useK(dbUpdater updateDB statusChangeEvent)
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty

      findFullEvent(CompoundEventId(eventId, projectId))
        .map { case (_, status, _, maybePayload, processingTimes) =>
          status        shouldBe TriplesGenerated
          maybePayload  shouldBe existingPayload
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

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new ToTriplesGeneratedUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status: EventStatus, eventDate: EventDate): (EventId,
                                                              EventStatus,
                                                              Option[EventMessage],
                                                              Option[EventPayload],
                                                              Option[SchemaVersion],
                                                              List[EventProcessingTime]
    ) = storeGeneratedEvent(status, eventDate, projectId, projectPath)

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
