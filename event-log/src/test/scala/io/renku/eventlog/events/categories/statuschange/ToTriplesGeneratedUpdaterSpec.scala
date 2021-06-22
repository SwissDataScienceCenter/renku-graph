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
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{timestamps, timestampsNotInTheFuture}
import ch.datascience.graph.model.EventsGenerators.{eventBodies, eventIds, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
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
      val eventDate            = eventDates.generateOne
      val eventPayload         = eventPayloads.generateOne
      val eventProcessingTime  = eventProcessingTimes.generateOne
      val payloadSchemaVersion = projectSchemaVersions.generateOne

      val eventsBeforeToChange = EventStatus.all
        .diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion))
        .map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate)))
      val skippedEventIdBeforeEvent =
        addEvent(EventStatus.Skipped, timestamps(max = eventDate.value).generateAs(EventDate))
      val awaitingDeletionEventIdBeforeEvent =
        addEvent(EventStatus.AwaitingDeletion, timestamps(max = eventDate.value).generateAs(EventDate))

      val eventId = addEvent(EventStatus.GeneratingTriples,
                             eventDate,
                             eventProcessingTime.some,
                             eventPayload.some,
                             payloadSchemaVersion.some
      )

      val newEventIdAfterEvent =
        addEvent(EventStatus.New, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      sessionResource
        .useK {
          dbUpdater.updateDB(
            ToTriplesGenerated(CompoundEventId(eventId, projectId),
                               projectPath,
                               eventProcessingTime,
                               eventPayload,
                               payloadSchemaVersion
            )
          )
        }
        .unsafeRunSync() shouldBe DBUpdateResults.ForProject(
        projectPath,
        EventStatus.all
          .filterNot(_ == EventStatus.Skipped)
          .map(status => status -> 1)
          .toMap
      )

      eventsBeforeToChange.flatMap(eventId =>
        findFullEvent(CompoundEventId(eventId, projectId))
      ) shouldBe eventsBeforeToChange.map(eventId => (eventId, EventStatus.TriplesGenerated, None, None, List()))

      findEvent(CompoundEventId(skippedEventIdBeforeEvent, projectId)).map(_._2)          shouldBe Some(EventStatus.Skipped)
      findEvent(CompoundEventId(awaitingDeletionEventIdBeforeEvent, projectId)).map(_._2) shouldBe None
      findEvent(CompoundEventId(eventId, projectId)).map(_._2)                            shouldBe Some(EventStatus.TriplesGenerated)
      findEvent(CompoundEventId(newEventIdAfterEvent, projectId)).map(_._2)               shouldBe Some(EventStatus.New)

      val Some(
        (actualEventId, EventStatus.TriplesGenerated, None, Some(actualEventPayload), actualEventProcessingTimes)
      ) =
        findFullEvent(
          CompoundEventId(eventId, projectId)
        )
      actualEventId                                            shouldBe eventId
      actualEventPayload                                       shouldBe eventPayload
      actualEventProcessingTimes.contains(eventProcessingTime) shouldBe true
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

    def addEvent(status:              EventStatus,
                 eventDate:           EventDate,
                 maybeProcessingTime: Option[EventProcessingTime] = None,
                 maybePayload:        Option[EventPayload] = None,
                 maybeSchemaVersion:  Option[SchemaVersion] = None
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
        maybeEventPayload = (maybePayload, status) match {
          case (somePayload @ Some(_), _) => somePayload
          case (_, _: EventStatus.TriplesStore | EventStatus.TriplesGenerated) => eventPayloads.generateSome
          case (_, _: EventStatus.AwaitingDeletion) => eventPayloads.generateOption
          case _ => eventPayloads.generateNone
        },
        payloadSchemaVersion = maybeSchemaVersion match {
          case Some(schemaVersion) => schemaVersion
          case None                => projectSchemaVersions.generateOne
        }
      )

      (maybeProcessingTime, status) match {
        case (Some(processingTime), _) => upsertProcessingTime(eventId, status, processingTime)
        case (_, _: EventStatus.TriplesGenerated | EventStatus.TriplesStore) =>
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        case (_, _: EventStatus.AwaitingDeletion) =>
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
