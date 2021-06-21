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
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, eventPayloads}
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AncestorsToTriplesGenerated
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random

class AncestorsToTriplesGeneratedUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all events up to the current event with the status TRIPLES_GENERATED" in new TestCase {
      val eventDate = eventDates.generateOne

      val eventsBeforeToChange = EventStatus.all
        .diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion))
        .map(addEvent(_, timestamps(max = eventDate.value).generateAs(EventDate)))
      val skippedEventIdBeforeEvent =
        addEvent(EventStatus.Skipped, timestamps(max = eventDate.value).generateAs(EventDate))
      val awaitingDeletionEventIdBeforeEvent =
        addEvent(EventStatus.AwaitingDeletion, timestamps(max = eventDate.value).generateAs(EventDate))

      val eventId = addEvent(EventStatus.TriplesGenerated, eventDate)

      val newEventIdAfterEvent =
        addEvent(EventStatus.New, timestamps(min = eventDate.value, max = now).generateAs(EventDate))

      sessionResource
        .useK {
          dbUpdater.updateDB(AncestorsToTriplesGenerated(CompoundEventId(eventId, projectId), projectPath))
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

      val Some((_, EventStatus.TriplesGenerated, _, maybePayload, processingTimes)) =
        findFullEvent(CompoundEventId(eventId, projectId))
      maybePayload             shouldBe a[Some[_]]
      processingTimes.nonEmpty shouldBe true
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new AncestorsToTriplesGeneratedUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now)

    def addEvent(status: EventStatus, eventDate: EventDate): EventId = {
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
          case _: EventStatus.TriplesStore | EventStatus.TriplesGenerated => eventPayloads.generateSome
          case _: EventStatus.AwaitingDeletion => eventPayloads.generateOption
          case _ => eventPayloads.generateNone
        }
      )

      status match {
        case _: EventStatus.TriplesGenerated | EventStatus.TriplesStore =>
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        case _: EventStatus.AwaitingDeletion =>
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
