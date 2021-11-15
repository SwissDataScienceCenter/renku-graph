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
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventMessages
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.events.consumers.Project
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators.{categoryNames, compoundEventIds, eventBodies, eventProcessingTimes, lastSyncedDates, zippedEventPayloads}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random

class ProjectEventsToNewUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {
    "change the status of all events of a specific project to NEW except SKIPPED events" in new TestCase {

      val (projectId, projectPath) = projectIdentifiers.generateOne
      val eventsStatuses = Gen
        .oneOf(EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion, EventStatus.Deleting)))
        .generateNonEmptyList(2)

      val eventsAndDates = eventsStatuses
        .map { status =>
          val eventDate = timestampsNotInTheFuture.generateAs(EventDate)
          addEvent(status, projectId, projectPath, eventDate) -> eventDate
        }
        .map { case (id, eventDate) => CompoundEventId(id, projectId) -> eventDate }
        .toList

      val events = eventsAndDates.map(_._1)

      val skippedEventDate          = timestampsNotInTheFuture.generateAs(EventDate)
      val awaitingDeletionEventDate = timestampsNotInTheFuture.generateAs(EventDate)
      val skippedEvent              = addEvent(EventStatus.Skipped, projectId, projectPath, skippedEventDate)
      val awaitingDeletionEvent =
        addEvent(EventStatus.AwaitingDeletion, projectId, projectPath, awaitingDeletionEventDate)
      val deletingEvent = addEvent(EventStatus.Deleting, projectId, projectPath)

      val (otherProjectId, otherProjectPath) = projectIdentifiers.generateOne
      val eventStatus = Gen
        .oneOf(EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion)))
        .generateOne

      val otherProjectEventId = CompoundEventId(addEvent(eventStatus, otherProjectId, otherProjectPath), otherProjectId)

      events.foreach(upsertEventDelivery(_, subscriberId))
      upsertEventDelivery(otherProjectEventId, subscriberId)

      upsertCategorySyncTime(projectId, categoryNames.generateOne, lastSyncedDates.generateOne)

      val counts: Map[EventStatus, Int] =
        eventsStatuses.toList
          .groupBy(identity)
          .map { case (eventStatus, statuses) => (eventStatus, -1 * statuses.length) }
          .updatedWith(EventStatus.New) { maybeNewEvents =>
            maybeNewEvents.map(_ + events.size).orElse(Some(events.size))
          }

      sessionResource
        .useK(dbUpdater.updateDB(ProjectEventsToNew(Project(projectId, projectPath))))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(projectPath, counts)

      events.flatMap(findFullEvent) shouldBe events.map { eventId =>
        (eventId.id, EventStatus.New, None, None, List())
      }

      findEvent(CompoundEventId(skippedEvent, projectId)).map(_._2)          shouldBe Some(EventStatus.Skipped)
      findEvent(CompoundEventId(awaitingDeletionEvent, projectId)).map(_._2) shouldBe Some(EventStatus.AwaitingDeletion)
      findEvent(CompoundEventId(deletingEvent, projectId)).map(_._2)         shouldBe None
      findAllDeliveries                               shouldBe List(otherProjectEventId -> subscriberId)
      findProjectCategorySyncTimes(projectId).isEmpty shouldBe false

      val latestEventDate: EventDate =
        (List(skippedEventDate, awaitingDeletionEventDate) ::: eventsAndDates.map(_._2)).max

      findProjects.find { case (id, _, _) => id == projectId }.map(_._3) shouldBe Some(latestEventDate)

      findEvent(otherProjectEventId).map(_._2) shouldBe Some(eventStatus)
    }
    "change the status of all events of a specific project to NEW except SKIPPED events - case when there are no events left in the project" in new TestCase {

      val (projectId, projectPath) = projectIdentifiers.generateOne

      val event1 = addEvent(EventStatus.Deleting, projectId, projectPath)
      val event2 = addEvent(EventStatus.Deleting, projectId, projectPath)

      upsertCategorySyncTime(projectId, categoryNames.generateOne, lastSyncedDates.generateOne)

      sessionResource
        .useK(dbUpdater.updateDB(ProjectEventsToNew(Project(projectId, projectPath))))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(projectPath, Map.empty)

      findEvent(CompoundEventId(event1, projectId)) shouldBe None
      findEvent(CompoundEventId(event2, projectId)) shouldBe None

      findProjectCategorySyncTimes(projectId).isEmpty shouldBe true
      findProjects.find(p => p._1 == projectId)       shouldBe None

    }
  }

  private trait TestCase {

    lazy val projectIdentifiers = for {
      id   <- projectIds
      path <- projectPaths
    } yield (id, path)
    val subscriberId = subscriberIds.generateOne
    val currentTime  = mockFunction[Instant]
    upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
    val queriesExecTimes      = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater             = new ProjectEventsToNewUpdater[IO](queriesExecTimes, currentTime)
    val now                   = Instant.now()
    private val subscriberUrl = subscriberUrls.generateOne
    currentTime.expects().returning(now)
    private val sourceUrl = microserviceBaseUrls.generateOne

    def addEvent(status:      EventStatus,
                 projectId:   projects.Id = projectIds.generateOne,
                 projectPath: projects.Path = projectPaths.generateOne,
                 eventDate:   EventDate = timestampsNotInTheFuture.generateAs(EventDate)
    ): EventId = {
      val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId,
        status,
        timestamps.generateAs(ExecutionDate),
        eventDate,
        eventBodies.generateOne,
        maybeMessage = status match {
          case _: EventStatus.FailureStatus => eventMessages.generateSome
          case _ => eventMessages.generateOption
        },
        maybeEventPayload = status match {
          case EventStatus.TriplesStore | EventStatus.TriplesGenerated => zippedEventPayloads.generateSome
          case EventStatus.AwaitingDeletion                            => zippedEventPayloads.generateOption
          case _                                                       => zippedEventPayloads.generateNone
        },
        projectPath = projectPath
      )

      status match {
        case EventStatus.TriplesGenerated | EventStatus.TriplesStore =>
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        case EventStatus.AwaitingDeletion =>
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
