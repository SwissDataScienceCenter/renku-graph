/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventMessages
import io.renku.eventlog._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.projectCleaner.ProjectCleaner
import io.renku.eventlog.subscriptions.{SubscriptionDataProvisioning, minprojectinfo}
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes, lastSyncedDates, zippedEventPayloads}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
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
    with SubscriptionDataProvisioning
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all events of a specific project to NEW " +
      "except SKIPPED and GENERATING_TRIPLES (if delivered)" in new TestCase {

        val project = consumerProjects.generateOne
        val eventsStatuses = Gen
          .oneOf(EventStatus.all diff Set(Skipped, GeneratingTriples, AwaitingDeletion, Deleting))
          .generateList(minElements = 2)

        val eventsAndDates = eventsStatuses.map { status =>
          val eventDate = timestampsNotInTheFuture.generateAs(EventDate)
          val id        = addEvent(status, project, eventDate)
          upsertEventDelivery(id, subscriberId)
          id -> eventDate
        }

        val events = eventsAndDates.map(_._1)

        val skippedEventDate           = timestampsNotInTheFuture.generateAs(EventDate)
        val skippedEvent               = addEvent(Skipped, project, skippedEventDate)
        val awaitingDeletionEventDate  = timestampsNotInTheFuture.generateAs(EventDate)
        val awaitingDeletionEvent      = addEvent(AwaitingDeletion, project, awaitingDeletionEventDate)
        val deletingEvent              = addEvent(Deleting, project)
        val generatingTriplesEventDate = timestampsNotInTheFuture.generateAs(EventDate)
        val generatingTriplesEvent     = addEvent(GeneratingTriples, project, generatingTriplesEventDate)
        upsertEventDelivery(generatingTriplesEvent, subscriberId)

        val otherProject      = consumerProjects.generateOne
        val eventStatus       = Gen.oneOf(EventStatus.all.diff(Set(Skipped, AwaitingDeletion))).generateOne
        val otherProjectEvent = addEvent(eventStatus, otherProject)
        upsertEventDelivery(otherProjectEvent, subscriberId)

        upsertCategorySyncTime(project.id, minprojectinfo.categoryName, lastSyncedDates.generateOne)
        val otherCategoryName: CategoryName = categoryNames.generateOne
        upsertCategorySyncTime(project.id, otherCategoryName, lastSyncedDates.generateOne)

        val counts: Map[EventStatus, Int] = eventsStatuses
          .groupBy(identity)
          .map { case (eventStatus, statuses) => (eventStatus, -1 * statuses.length) }
          .updatedWith(EventStatus.New) { maybeNewEvents =>
            maybeNewEvents.map(_ + events.size).orElse(Some(events.size))
          }
          .updated(AwaitingDeletion, -1)
          .updated(Deleting, -1)

        sessionResource
          .useK(dbUpdater updateDB ProjectEventsToNew(project))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path, counts)

        events.flatMap(findFullEvent) shouldBe events.map { eventId =>
          (eventId.id, EventStatus.New, None, None, Nil)
        }

        findEvent(skippedEvent).map(_._2)           shouldBe Some(Skipped)
        findEvent(awaitingDeletionEvent).map(_._2)  shouldBe None
        findEvent(deletingEvent).map(_._2)          shouldBe None
        findEvent(generatingTriplesEvent).map(_._2) shouldBe Some(GeneratingTriples)
        findAllEventDeliveries should contain theSameElementsAs List(otherProjectEvent -> subscriberId,
                                                                     generatingTriplesEvent -> subscriberId
        )
        findProjectCategorySyncTimes(project.id).map(_._1) shouldBe List(otherCategoryName)

        val latestEventDate = (skippedEventDate :: generatingTriplesEventDate :: eventsAndDates.map(_._2)).max

        findProjects.find(_._1 == project.id).map(_._3) shouldBe Some(latestEventDate)

        findEvent(otherProjectEvent).map(_._2) shouldBe Some(eventStatus)
      }

    "change the status of all events of a specific project to NEW except SKIPPED events " +
      "- case when there are no events left in the project" in new TestCase {

        val project = consumerProjects.generateOne

        val event1 = addEvent(Deleting, project)
        val event2 = addEvent(Deleting, project)

        upsertCategorySyncTime(project.id, categoryNames.generateOne, lastSyncedDates.generateOne)

        (projectCleaner.cleanUp _).expects(project).returns(Kleisli.pure(()))

        sessionResource
          .useK(dbUpdater.updateDB(ProjectEventsToNew(project)))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path,
                                                                Map(AwaitingDeletion -> 0, Deleting -> -2)
        )

        findEvent(event1) shouldBe None
        findEvent(event2) shouldBe None
      }

    "change the status of all events of a specific project to NEW except SKIPPED events " +
      "- case when there are no events left in the project and cleaning the project fails" in new TestCase {

        val exception = exceptions.generateOne
        val project   = consumerProjects.generateOne

        val event1 = addEvent(Deleting, project)
        val event2 = addEvent(Deleting, project)

        (projectCleaner.cleanUp _)
          .expects(project)
          .returning(Kleisli.liftF(exception.raiseError[IO, Unit]))

        sessionResource
          .useK(dbUpdater.updateDB(ProjectEventsToNew(project)))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path,
                                                                Map(AwaitingDeletion -> 0, Deleting -> -2)
        )

        findEvent(event1) shouldBe None
        findEvent(event2) shouldBe None

        logger.loggedOnly(Error(s"Clean up project failed: ${project.show}", exception))
      }
  }

  private def addEvent(status:    EventStatus,
                       project:   Project = consumerProjects.generateOne,
                       eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
  ): CompoundEventId = {
    val eventId = compoundEventIds.generateOne.copy(projectId = project.id)
    storeEvent(
      eventId,
      status,
      timestamps.generateAs(ExecutionDate),
      eventDate,
      eventBodies.generateOne,
      maybeMessage = status match {
        case _: FailureStatus => eventMessages.generateSome
        case _ => eventMessages.generateOption
      },
      maybeEventPayload = status match {
        case TriplesStore | TriplesGenerated => zippedEventPayloads.generateSome
        case AwaitingDeletion                => zippedEventPayloads.generateOption
        case _                               => zippedEventPayloads.generateNone
      },
      projectPath = project.path
    )

    status match {
      case TriplesGenerated | TriplesStore =>
        upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
      case AwaitingDeletion =>
        if (Random.nextBoolean())
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        else ()
      case _ => ()
    }

    eventId
  }

  private def findFullEvent(eventId: CompoundEventId) = {
    val maybeEvent     = findEvent(eventId)
    val maybePayload   = findPayload(eventId).map(_._2)
    val processingTime = findProcessingTime(eventId)
    maybeEvent.map { case (_, status, maybeMessage) =>
      (eventId.id, status, maybeMessage, maybePayload, processingTime.map(_._2))
    }
  }

  private trait TestCase {
    val currentTime = mockFunction[Instant]

    val subscriberId  = subscriberIds.generateOne
    val sourceUrl     = microserviceBaseUrls.generateOne
    val subscriberUrl = subscriberUrls.generateOne
    upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val projectCleaner   = mock[ProjectCleaner[IO]]
    val dbUpdater        = new ProjectEventsToNewUpdaterImpl[IO](projectCleaner, queriesExecTimes, currentTime)
    val now              = Instant.now()

    currentTime.expects().returning(now)
  }
}
