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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventMessages
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.categories.statuschange.projectCleaner.ProjectCleaner
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes, lastSyncedDates, zippedEventPayloads}
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting, FailureStatus, Skipped, TriplesGenerated, TriplesStore}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.metrics.{LabeledGauge, TestLabeledHistogram}
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

      val project = consumerProjects.generateOne
      val eventsStatuses = Gen
        .oneOf(EventStatus.all.diff(Set(Skipped, AwaitingDeletion, Deleting)))
        .generateNonEmptyList(2)

      val eventsAndDates = eventsStatuses
        .map { status =>
          val eventDate = timestampsNotInTheFuture.generateAs(EventDate)
          addEvent(status, project, eventDate) -> eventDate
        }
        .map { case (id, eventDate) => CompoundEventId(id, project.id) -> eventDate }
        .toList

      val events = eventsAndDates.map(_._1)

      val skippedEventDate          = timestampsNotInTheFuture.generateAs(EventDate)
      val awaitingDeletionEventDate = timestampsNotInTheFuture.generateAs(EventDate)
      val skippedEvent              = addEvent(Skipped, project, skippedEventDate)
      val awaitingDeletionEvent     = addEvent(AwaitingDeletion, project, awaitingDeletionEventDate)
      val deletingEvent             = addEvent(Deleting, project)

      val otherProject = consumerProjects.generateOne
      val eventStatus  = Gen.oneOf(EventStatus.all.diff(Set(Skipped, AwaitingDeletion))).generateOne

      val otherProjectEventId = CompoundEventId(addEvent(eventStatus, otherProject), otherProject.id)

      events.foreach(upsertEventDelivery(_, subscriberId))
      upsertEventDelivery(otherProjectEventId, subscriberId)

      upsertCategorySyncTime(project.id, categoryNames.generateOne, lastSyncedDates.generateOne)

      val counts: Map[EventStatus, Int] =
        eventsStatuses.toList
          .groupBy(identity)
          .map { case (eventStatus, statuses) => (eventStatus, -1 * statuses.length) }
          .updatedWith(EventStatus.New) { maybeNewEvents =>
            maybeNewEvents.map(_ + events.size).orElse(Some(events.size))
          }
          .updated(AwaitingDeletion, -1)
          .updated(Deleting, -1)
      (awaitingDeletionGauge.update _).expects((project.path, -1d)).returning(().pure[IO])
      (deletingGauge.update _).expects((project.path, -1d)).returning(().pure[IO])

      sessionResource
        .useK(dbUpdater.updateDB(ProjectEventsToNew(project)))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path, counts)

      events.flatMap(findFullEvent) shouldBe events.map { eventId =>
        (eventId.id, EventStatus.New, None, None, List())
      }

      findEvent(CompoundEventId(skippedEvent, project.id)).map(_._2)          shouldBe Some(Skipped)
      findEvent(CompoundEventId(awaitingDeletionEvent, project.id)).map(_._2) shouldBe None
      findEvent(CompoundEventId(deletingEvent, project.id)).map(_._2)         shouldBe None
      findAllEventDeliveries shouldBe List(otherProjectEventId -> subscriberId)

      val latestEventDate: EventDate = (skippedEventDate :: eventsAndDates.map(_._2)).max

      findProjects.find { case (id, _, _) => id == project.id }.map(_._3) shouldBe Some(latestEventDate)

      findEvent(otherProjectEventId).map(_._2) shouldBe Some(eventStatus)
    }

    "change the status of all events of a specific project to NEW except SKIPPED events " +
      "- case when there are no events left in the project" in new TestCase {

        val project = consumerProjects.generateOne

        val event1 = addEvent(Deleting, project)
        val event2 = addEvent(Deleting, project)

        upsertCategorySyncTime(project.id, categoryNames.generateOne, lastSyncedDates.generateOne)

        (projectCleaner.cleanUp _).expects(project).returns(Kleisli.pure(()))

        (awaitingDeletionGauge.update _).expects((project.path, 0d)).returning(().pure[IO])
        (deletingGauge.update _).expects((project.path, -2d)).returning(().pure[IO])

        sessionResource
          .useK(dbUpdater.updateDB(ProjectEventsToNew(project)))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path,
                                                                Map(AwaitingDeletion -> 0, Deleting -> -2)
        )

        findEvent(CompoundEventId(event1, project.id)) shouldBe None
        findEvent(CompoundEventId(event2, project.id)) shouldBe None
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

        (awaitingDeletionGauge.update _).expects((project.path, 0d)).returning(().pure[IO])
        (deletingGauge.update _).expects((project.path, -2d)).returning(().pure[IO])

        sessionResource
          .useK(dbUpdater.updateDB(ProjectEventsToNew(project)))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path,
                                                                Map(AwaitingDeletion -> 0, Deleting -> -2)
        )

        findEvent(CompoundEventId(event1, project.id)) shouldBe None
        findEvent(CompoundEventId(event2, project.id)) shouldBe None

        logger.loggedOnly(Error(s"Clean up project failed: ${project.show}", exception))
      }
  }

  private def addEvent(status:    EventStatus,
                       project:   Project = consumerProjects.generateOne,
                       eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
  ): EventId = {
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

    eventId.id
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
    val queriesExecTimes      = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val awaitingDeletionGauge = mock[LabeledGauge[IO, projects.Path]]
    val deletingGauge         = mock[LabeledGauge[IO, projects.Path]]
    val projectCleaner        = mock[ProjectCleaner[IO]]
    val dbUpdater = new ProjectEventsToNewUpdaterImpl[IO](projectCleaner,
                                                          queriesExecTimes,
                                                          awaitingDeletionGauge,
                                                          deletingGauge,
                                                          currentTime
    )
    val now = Instant.now()

    currentTime.expects().returning(now)
  }
}
