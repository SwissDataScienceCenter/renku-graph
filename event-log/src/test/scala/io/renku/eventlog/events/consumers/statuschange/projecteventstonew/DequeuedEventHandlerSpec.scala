/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
package projecteventstonew

import SkunkExceptionsGenerators._
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.events.consumers.statuschange.projecteventstonew.cleaning.ProjectCleaner
import io.renku.eventlog.events.producers.{SubscriptionProvisioning, minprojectinfo}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators.{categoryNames, subscriberIds, subscriberUrls}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestampsNotInTheFuture}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.interpreters.TestLogger.Level.Error
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk.SqlState.{DeadlockDetected, ForeignKeyViolation}

import java.time.Instant

class DequeuedEventHandlerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "updateDB" should {

    "change the status of all events of a specific project to NEW " +
      "except SKIPPED and GENERATING_TRIPLES (if delivered)" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        val eventsStatuses = Gen
          .oneOf(EventStatus.all diff Set(Skipped, GeneratingTriples, AwaitingDeletion, Deleting))
          .generateList(min = 2)

        for {
          _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

          eventsAndDates <- eventsStatuses.map { status =>
                              for {
                                event <- addEvent(status, project)
                                _     <- upsertEventDelivery(event.eventId, subscriberId)
                              } yield event.eventId -> event.eventDate
                            }.sequence
          events = eventsAndDates.map(_._1)

          skippedEventDate = timestampsNotInTheFuture.generateAs(EventDate)
          skippedEvent <- addEvent(Skipped, project, skippedEventDate)
          awaitingDeletionEventDate = timestampsNotInTheFuture.generateAs(EventDate)
          awaitingDeletionEvent <- addEvent(AwaitingDeletion, project, awaitingDeletionEventDate)
          deletingEvent         <- addEvent(Deleting, project)
          generatingTriplesEventDate = timestampsNotInTheFuture.generateAs(EventDate)
          generatingTriplesEvent <- addEvent(GeneratingTriples, project, generatingTriplesEventDate)
          _                      <- upsertEventDelivery(generatingTriplesEvent.eventId, subscriberId)

          otherProject = consumerProjects.generateOne
          eventStatus  = Gen.oneOf(EventStatus.all.diff(Set(Skipped, AwaitingDeletion))).generateOne
          otherProjectEvent <- addEvent(eventStatus, otherProject)
          _                 <- upsertEventDelivery(otherProjectEvent.eventId, subscriberId)

          _ <- upsertCategorySyncTime(project.id, minprojectinfo.categoryName)
          otherCategoryName = categoryNames.generateOne
          _ <- upsertCategorySyncTime(project.id, otherCategoryName)

          counts: Map[EventStatus, Int] =
            eventsStatuses
              .groupBy(identity)
              .map { case (eventStatus, statuses) => (eventStatus, -1 * statuses.length) }
              .updatedWith(EventStatus.New)(_.map(_ + events.size).orElse(Some(events.size)))
              .updated(AwaitingDeletion, -1)
              .updated(Deleting, -1)

          _ <- moduleSessionResource.session
                 .useKleisli(dbUpdater updateDB ProjectEventsToNew(project))
                 .asserting(_ shouldBe DBUpdateResults.ForProjects(project.slug, counts))

          _ <- events
                 .map(findFullEvent)
                 .sequence
                 .asserting(_ shouldBe events.map(eventId => (eventId.id, EventStatus.New, None, None, Nil).some))

          _ <- findEvent(skippedEvent.eventId).asserting(_.map(_.status) shouldBe Some(Skipped))
          _ <- findEvent(awaitingDeletionEvent.eventId).asserting(_.map(_.status) shouldBe None)
          _ <- findEvent(deletingEvent.eventId).asserting(_.map(_.status) shouldBe None)
          _ <- findEvent(generatingTriplesEvent.eventId).asserting(_.map(_.status) shouldBe Some(GeneratingTriples))
          _ <- findAllEventDeliveries.asserting(
                 _ should contain theSameElementsAs List(FoundDelivery(otherProjectEvent, subscriberId),
                                                         FoundDelivery(generatingTriplesEvent, subscriberId)
                 )
               )
          _ <- findCategorySyncTimes(project.id).asserting(_.map(_.name) shouldBe List(otherCategoryName))

          latestEventDate = (skippedEventDate :: generatingTriplesEventDate :: eventsAndDates.map(_._2)).max

          _ <- findProjects
                 .asserting(_.find(_.project.id == project.id).map(_.eventDate) shouldBe Some(latestEventDate))

          _ <- findEvent(otherProjectEvent.eventId).asserting(_.map(_.status) shouldBe Some(eventStatus))
        } yield Succeeded
      }

    "change the status of all events of a specific project to NEW except SKIPPED events " +
      "- case when there are no events left in the project" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne

        for {
          event1 <- addEvent(Deleting, project)
          event2 <- addEvent(Deleting, project)

          _ <- upsertCategorySyncTime(project.id, categoryNames.generateOne)

          _ = givenProjectCleaning(project, returning = ().pure[IO])

          _ <- moduleSessionResource(cfg).session
                 .useKleisli(dbUpdater.updateDB(ProjectEventsToNew(project)))
                 .asserting(_ shouldBe DBUpdateResults(project.slug, AwaitingDeletion -> 0, Deleting -> -2))

          _ <- findEvent(event1.eventId).asserting(_ shouldBe None)
          _ <- findEvent(event2.eventId).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "fail if the cleaning the project fails" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne

      for {
        _ <- addEvent(Deleting, project)
        _ <- addEvent(Deleting, project)
        exception = exceptions.generateOne
        _         = givenProjectCleaning(project, returning = exception.raiseError[IO, Unit])

        _ <- logger.resetF()

        _ <- moduleSessionResource(cfg).session
               .useKleisli(dbUpdater.updateDB(ProjectEventsToNew(project)))
               .assertThrowsError[Exception](_ shouldBe exception)

        _ <- logger.loggedOnlyF(
               Error(show"$categoryName: project clean up failed: ${project.show}; will retry", exception)
             )
      } yield Succeeded
    }
  }

  "onRollback" should {

    s"run the updateDB on db failure" in testDBResource.use { implicit cfg =>
      List(postgresErrors(DeadlockDetected).generateOne, postgresErrors(ForeignKeyViolation).generateOne)
        .traverse_ { failure =>
          val project = consumerProjects.generateOne
          for {
            event <- addEvent(Deleting, project)

            _ <- upsertCategorySyncTime(project.id, categoryNames.generateOne)

            _ = givenProjectCleaning(project, returning = ().pure[IO])

            _ <- (dbUpdater onRollback ProjectEventsToNew(project))
                   .apply(failure)
                   .asserting(_ shouldBe DBUpdateResults(project.slug, AwaitingDeletion -> 0, Deleting -> -1))

            _ <- findEvent(event.eventId).asserting(_ shouldBe None)
          } yield Succeeded
        }
    }
  }

  private def addEvent(status:    EventStatus,
                       project:   Project = consumerProjects.generateOne,
                       eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
  )(implicit cfg: DBConfig[EventLogDB]) =
    storeGeneratedEvent(status, eventDate, project)

  private def findFullEvent(eventId: CompoundEventId)(implicit cfg: DBConfig[EventLogDB]) =
    for {
      maybeEvent     <- findEvent(eventId)
      maybePayload   <- findPayload(eventId)
      processingTime <- findProcessingTimes(eventId)
    } yield maybeEvent.map { fe =>
      (eventId.id, fe.status, fe.maybeMessage, maybePayload.map(_.payload), processingTime.map(_.processingTime))
    }

  private lazy val now          = Instant.now()
  private lazy val subscriberId = subscriberIds.generateOne
  private val sourceUrl         = microserviceBaseUrls.generateOne
  private val subscriberUrl     = subscriberUrls.generateOne

  private lazy val projectCleaner = mock[ProjectCleaner[IO]]
  private def dbUpdater(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DequeuedEventHandler.DequeuedEventHandlerImpl[IO](projectCleaner, () => now)
  }

  private def givenProjectCleaning(project: Project, returning: IO[Unit]) =
    (projectCleaner.cleanUp _)
      .expects(project)
      .returning(Kleisli.liftF(returning))
}
