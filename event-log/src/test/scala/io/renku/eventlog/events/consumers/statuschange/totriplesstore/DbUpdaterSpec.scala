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

package io.renku.eventlog.events.consumers.statuschange.totriplesstore

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.ToTriplesStore
import io.renku.eventlog.events.consumers.statuschange.SkunkExceptionsGenerators.postgresErrors
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DeliveryInfoRemover}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps}
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators
import io.renku.graph.model.EventsGenerators.eventProcessingTimes
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import skunk.SqlState

import java.time.Instant

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  "updateDB" should {

    "change the status of all events in statuses before TRIPLES_STORE up to the current event with the status TRIPLES_STORE" in testDBResource
      .use { implicit cfg =>
        val project   = consumerProjects.generateOne
        val eventDate = eventDates.generateOne
        for {
          eventsToUpdate <-
            statusesToUpdate
              .map(storeGeneratedEvent(_, timestamps(max = eventDate.value).generateAs(EventDate), project))
              .sequence

          eventsToSkip <-
            (EventStatus.all.toList diff statusesToUpdate)
              .map(storeGeneratedEvent(_, timestamps(max = eventDate.value).generateAs(EventDate), project))
              .sequence >>= (events =>
              storeGeneratedEvent(TriplesGenerated,
                                  timestamps(min = eventDate.value, max = now).generateAs(EventDate),
                                  project
              ).map(_ :: events)
            )

          event <- storeGeneratedEvent(TransformingTriples, eventDate, project)

          statusChangeEvent = ToTriplesStore(event.eventId.id, project, eventProcessingTimes.generateOne)

          _ = givenDeliveryInfoRemoved(statusChangeEvent.eventId)

          _ <- moduleSessionResource.session.useKleisli(dbUpdater updateDB statusChangeEvent).asserting {
                 _ shouldBe DBUpdateResults.ForProjects(
                   project.slug,
                   statusesToUpdate
                     .map(_ -> -1)
                     .toMap +
                     (TransformingTriples -> (-1 /* for the event */ - 1 /* for the old TransformingTriples */ )) +
                     (TriplesStore        -> (eventsToUpdate.size + 1 /* for the event */ ))
                 )
               }

          _ <- findFullEvent(event.eventId)
                 .map(_.value)
                 .asserting { case (_, status, _, maybePayload, processingTimes) =>
                   status        shouldBe TriplesStore
                   maybePayload  shouldBe a[Some[_]]
                   processingTimes should contain(statusChangeEvent.processingTime)
                 }

          _ <- eventsToUpdate.map { ge =>
                 findFullEvent(ge.eventId).map(_.value).asserting {
                   case (_, status, maybeMessage, maybePayload, processingTimes) =>
                     status               shouldBe TriplesStore
                     maybeMessage         shouldBe None
                     (maybePayload.map(_.value) -> ge.maybePayload.map(_.value))
                       .mapN(_ should contain theSameElementsAs _)
                     processingTimes shouldBe ge.processingTimes
                 }
               }.sequence

          _ <- eventsToSkip.map { ge =>
                 findFullEvent(ge.eventId).map(_.value).asserting {
                   case (_, status, maybeMessage, maybePayload, processingTimes) =>
                     status               shouldBe ge.status
                     maybeMessage         shouldBe ge.maybeMessage
                     (maybePayload.map(_.value) -> ge.maybePayload.map(_.value))
                       .mapN(_ should contain theSameElementsAs _)
                     processingTimes shouldBe ge.processingTimes
                 }
               }.sequence
        } yield Succeeded
      }

    "change the status of all events older than the current event but not the events with the same date" in testDBResource
      .use { implicit cfg =>
        val project   = consumerProjects.generateOne
        val eventDate = eventDates.generateOne
        for {
          event1 <- storeGeneratedEvent(TransformingTriples, eventDate, project)
          event2 <- storeGeneratedEvent(TriplesGenerated, eventDate, project)

          statusChangeEvent = ToTriplesStore(event1.eventId.id, project, eventProcessingTimes.generateOne)

          _ = givenDeliveryInfoRemoved(statusChangeEvent.eventId)

          _ <- moduleSessionResource.session.useKleisli(dbUpdater updateDB statusChangeEvent).asserting {
                 _ shouldBe DBUpdateResults(project.slug, TransformingTriples -> -1, TriplesStore -> 1)
               }

          _ <- findFullEvent(event1.eventId)
                 .map(_.value)
                 .asserting { case (_, status, _, maybePayload, processingTimes) =>
                   status        shouldBe TriplesStore
                   maybePayload  shouldBe a[Some[_]]
                   processingTimes should contain(statusChangeEvent.processingTime)
                 }

          _ <- findFullEvent(event2.eventId).asserting(_.map(_._2) shouldBe TriplesGenerated.some)
        } yield Succeeded
      }

    (EventStatus.all - TransformingTriples) foreach { invalidStatus =>
      s"do nothing if event in $invalidStatus" in testDBResource.use { implicit cfg =>
        val latestEventDate = eventDates.generateOne
        val project         = consumerProjects.generateOne
        for {
          event <- storeGeneratedEvent(invalidStatus, latestEventDate, project)
          ancestorEvent <- storeGeneratedEvent(
                             TransformingTriples,
                             timestamps(max = latestEventDate.value.minusSeconds(1)).generateAs(EventDate),
                             project
                           )

          statusChangeEvent = ToTriplesStore(event.eventId.id, project, eventProcessingTimes.generateOne)

          _ = givenDeliveryInfoRemoved(statusChangeEvent.eventId)

          _ <- moduleSessionResource.session
                 .useKleisli(dbUpdater updateDB statusChangeEvent)
                 .asserting(_ shouldBe DBUpdateResults.empty)

          _ <- findFullEvent(event.eventId).asserting(_.map(_._2) shouldBe invalidStatus.some)
          _ <- findFullEvent(ancestorEvent.eventId) asserting (_.map(_._2) shouldBe TransformingTriples.some)
        } yield Succeeded
      }
    }
  }

  "onRollback" should {

    "retry the updateDB procedure on DeadlockDetected" in testDBResource.use { implicit cfg =>
      val eventDate = eventDates.generateOne
      val project   = consumerProjects.generateOne

      for {
        // event to update =
        _ <- storeGeneratedEvent(New, timestamps(max = eventDate.value).generateAs(EventDate), project)

        // event to skip
        _ <- storeGeneratedEvent((EventStatus.all.toList diff statusesToUpdate).head,
                                 timestamps(max = eventDate.value).generateAs(EventDate),
                                 project
             )

        event <- storeGeneratedEvent(TransformingTriples, eventDate, project)

        statusChangeEvent = ToTriplesStore(event.eventId.id, project, eventProcessingTimes.generateOne)

        _ = givenDeliveryInfoRemoved(statusChangeEvent.eventId)

        deadlockException = postgresErrors(SqlState.DeadlockDetected).generateOne
        _ <- (dbUpdater onRollback statusChangeEvent).apply(deadlockException).asserting {
               _ shouldBe DBUpdateResults(
                 project.slug,
                 New                 -> -1 /* for the event to update */,
                 TransformingTriples -> -1 /* for the event */,
                 TriplesStore        -> 2 /* event to update + the event */
               )
             }

        _ <- findFullEvent(event.eventId).map(_.value).asserting { case (_, status, _, maybePayload, processingTimes) =>
               status        shouldBe TriplesStore
               maybePayload  shouldBe a[Some[_]]
               processingTimes should contain(statusChangeEvent.processingTime)
             }
      } yield Succeeded
    }

    "clean the delivery info for the event when Exception different than DeadlockDetected " +
      "and rethrow the exception" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        val event   = ToTriplesStore(EventsGenerators.eventIds.generateOne, project, eventProcessingTimes.generateOne)

        givenDeliveryInfoRemoved(event.eventId)

        val exception = exceptions.generateOne
        (dbUpdater onRollback event).apply(exception).assertThrowsError[Exception](_ shouldBe exception)
      }
  }

  private lazy val statusesToUpdate = List(New,
                                           GeneratingTriples,
                                           GenerationRecoverableFailure,
                                           TriplesGenerated,
                                           TransformingTriples,
                                           TransformationRecoverableFailure
  )

  private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
  private lazy val now            = Instant.now()
  private val dbUpdater = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DbUpdater[IO](deliveryInfoRemover, () => now)
  }

  private def givenDeliveryInfoRemoved(eventId: CompoundEventId) =
    (deliveryInfoRemover.deleteDelivery _).expects(eventId).returning(Kleisli.pure(()))

  private def findFullEvent(eventId: CompoundEventId)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Option[(EventId, EventStatus, Option[EventMessage], Option[ZippedEventPayload], List[EventProcessingTime])]] =
    for {
      maybeEvent      <- findEvent(eventId)
      maybePayload    <- findPayload(eventId)
      processingTimes <- findProcessingTimes(eventId)
    } yield maybeEvent.map { fe =>
      (eventId.id, fe.status, fe.maybeMessage, maybePayload.map(_.payload), processingTimes.map(_.processingTime))
    }
}
