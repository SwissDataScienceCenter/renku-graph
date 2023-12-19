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
package totriplesgenerated

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.ToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.SkunkExceptionsGenerators.postgresErrors
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DeliveryInfoRemover}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps}
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators.{eventIds, eventProcessingTimes, zippedEventPayloads}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import skunk.Session
import skunk.SqlState.DeadlockDetected

import java.time.Instant

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  private val project = consumerProjects.generateOne

  "updateDB" should {

    "change statuses to TRIPLES_GENERATED of all events older than the current event" in testDBResource.use {
      implicit cfg =>
        val eventDate        = eventDates.generateOne
        val statusesToUpdate = List(New, GeneratingTriples, GenerationRecoverableFailure, AwaitingDeletion)
        for {
          eventsToUpdate <-
            statusesToUpdate
              .map(storeGeneratedEvent(_, timestamps(max = eventDate.value).generateAs(EventDate), project))
              .sequence

          eventsToSkip <-
            (storeGeneratedEvent(New, timestamps(min = eventDate.value, max = now).generateAs(EventDate), project) ::
              (EventStatus.all.toList diff statusesToUpdate)
                .map(storeGeneratedEvent(_, timestamps(max = eventDate.value).generateAs(EventDate), project))).sequence

          event <- storeGeneratedEvent(GeneratingTriples, eventDate, project)

          _ = givenDeliveryInfoRemoving(event.eventId, returning = Kleisli.pure(()))

          statusChangeEvent = ToTriplesGenerated(event.eventId.id,
                                                 project,
                                                 eventProcessingTimes.generateOne,
                                                 zippedEventPayloads.generateOne
                              )

          _ <- moduleSessionResource.session
                 .useKleisli(dbUpdater updateDB statusChangeEvent)
                 .asserting {
                   _ shouldBe DBUpdateResults.ForProjects(
                     project.slug,
                     statusesToUpdate
                       .map(_ -> -1)
                       .toMap +
                       (GeneratingTriples -> (-1 /* for the event */ - 1 /* for the old GeneratingTriples */ )) +
                       (TriplesGenerated -> (eventsToUpdate.size + 1 /* for the event */ - 1 /* for the AwaitingDeletion */ ))
                   )
                 }

          _ <- findFullEvent(event.eventId)
                 .map(_.value)
                 .asserting { fe =>
                   fe.status                 shouldBe TriplesGenerated
                   fe.maybePayload.value.value should contain theSameElementsAs statusChangeEvent.payload.value
                   fe.processingTimes          should contain(statusChangeEvent.processingTime)
                 }

          _ <- eventsToUpdate.map {
                 case ev if ev.status == AwaitingDeletion =>
                   findFullEvent(ev.eventId).asserting(_ shouldBe None)
                 case ev =>
                   findFullEvent(ev.eventId)
                     .map(_.value)
                     .asserting { fe =>
                       fe.status          shouldBe TriplesGenerated
                       fe.processingTimes shouldBe Nil
                       fe.maybePayload    shouldBe None
                     }
               }.sequence

          _ <- eventsToSkip.map { ev =>
                 findFullEvent(ev.eventId)
                   .map(_.value)
                   .map { fe =>
                     fe.status       shouldBe ev.status
                     fe.maybeMessage shouldBe ev.maybeMessage
                     (fe.maybePayload, ev.maybePayload).mapN { (actualPayload, toSkipPayload) =>
                       actualPayload.value should contain theSameElementsAs toSkipPayload.value
                     }
                     fe.processingTimes shouldBe ev.processingTimes
                   }
               }.sequence
        } yield Succeeded
    }

    "change statuses to TRIPLES_GENERATED of all events older than the current event but not the events with the same date" in testDBResource
      .use { implicit cfg =>
        val eventDate = eventDates.generateOne
        for {
          event1 <- storeGeneratedEvent(GeneratingTriples, eventDate, project)
          event2 <- storeGeneratedEvent(TriplesGenerated, eventDate, project)

          statusChangeEvent = ToTriplesGenerated(event1.eventId.id,
                                                 project,
                                                 eventProcessingTimes.generateOne,
                                                 zippedEventPayloads.generateOne
                              )

          _ = givenDeliveryInfoRemoving(event1.eventId, returning = Kleisli.pure(()))

          _ <- moduleSessionResource(cfg).session
                 .useKleisli(dbUpdater updateDB statusChangeEvent)
                 .asserting(_ shouldBe DBUpdateResults(project.slug, GeneratingTriples -> -1, TriplesGenerated -> 1))

          _ <- findFullEvent(event1.eventId).map(_.value).map { fe =>
                 fe.status                 shouldBe TriplesGenerated
                 fe.maybePayload.value.value should contain theSameElementsAs statusChangeEvent.payload.value
                 fe.processingTimes          should contain(statusChangeEvent.processingTime)
               }

          _ <- findFullEvent(event2.eventId).asserting(_.map(_.status) shouldBe TriplesGenerated.some)
        } yield Succeeded
      }

    "just clean the delivery info if the event is already in the TRIPLES_GENERATED" in testDBResource.use {
      implicit cfg =>
        val eventDate = eventDates.generateOne
        for {
          olderEvent <- storeGeneratedEvent(New, timestamps(max = eventDate.value).generateAs(EventDate), project)
          event      <- storeGeneratedEvent(TriplesGenerated, eventDate, project)

          statusChangeEvent =
            ToTriplesGenerated(event.eventId.id,
                               project,
                               eventProcessingTimes.generateOne,
                               zippedEventPayloads.generateOne
            )

          _ = givenDeliveryInfoRemoving(statusChangeEvent.eventId, returning = Kleisli.pure(()))

          _ <- moduleSessionResource.session
                 .useKleisli(dbUpdater updateDB statusChangeEvent)
                 .asserting(_ shouldBe DBUpdateResults.empty)

          _ <- findFullEvent(statusChangeEvent.eventId).map(_.value).asserting { fe =>
                 fe.status shouldBe TriplesGenerated
                 (fe.maybePayload, event.maybePayload).mapN { (actualPayload, payload) =>
                   actualPayload.value should contain theSameElementsAs payload.value
                 }
                 fe.processingTimes should not contain statusChangeEvent.processingTime
               }

          _ <- findFullEvent(olderEvent.eventId).map(_.value).asserting { fe =>
                 fe.status          shouldBe olderEvent.status
                 fe.processingTimes shouldBe Nil
                 fe.maybePayload    shouldBe None
               }
        } yield Succeeded
    }
  }

  "onRollback" should {

    "retry updating DB on a DeadlockDetected" in testDBResource.use { implicit cfg =>
      for {
        event <- storeGeneratedEvent(GeneratingTriples, eventDates.generateOne, project)

        statusChangeEvent = ToTriplesGenerated(event.eventId.id,
                                               project,
                                               eventProcessingTimes.generateOne,
                                               zippedEventPayloads.generateOne
                            )

        _ = givenDeliveryInfoRemoving(statusChangeEvent.eventId, returning = Kleisli.pure(()))

        _ <- (dbUpdater onRollback statusChangeEvent)
               .apply(postgresErrors(DeadlockDetected).generateOne)
               .asserting(_ shouldBe DBUpdateResults(project.slug, GeneratingTriples -> -1, TriplesGenerated -> 1))
      } yield Succeeded
    }

    "clean the delivery info for the event if a random exception thrown" in testDBResource.use { implicit cfg =>
      val event = ToTriplesGenerated(eventIds.generateOne,
                                     project,
                                     eventProcessingTimes.generateOne,
                                     zippedEventPayloads.generateOne
      )

      givenDeliveryInfoRemoving(event.eventId, returning = Kleisli.pure(()))

      val exception = exceptions.generateOne

      (dbUpdater onRollback event).apply(exception).assertThrowsError[Exception](_ shouldBe exception)
    }
  }

  private lazy val now            = Instant.now()
  private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
  private implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private lazy val dbUpdater = new DbUpdater[IO](deliveryInfoRemover, () => now)

  private def givenDeliveryInfoRemoving(eventId: CompoundEventId, returning: Kleisli[IO, Session[IO], Unit]) =
    (deliveryInfoRemover.deleteDelivery _).expects(eventId).returning(returning)

  private case class FullEvent(eventId:         CompoundEventId,
                               status:          EventStatus,
                               maybeMessage:    Option[EventMessage],
                               maybePayload:    Option[ZippedEventPayload],
                               processingTimes: List[EventProcessingTime]
  )
  private def findFullEvent(eventId: CompoundEventId)(implicit cfg: DBConfig[EventLogDB]) =
    for {
      maybeEvent     <- findEvent(eventId)
      maybePayload   <- findPayload(eventId)
      processingTime <- findProcessingTimes(eventId)
    } yield maybeEvent.map { event =>
      FullEvent(event.id,
                event.status,
                event.maybeMessage,
                maybePayload.map(_.payload),
                processingTime.map(_.processingTime)
      )
    }
}
