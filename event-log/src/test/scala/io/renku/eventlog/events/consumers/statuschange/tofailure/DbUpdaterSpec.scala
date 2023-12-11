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

package io.renku.eventlog.events.consumers.statuschange.tofailure

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.ToFailure
import io.renku.eventlog.events.consumers.statuschange.SkunkExceptionsGenerators.postgresErrors
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DeliveryInfoRemover}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventContentGenerators.{eventDates, eventMessages}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventIds}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import skunk.SqlState

import java.time.{Duration, Instant}

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  "updateDB" should {

    "change status of the given event from ProcessingStatus to GenerationNonRecoverableFailure" in testDBResource.use {
      implicit cfg =>
        for {
          event <- createFailureEvent(GenerationNonRecoverableFailure)

          _ = givenDeliveryInfoRemoved(event.eventId)

          _ <- runDBUpdate(event).asserting {
                 _ shouldBe DBUpdateResults(event.project.slug, event.currentStatus -> -1, event.newStatus -> 1)
               }

          res <- findEvent(event.eventId).map(_.value).asserting {
                   _.unselect(Field.BatchDate) shouldBe FoundEvent(event.eventId,
                                                                   ExecutionDate(now),
                                                                   event.newStatus,
                                                                   event.message.some
                   )
                 }
        } yield res
    }

    "change status of the given event from ProcessingStatus to GenerationRecoverableFailure" in testDBResource.use {
      implicit cfg =>
        for {
          event <- createFailureEvent(GenerationRecoverableFailure, executionDelay = Duration.ofHours(100))

          _ = givenDeliveryInfoRemoved(event.eventId)

          _ <- runDBUpdate(event).asserting {
                 _ shouldBe DBUpdateResults(event.project.slug, event.currentStatus -> -1, event.newStatus -> 1)
               }

          res <- findEvent(event.eventId).map(_.value).asserting {
                   _.unselect(Field.BatchDate) shouldBe FoundEvent(event.eventId,
                                                                   ExecutionDate(now plus event.executionDelay.value),
                                                                   event.newStatus,
                                                                   event.message.some
                   )
                 }
        } yield res
    }

    "change status of the given event from TRANSFORMING_TRIPLES to TRANSFORMATION_RECOVERABLE_FAILURE " +
      "as well as all the older events which are TRIPLES_GENERATED status to TRANSFORMATION_RECOVERABLE_FAILURE" in testDBResource
        .use { implicit cfg =>
          val project         = consumerProjects.generateOne
          val latestEventDate = timestamps(max = now.minusSeconds(1)).generateAs(EventDate)

          for {
            event <- addEvent(project, TransformingTriples, latestEventDate).map { eId =>
                       ToFailure(eId.id, project, eventMessages.generateOne, TransformationRecoverableFailure, None)
                     }

            eventToUpdate <- addEvent(project, TriplesGenerated, timestamps(max = latestEventDate.value))
            eventsNotToUpdate <- {
                                   (EventStatus.all - TriplesGenerated).map { status =>
                                     addEvent(project, status, timestamps(max = latestEventDate.value))
                                       .tupleRight(status)
                                   } +
                                     addEvent(
                                       project,
                                       TriplesGenerated,
                                       timestamps(min = latestEventDate.value.plusSeconds(1), max = now)
                                     ).tupleRight(TriplesGenerated) +
                                     addEvent(project, TriplesGenerated, latestEventDate)
                                       .tupleRight(TriplesGenerated)
                                 }.toList.sequence

            _ = givenDeliveryInfoRemoved(event.eventId)

            _ <- runDBUpdate(event).asserting {
                   _ shouldBe DBUpdateResults(project.slug,
                                              TriplesGenerated    -> -1,
                                              event.currentStatus -> -1,
                                              event.newStatus     -> 2
                   )
                 }

            _ <- findEvent(event.eventId).asserting {
                   _.value.select(Field.Status, Field.Message) shouldBe FoundEvent(event.newStatus, event.message.some)
                 }

            _ <- findEvent(eventToUpdate).asserting(_.value.status shouldBe TransformationRecoverableFailure)

            _ <- eventsNotToUpdate.map { case (eventId, status) =>
                   findEvent(eventId).asserting(_.value.status shouldBe status)
                 }.sequence
          } yield Succeeded
        }

    "change status of the given event from TRANSFORMING_TRIPLES to TRANSFORMATION_NON_RECOVERABLE_FAILURE " +
      "as well as all the older events which are TRIPLES_GENERATED status to NEW" in testDBResource.use {
        implicit cfg =>
          val project         = consumerProjects.generateOne
          val latestEventDate = timestamps(max = now.minusSeconds(1)).generateAs(EventDate)

          for {
            event <-
              addEvent(project, TransformingTriples, latestEventDate).map { eId =>
                ToFailure(eId.id, project, eventMessages.generateOne, TransformationNonRecoverableFailure, None)
              }

            eventToUpdate <- addEvent(project, TriplesGenerated, timestamps(max = latestEventDate.value))

            eventsNotToUpdate <- {
                                   (EventStatus.all - TriplesGenerated).map { status =>
                                     addEvent(project, status, timestamps(max = latestEventDate.value))
                                       .tupleRight(status)
                                   } +
                                     addEvent(
                                       project,
                                       TriplesGenerated,
                                       timestamps(min = latestEventDate.value.plusSeconds(1), max = now)
                                     ).tupleRight(TriplesGenerated) +
                                     addEvent(project, TriplesGenerated, latestEventDate)
                                       .tupleRight(TriplesGenerated)
                                 }.toList.sequence

            _ = givenDeliveryInfoRemoved(event.eventId)

            _ <- runDBUpdate(event).asserting {
                   _ shouldBe DBUpdateResults(project.slug,
                                              event.currentStatus -> -1,
                                              event.newStatus     -> 1,
                                              New                 -> 1,
                                              TriplesGenerated    -> -1
                   )
                 }

            _ <- findEvent(event.eventId).asserting {
                   _.value.select(Field.Status, Field.Message) shouldBe FoundEvent(event.newStatus, event.message.some)
                 }

            _ <- findEvent(eventToUpdate).asserting(_.value.status shouldBe New)

            _ <- eventsNotToUpdate.map { case (eventId, status) =>
                   findEvent(eventId).asserting(_.value.status shouldBe status)
                 }.sequence
          } yield Succeeded
      }

    "do nothing if the given event is in an invalidStatus" in testDBResource.use { implicit cfg =>
      EventStatus.all
        .diff(Set(GeneratingTriples, TransformingTriples))
        .toList
        .map { invalidStatus =>
          val project         = consumerProjects.generateOne
          val latestEventDate = eventDates.generateOne
          val message         = eventMessages.generateOne

          for {
            eventId <- addEvent(project, invalidStatus, latestEventDate)

            res <- List(
                     ToFailure(eventId.id, project, message, GenerationNonRecoverableFailure, None),
                     ToFailure(eventId.id, project, message, GenerationRecoverableFailure, None),
                     ToFailure(eventId.id, project, message, TransformationNonRecoverableFailure, None),
                     ToFailure(eventId.id, project, message, TransformationRecoverableFailure, None)
                   ).map { event =>
                     givenDeliveryInfoRemoved(event.eventId)
                     for {
                       ancestorEventId <- addEvent(
                                            project,
                                            TriplesGenerated,
                                            timestamps(max = latestEventDate.value.minusSeconds(1))
                                          )

                       _ <- runDBUpdate(event).asserting(_ shouldBe DBUpdateResults.empty)

                       _ <- findEvent(eventId).asserting(_.value.status shouldBe invalidStatus)
                       _ <- findEvent(ancestorEventId).asserting(_.value.status shouldBe TriplesGenerated)
                     } yield Succeeded
                   }.sequence
          } yield res
        }
        .sequence
        .void
    }
  }

  "onRollback" should {

    "retry the updateDB procedure on DeadlockDetected" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        event <- createFailureEvent(project, GenerationNonRecoverableFailure, None)

        _ = givenDeliveryInfoRemoved(event.eventId)

        deadlockException = postgresErrors(SqlState.DeadlockDetected).generateOne
        _ <- (dbUpdater onRollback event)
               .apply(deadlockException)
               .asserting(_ shouldBe DBUpdateResults(project.slug, event.currentStatus -> -1, event.newStatus -> 1))

        res <- findEvent(event.eventId).asserting {
                 _.value.unselect(Field.BatchDate) shouldBe FoundEvent(event.eventId,
                                                                       ExecutionDate(now),
                                                                       event.newStatus,
                                                                       event.message.some
                 )
               }
      } yield res
    }

    "clean the delivery info for the event when Exception different than DeadlockDetected " +
      "and rethrow the exception" in testDBResource.use { implicit cfg =>
        val event = ToFailure(eventIds.generateOne,
                              consumerProjects.generateOne,
                              eventMessages.generateOne,
                              GenerationNonRecoverableFailure,
                              executionDelay = None
        )

        givenDeliveryInfoRemoved(event.eventId)

        val exception = exceptions.generateOne
        (dbUpdater onRollback event).apply(exception).assertThrowsError[Exception](_ shouldBe exception)
      }
  }

  private lazy val now            = Instant.now()
  private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
  private lazy val dbUpdater = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DbUpdater[IO](deliveryInfoRemover, () => now)
  }

  private def runDBUpdate(event: ToFailure)(implicit cfg: DBConfig[EventLogDB]) =
    moduleSessionResource
      .useK(dbUpdater updateDB event)

  private def givenDeliveryInfoRemoved(eventId: CompoundEventId) =
    (deliveryInfoRemover.deleteDelivery _).expects(eventId).returning(Kleisli.pure(()))

  private def createFailureEvent(newStatus: FailureStatus)(implicit cfg: DBConfig[EventLogDB]): IO[ToFailure] =
    createFailureEvent(consumerProjects.generateOne, newStatus, executionDelay = None)

  private def createFailureEvent(newStatus: FailureStatus, executionDelay: Duration)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[ToFailure] =
    createFailureEvent(consumerProjects.generateOne, newStatus, executionDelay.some)

  private def createFailureEvent(project: Project, newStatus: FailureStatus, executionDelay: Option[Duration] = None)(
      implicit cfg: DBConfig[EventLogDB]
  ): IO[ToFailure] = {
    val currentStatus = newStatus match {
      case GenerationNonRecoverableFailure | GenerationRecoverableFailure         => GeneratingTriples
      case TransformationNonRecoverableFailure | TransformationRecoverableFailure => TransformingTriples
    }
    addEvent(project, currentStatus).map(compoundId =>
      ToFailure(compoundId.id,
                project.copy(id = compoundId.projectId),
                eventMessages.generateOne,
                newStatus,
                executionDelay
      )
    )
  }

  private def addEvent(project: Project, status: ProcessingStatus)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[CompoundEventId] =
    addEvent(project, status, timestampsNotInTheFuture.generateAs(EventDate))

  private def addEvent(project: Project, status: EventStatus, eventDate: Gen[Instant])(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[CompoundEventId] =
    addEvent(project, status, eventDate.generateAs(EventDate))

  private def addEvent(project: Project, status: EventStatus, eventDate: EventDate)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[CompoundEventId] = {
    val ceId = compoundEventIds(project.id).generateOne
    storeEvent(
      ceId,
      status,
      timestampsNotInTheFuture.generateAs(ExecutionDate),
      eventDate,
      eventBodies.generateOne,
      projectSlug = project.slug
    ).as(ceId)
  }
}
