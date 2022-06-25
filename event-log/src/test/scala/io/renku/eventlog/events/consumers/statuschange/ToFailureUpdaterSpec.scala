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

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventIds}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import EventStatus._
import eu.timepit.refined.auto._
import io.renku.eventlog._
import EventContentGenerators.{eventDates, eventMessages}
import cats.data.Kleisli
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.{AllowedCombination, ToFailure}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class ToFailureUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change status of the given event from ProcessingStatus to FailureStatus" in new TestCase {
      Set(
        createFailureEvent(GeneratingTriples, GenerationNonRecoverableFailure, None),
        createFailureEvent(GeneratingTriples, GenerationRecoverableFailure, Duration.ofHours(100).some)
      ) foreach { statusChangeEvent =>
        (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          projectPath,
          Map(statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 1)
        )

        val Some((executionDate, executionStatus, Some(message))) = findEvent(statusChangeEvent.eventId)

        statusChangeEvent.newStatus match {
          case _: GenerationRecoverableFailure | _: TransformationRecoverableFailure =>
            executionDate.value shouldBe >(Instant.now())
          case _ => executionDate.value shouldBe <=(Instant.now())
        }

        executionStatus shouldBe statusChangeEvent.newStatus
        message         shouldBe statusChangeEvent.message
      }
    }

    "change status of the given event from TRANSFORMING_TRIPLES to TRANSFORMATION_RECOVERABLE_FAILURE " +
      "as well as all the older events which are TRIPLES_GENERATED status to TRANSFORMATION_RECOVERABLE_FAILURE" in new TestCase {
        val latestEventDate = eventDates.generateOne
        val statusChangeEvent = ToFailure(
          addEvent(compoundEventIds.generateOne.copy(projectId = projectId), TransformingTriples, latestEventDate),
          projectPath,
          eventMessages.generateOne,
          TransformingTriples,
          TransformationRecoverableFailure,
          None
        )

        val eventToUpdate = addEvent(
          compoundEventIds.generateOne.copy(projectId = projectId),
          TriplesGenerated,
          timestamps(max = latestEventDate.value).generateAs(EventDate)
        )

        val eventsNotToUpdate = (EventStatus.all - TriplesGenerated).map { status =>
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            status,
            timestamps(max = latestEventDate.value).generateAs(EventDate)
          ) -> status
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            TriplesGenerated,
            timestamps(min = latestEventDate.value.plusSeconds(2), max = Instant.now()).generateAs(EventDate)
          ) -> TriplesGenerated
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            TriplesGenerated,
            latestEventDate
          ) -> TriplesGenerated
        }

        (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          projectPath,
          Map(TriplesGenerated -> -1, statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 2)
        )

        val updatedEvent = findEvent(statusChangeEvent.eventId)
        updatedEvent.map(_._2)     shouldBe Some(statusChangeEvent.newStatus)
        updatedEvent.flatMap(_._3) shouldBe Some(statusChangeEvent.message)

        findEvent(eventToUpdate).map(_._2) shouldBe Some(TransformationRecoverableFailure)

        eventsNotToUpdate foreach { case (eventId, status) =>
          findEvent(eventId).map(_._2) shouldBe Some(status)
        }
      }

    "change status of the given event from TRANSFORMING_TRIPLES to TRANSFORMATION_NON_RECOVERABLE_FAILURE " +
      "as well as all the older events which are TRIPLES_GENERATED status to NEW" in new TestCase {
        val latestEventDate = eventDates.generateOne
        val statusChangeEvent = ToFailure(
          addEvent(compoundEventIds.generateOne.copy(projectId = projectId), TransformingTriples, latestEventDate),
          projectPath,
          eventMessages.generateOne,
          TransformingTriples,
          TransformationNonRecoverableFailure,
          None
        )

        val eventToUpdate = addEvent(
          compoundEventIds.generateOne.copy(projectId = projectId),
          TriplesGenerated,
          timestamps(max = latestEventDate.value).generateAs(EventDate)
        )

        val eventsNotToUpdate = (EventStatus.all - TriplesGenerated).map { status =>
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            status,
            timestamps(max = latestEventDate.value).generateAs(EventDate)
          ) -> status
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            TriplesGenerated,
            timestamps(min = latestEventDate.value.plusSeconds(2), max = Instant.now()).generateAs(EventDate)
          ) -> TriplesGenerated
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            TriplesGenerated,
            latestEventDate
          ) -> TriplesGenerated
        }

        (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          projectPath,
          Map(statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 1, New -> 1, TriplesGenerated -> -1)
        )

        val updatedEvent = findEvent(statusChangeEvent.eventId)
        updatedEvent.map(_._2)     shouldBe Some(statusChangeEvent.newStatus)
        updatedEvent.flatMap(_._3) shouldBe Some(statusChangeEvent.message)

        findEvent(eventToUpdate).map(_._2) shouldBe Some(New)

        eventsNotToUpdate foreach { case (eventId, status) =>
          findEvent(eventId).map(_._2) shouldBe Some(status)
        }
      }

    EventStatus.all.diff(Set(GeneratingTriples, TransformingTriples)) foreach { invalidStatus =>
      s"do nothing if the given event is in $invalidStatus" in new TestCase {

        val latestEventDate = eventDates.generateOne
        val eventId = addEvent(compoundEventIds.generateOne.copy(projectId = projectId), invalidStatus, latestEventDate)
        val message = eventMessages.generateOne

        Set(
          ToFailure(eventId, projectPath, message, GeneratingTriples, GenerationNonRecoverableFailure, None),
          ToFailure(eventId, projectPath, message, GeneratingTriples, GenerationRecoverableFailure, None),
          ToFailure(eventId, projectPath, message, TransformingTriples, TransformationNonRecoverableFailure, None),
          ToFailure(eventId, projectPath, message, TransformingTriples, TransformationRecoverableFailure, None)
        ) foreach { statusChangeEvent =>
          (deliveryInfoRemover.deleteDelivery _).expects(statusChangeEvent.eventId).returning(Kleisli.pure(()))

          val ancestorEventId = addEvent(
            compoundEventIds.generateOne.copy(projectId = projectId),
            TriplesGenerated,
            timestamps(max = latestEventDate.value.minusSeconds(1)).generateAs(EventDate)
          )

          sessionResource
            .useK(dbUpdater.updateDB(statusChangeEvent))
            .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty

          findEvent(eventId).map(_._2)         shouldBe Some(invalidStatus)
          findEvent(ancestorEventId).map(_._2) shouldBe Some(TriplesGenerated)
        }
      }
    }
  }

  "onRollback" should {
    "clean the delivery info for the event" in new TestCase {
      val event = ToFailure(compoundEventIds.generateOne,
                            projectPath,
                            eventMessages.generateOne,
                            GeneratingTriples,
                            GenerationNonRecoverableFailure,
                            None
      )

      (deliveryInfoRemover.deleteDelivery _).expects(event.eventId).returning(Kleisli.pure(()))

      sessionResource
        .useK(dbUpdater onRollback event)
        .unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime         = mockFunction[Instant]
    val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
    val queriesExecTimes    = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater           = new ToFailureUpdater[IO](deliveryInfoRemover, queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def createFailureEvent[C <: ProcessingStatus, N <: FailureStatus](currentStatus:  C,
                                                                      newStatus:      N,
                                                                      executionDelay: Option[Duration]
    )(implicit
        evidence: AllowedCombination[C, N]
    ): ToFailure[C, N] =
      ToFailure(addEvent(currentStatus),
                projectPath,
                eventMessages.generateOne,
                currentStatus,
                newStatus,
                executionDelay
      )

    def addEvent[S <: ProcessingStatus](status: S): CompoundEventId = {
      val eventId = CompoundEventId(eventIds.generateOne, projectId)
      addEvent(eventId, status)
      eventId
    }

    def addEvent(eventId:   CompoundEventId,
                 status:    EventStatus,
                 eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
    ): CompoundEventId = {
      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        eventDate,
        eventBodies.generateOne,
        projectPath = projectPath
      )
      eventId
    }
  }
}
