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
import cats.syntax.all._
import io.renku.eventlog.api.events.StatusChangeEvent.ToFailure
import io.renku.eventlog.events.consumers.statuschange.SkunkExceptionsGenerators.postgresErrors
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DeliveryInfoRemover}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.events.consumers.ConsumersModelGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventContentGenerators.{eventDates, eventMessages}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventIds}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.SqlState

import java.time.{Duration, Instant}

class DbUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with OptionValues
    with MockFactory {

  "updateDB" should {

    "change status of the given event from ProcessingStatus to FailureStatus" in new TestCase {
      Set(
        createFailureEvent(GenerationNonRecoverableFailure, None),
        createFailureEvent(GenerationRecoverableFailure, Duration.ofHours(100).some)
      ) foreach { statusChangeEvent =>
        givenDeliveryInfoRemoved(statusChangeEvent.eventId)

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          project.slug,
          Map(statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 1)
        )

        val (executionDate, executionStatus, Some(message)) = findEvent(statusChangeEvent.eventId).value

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
          addEvent(compoundEventIds.generateOne.copy(projectId = project.id), TransformingTriples, latestEventDate).id,
          project,
          eventMessages.generateOne,
          TransformationRecoverableFailure,
          None
        )

        val eventToUpdate = addEvent(
          compoundEventIds.generateOne.copy(projectId = project.id),
          TriplesGenerated,
          timestamps(max = latestEventDate.value).generateAs(EventDate)
        )

        val eventsNotToUpdate = (EventStatus.all - TriplesGenerated).map { status =>
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            status,
            timestamps(max = latestEventDate.value).generateAs(EventDate)
          ) -> status
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            TriplesGenerated,
            timestamps(min = latestEventDate.value.plusSeconds(2), max = Instant.now()).generateAs(EventDate)
          ) -> TriplesGenerated
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            TriplesGenerated,
            latestEventDate
          ) -> TriplesGenerated
        }

        givenDeliveryInfoRemoved(statusChangeEvent.eventId)

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          project.slug,
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
          addEvent(compoundEventIds.generateOne.copy(projectId = project.id), TransformingTriples, latestEventDate).id,
          project,
          eventMessages.generateOne,
          TransformationNonRecoverableFailure,
          None
        )

        val eventToUpdate = addEvent(
          compoundEventIds.generateOne.copy(projectId = project.id),
          TriplesGenerated,
          timestamps(max = latestEventDate.value).generateAs(EventDate)
        )

        val eventsNotToUpdate = (EventStatus.all - TriplesGenerated).map { status =>
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            status,
            timestamps(max = latestEventDate.value).generateAs(EventDate)
          ) -> status
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            TriplesGenerated,
            timestamps(min = latestEventDate.value.plusSeconds(2), max = Instant.now()).generateAs(EventDate)
          ) -> TriplesGenerated
        } + {
          addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
            TriplesGenerated,
            latestEventDate
          ) -> TriplesGenerated
        }

        givenDeliveryInfoRemoved(statusChangeEvent.eventId)

        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          project.slug,
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
        val eventId =
          addEvent(compoundEventIds.generateOne.copy(projectId = project.id), invalidStatus, latestEventDate)
        val message = eventMessages.generateOne

        Set(
          ToFailure(eventId.id, project, message, GenerationNonRecoverableFailure, None),
          ToFailure(eventId.id, project, message, GenerationRecoverableFailure, None),
          ToFailure(eventId.id, project, message, TransformationNonRecoverableFailure, None),
          ToFailure(eventId.id, project, message, TransformationRecoverableFailure, None)
        ) foreach { statusChangeEvent =>
          givenDeliveryInfoRemoved(statusChangeEvent.eventId)

          val ancestorEventId = addEvent(
            compoundEventIds.generateOne.copy(projectId = project.id),
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

    "retry the updateDB procedure on DeadlockDetected" in new TestCase {

      val statusChangeEvent = createFailureEvent(GenerationNonRecoverableFailure, None)

      givenDeliveryInfoRemoved(statusChangeEvent.eventId)

      val deadlockException = postgresErrors(SqlState.DeadlockDetected).generateOne
      (dbUpdater onRollback statusChangeEvent)
        .apply(deadlockException)
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        project.slug,
        Map(statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 1)
      )

      val (executionDate, executionStatus, Some(message)) = findEvent(statusChangeEvent.eventId).value

      executionDate.value shouldBe <=(Instant.now())
      executionStatus     shouldBe statusChangeEvent.newStatus
      message             shouldBe statusChangeEvent.message
    }

    "clean the delivery info for the event when Exception different than DeadlockDetected " +
      "and rethrow the exception" in new TestCase {

        val event = ToFailure(eventIds.generateOne,
                              ConsumersModelGenerators.consumerProjects.generateOne,
                              eventMessages.generateOne,
                              GenerationNonRecoverableFailure,
                              None
        )

        givenDeliveryInfoRemoved(event.eventId)

        val exception = exceptions.generateOne
        intercept[Exception] {
          (dbUpdater onRollback event)
            .apply(exception)
            .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty
        } shouldBe exception
      }
  }

  private trait TestCase {

    val project = ConsumersModelGenerators.consumerProjects.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val currentTime         = mockFunction[Instant]
    private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val dbUpdater = new DbUpdater[IO](deliveryInfoRemover, currentTime)

    private val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def givenDeliveryInfoRemoved(eventId: CompoundEventId) =
      (deliveryInfoRemover.deleteDelivery _).expects(eventId).returning(Kleisli.pure(()))

    def createFailureEvent(newStatus: FailureStatus, executionDelay: Option[Duration]): ToFailure = {
      val currentStatus = newStatus match {
        case GenerationNonRecoverableFailure | GenerationRecoverableFailure         => GeneratingTriples
        case TransformationNonRecoverableFailure | TransformationRecoverableFailure => TransformingTriples
      }
      val compoundId = addEvent(currentStatus)
      ToFailure(compoundId.id,
                project.copy(id = compoundId.projectId),
                eventMessages.generateOne,
                newStatus,
                executionDelay
      )
    }

    private def addEvent(status: ProcessingStatus): CompoundEventId = {
      val eventId = CompoundEventId(eventIds.generateOne, project.id)
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
        projectSlug = project.slug
      )
      eventId
    }
  }
}
