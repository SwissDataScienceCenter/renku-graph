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

package io.renku.eventlog.statuschange.commands
import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, schemaVersions}
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.statuschange.ChangeStatusRequest.{EventAndPayloadRequest, EventOnlyRequest}
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import io.renku.eventlog.statuschange.commands.Generators.changeStatusRequestsWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToTriplesGeneratedSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "command" should {

    s"set status $TriplesGenerated on the event with the given id and $GeneratingTriples status, " +
      "insert the payload in the event_payload table " +
      "increment awaiting transformation gauge and decrement under triples generation gauge for the project, " +
      "insert the processingTime " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

        val projectPath = projectPaths.generateOne
        storeEvent(
          eventId,
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate,
          projectPath = projectPath
        )
        storeEvent(
          compoundEventIds.generateOne.copy(id = eventId.id),
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )
        storeEvent(
          compoundEventIds.generateOne,
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )

        findEvents(status = TriplesGenerated) shouldBe List.empty
        findPayload(eventId)                  shouldBe None

        (underTriplesGenerationGauge.decrement _).expects(projectPath).returning(IO.unit)
        (awaitingTransformationGauge.increment _).expects(projectPath).returning(IO.unit)

        val command = GeneratingToTriplesGenerated[IO](eventId,
                                                       payload,
                                                       schemaVersion,
                                                       underTriplesGenerationGauge,
                                                       awaitingTransformationGauge,
                                                       processingTime,
                                                       currentTime
        )

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

        findEvents(status = TriplesGenerated)    shouldBe List((eventId, ExecutionDate(now), eventBatchDate))
        findPayload(eventId)                     shouldBe Some((eventId, payload))
        findProcessingTime(eventId).eventIdsOnly shouldBe List(eventId)

        histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
      }

    s"set status $TriplesGenerated on the event with the given id and $TransformingTriples status, " +
      "do not insert the payload " +
      "increment awaiting transformation gauge and decrement under triples transformation gauge for the project " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

        val projectPath = projectPaths.generateOne
        storeEvent(
          eventId,
          EventStatus.TransformingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate,
          projectPath = projectPath,
          maybeEventPayload = Some(payload)
        )
        storeEvent(
          compoundEventIds.generateOne.copy(id = eventId.id),
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )
        storeEvent(
          compoundEventIds.generateOne,
          EventStatus.GeneratingTriples,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )

        findEvents(status = TriplesGenerated) shouldBe List.empty
        findPayload(eventId).map(_._2)        shouldBe Some(payload)

        (underTriplesTransformationGauge.decrement _).expects(projectPath).returning(IO.unit)
        (awaitingTransformationGauge.increment _).expects(projectPath).returning(IO.unit)

        val command = TransformingToTriplesGenerated[IO](eventId,
                                                         awaitingTransformationGauge,
                                                         underTriplesTransformationGauge,
                                                         now = currentTime
        )

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

        findEvents(status = TriplesGenerated) shouldBe List((eventId, ExecutionDate(now), eventBatchDate))
        findPayload(eventId)                  shouldBe Some((eventId, payload))

        histogram.verifyExecutionTimeMeasured("transforming_triples->triples_generated")
      }

    EventStatus.all.filterNot(_ == GeneratingTriples) foreach { eventStatus =>
      s"return Failure when updating event from $GeneratingTriples to $eventStatus status " +
        s"and return ${UpdateResult.Failure}" in new TestCase {
          val executionDate = executionDates.generateOne
          storeEvent(eventId,
                     eventStatus,
                     executionDate,
                     eventDates.generateOne,
                     eventBodies.generateOne,
                     batchDate = eventBatchDate
          )

          findEvents(status = eventStatus) shouldBe List((eventId, executionDate, eventBatchDate))
          findPayload(eventId)             shouldBe None
          val command = GeneratingToTriplesGenerated[IO](eventId,
                                                         payload,
                                                         schemaVersion,
                                                         awaitingTransformationGauge,
                                                         underTriplesGenerationGauge,
                                                         processingTime,
                                                         currentTime
          )

          (commandRunner run command).unsafeRunSync() shouldBe a[UpdateResult.Failure]

          val expectedEvents =
            if (eventStatus != TriplesGenerated) List.empty
            else List((eventId, executionDate, eventBatchDate))
          findEvents(status = TriplesGenerated)    shouldBe expectedEvents
          findPayload(eventId)                     shouldBe None
          findProcessingTime(eventId).eventIdsOnly shouldBe List()

          histogram.verifyExecutionTimeMeasured(command.queries.head.name)
        }

      s"do nothing when updating event from $GeneratingTriples to $eventStatus status " +
        s"and return ${UpdateResult.NotFound}" in new TestCase {

          findEvents(status = eventStatus) shouldBe List()
          findPayload(eventId)             shouldBe None
          val command = GeneratingToTriplesGenerated[IO](eventId,
                                                         payload,
                                                         schemaVersion,
                                                         awaitingTransformationGauge,
                                                         underTriplesGenerationGauge,
                                                         processingTime,
                                                         currentTime
          )

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.NotFound

          findEvents(status = TriplesGenerated)    shouldBe List()
          findPayload(eventId)                     shouldBe None
          findProcessingTime(eventId).eventIdsOnly shouldBe List()
        }
    }

    EventStatus.all.filterNot(_ == TransformingTriples) foreach { eventStatus =>
      s"return Failure when updating event from $TransformingTriples to $eventStatus status " +
        s"and return ${UpdateResult.Failure}" in new TestCase {
          val executionDate = executionDates.generateOne
          storeEvent(eventId,
                     eventStatus,
                     executionDate,
                     eventDates.generateOne,
                     eventBodies.generateOne,
                     batchDate = eventBatchDate
          )

          findEvents(status = eventStatus) shouldBe List((eventId, executionDate, eventBatchDate))

          val command = TransformingToTriplesGenerated[IO](eventId,
                                                           awaitingTransformationGauge,
                                                           underTriplesTransformationGauge,
                                                           currentTime
          )

          (commandRunner run command).unsafeRunSync() shouldBe a[UpdateResult.Failure]

          val expectedEvents =
            if (eventStatus != TriplesGenerated) List.empty
            else List((eventId, executionDate, eventBatchDate))
          findEvents(status = TriplesGenerated)    shouldBe expectedEvents
          findProcessingTime(eventId).eventIdsOnly shouldBe List()

          histogram.verifyExecutionTimeMeasured(command.queries.head.name)
        }

      s"do nothing when updating event from $TransformingTriples to $eventStatus status " +
        s"and return ${UpdateResult.NotFound}" in new TestCase {

          findEvents(status = eventStatus) shouldBe List()
          findPayload(eventId)             shouldBe None
          val command = TransformingToTriplesGenerated[IO](eventId,
                                                           awaitingTransformationGauge,
                                                           underTriplesTransformationGauge,
                                                           currentTime
          )

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.NotFound

          findEvents(status = TriplesGenerated)    shouldBe List()
          findPayload(eventId)                     shouldBe None
          findProcessingTime(eventId).eventIdsOnly shouldBe List()
        }
    }
  }

  "factory" should {

    s"return a CommandFound for the $TriplesGenerated status " +
      s"when event is currently in the $GeneratingTriples status" in new TestCase {

        createEvent(GeneratingTriples)

        val maybeProcessingTime = eventProcessingTimes.generateSome
        val eventPayload        = eventPayloads.generateOne
        val payloadPartBody     = json"""{
            "schemaVersion": ${schemaVersion.value},
            "payload":       ${eventPayload.value}
          }""".spaces2

        val actual = ToTriplesGenerated
          .factory(transactor,
                   underTriplesTransformationGauge,
                   underTriplesGenerationGauge,
                   awaitingTransformationGauge
          )
          .run(EventAndPayloadRequest(eventId, TriplesGenerated, maybeProcessingTime, payloadPartBody))
          .unsafeRunSync()

        actual shouldBe CommandFound(
          GeneratingToTriplesGenerated(eventId,
                                       eventPayload,
                                       schemaVersion,
                                       underTriplesGenerationGauge,
                                       awaitingTransformationGauge,
                                       maybeProcessingTime
          )
        )
      }

    s"return a CommandFound for the $TriplesGenerated status " +
      s"when event is currently in the $TransformingTriples status" in new TestCase {

        createEvent(TransformingTriples)

        val actual = ToTriplesGenerated
          .factory(transactor,
                   underTriplesTransformationGauge,
                   underTriplesGenerationGauge,
                   awaitingTransformationGauge
          )
          .run(
            EventOnlyRequest(eventId,
                             TriplesGenerated,
                             eventProcessingTimes.generateOption,
                             eventMessages.generateOption
            )
          )
          .unsafeRunSync()

        actual shouldBe CommandFound(
          TransformingToTriplesGenerated(eventId, awaitingTransformationGauge, underTriplesTransformationGauge)
        )
      }

    EventStatus.all.filterNot(status => status == TriplesGenerated) foreach { eventStatus =>
      s"return NotSupported if status is $eventStatus" in new TestCase {
        ToTriplesGenerated
          .factory(transactor,
                   underTriplesTransformationGauge,
                   underTriplesGenerationGauge,
                   awaitingTransformationGauge
          )
          .run(changeStatusRequestsWith(eventStatus).generateOne)
          .unsafeRunSync() shouldBe NotSupported
      }
    }

    "return PayloadMalformed if there's payload but no processing time in the request" in new TestCase {
      val payloadPartBody = json"""{
            "schemaVersion": ${schemaVersion.value},
            "payload":       ${eventPayloads.generateOne.value}
          }""".spaces2

      ToTriplesGenerated
        .factory(transactor, underTriplesTransformationGauge, underTriplesGenerationGauge, awaitingTransformationGauge)
        .run(EventAndPayloadRequest(eventId, TriplesGenerated, None, payloadPartBody))
        .unsafeRunSync() shouldBe PayloadMalformed("No processing time provided")
    }

    s"return PayloadMalformed if the decoding failed because of missing schemaVersion property" in new TestCase {

      createEvent(GeneratingTriples)

      val eventPayload    = eventPayloads.generateOne
      val payloadPartBody = json"""{"payload": ${eventPayload.value}}""".spaces2

      ToTriplesGenerated
        .factory(transactor, underTriplesTransformationGauge, underTriplesGenerationGauge, awaitingTransformationGauge)
        .run(EventAndPayloadRequest(eventId, TriplesGenerated, processingTime, payloadPartBody))
        .unsafeRunSync() shouldBe PayloadMalformed(
        "Attempt to decode value on failed cursor: DownField(schemaVersion)"
      )
    }

    s"return PayloadMalformed if the decoding failed because of missing payload property" in new TestCase {

      createEvent(GeneratingTriples)

      val payloadPartBody = json"""{"schemaVersion": ${schemaVersion.value}}""".spaces2

      ToTriplesGenerated
        .factory(transactor, underTriplesTransformationGauge, underTriplesGenerationGauge, awaitingTransformationGauge)
        .run(EventAndPayloadRequest(eventId, TriplesGenerated, processingTime, payloadPartBody))
        .unsafeRunSync() shouldBe PayloadMalformed(
        "Attempt to decode value on failed cursor: DownField(payload)"
      )
    }
  }

  private trait TestCase {
    val awaitingTransformationGauge     = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesTransformationGauge = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesGenerationGauge     = mock[LabeledGauge[IO, projects.Path]]
    val histogram                       = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val currentTime    = mockFunction[Instant]
    val eventId        = compoundEventIds.generateOne
    val eventBatchDate = batchDates.generateOne
    val processingTime = eventProcessingTimes.generateSome

    val payload       = eventPayloads.generateOne
    val schemaVersion = schemaVersions.generateOne
    val commandRunner = new StatusUpdatesRunnerImpl(transactor, histogram, TestLogger[IO]())
    val now           = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def createEvent(status: EventStatus, maybePayload: Option[EventPayload] = None): Unit = storeEvent(
      eventId,
      status,
      executionDates.generateOne,
      eventDates.generateOne,
      eventBodies.generateOne,
      batchDate = eventBatchDate,
      maybeEventPayload = maybePayload
    )
  }
}
