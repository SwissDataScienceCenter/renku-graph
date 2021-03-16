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
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, projectSchemaVersions}
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import io.renku.eventlog.statuschange.commands.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import io.renku.eventlog.{ExecutionDate, InMemoryEventLogDbSpec}
import org.http4s.circe.jsonEncoder
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{MediaType, Request}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToTriplesGeneratedSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "command" should {

    s"set status $TriplesGenerated on the event with the given id and $GeneratingTriples status, " +
      "insert the payload in the event_payload table" +
      "increment awaiting transformation gauge and decrement under triples generation gauge for the project, insert the processingTime " +
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

        val command = ToTriplesGenerated[IO](eventId,
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

    EventStatus.all.filterNot(_ == GeneratingTriples) foreach { eventStatus =>
      s"do nothing when updating event with $eventStatus status " +
        s"and return ${UpdateResult.NotFound}" in new TestCase {
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
          val command =
            ToTriplesGenerated[IO](eventId,
                                   payload,
                                   schemaVersion,
                                   awaitingTransformationGauge,
                                   underTriplesGenerationGauge,
                                   processingTime,
                                   currentTime
            )

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.NotFound

          val expectedEvents =
            if (eventStatus != TriplesGenerated) List.empty
            else List((eventId, executionDate, eventBatchDate))
          findEvents(status = TriplesGenerated)    shouldBe expectedEvents
          findPayload(eventId)                     shouldBe None
          findProcessingTime(eventId).eventIdsOnly shouldBe List()

          histogram.verifyExecutionTimeMeasured(command.queries.head.name)
        }
    }

    "factory" should {
      "return a CommandFound when properly decoding a request" in new TestCase {
        val triples             = eventPayloads.generateOne
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val expected =
          ToTriplesGenerated(eventId,
                             triples,
                             schemaVersion,
                             underTriplesGenerationGauge,
                             awaitingTransformationGauge,
                             maybeProcessingTime
          )

        val payloadStr = json"""{ "schemaVersion": ${schemaVersion.value}, "payload": ${triples.value}  }"""

        val body = Multipart[IO](
          Vector(
            Part.formData[IO](
              "event",
              (json"""{
            "status": ${EventStatus.TriplesGenerated.value}
          }""" deepMerge maybeProcessingTime
                .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
                .getOrElse(Json.obj())).noSpaces,
              `Content-Type`(MediaType.application.json)
            ),
            Part.formData[IO]("payload", payloadStr.noSpaces)
          )
        )

        val request = Request[IO]().withEntity(body).withHeaders(body.headers)

        val actual = ToTriplesGenerated
          .factory(underTriplesGenerationGauge, awaitingTransformationGauge)
          .run((eventId, request))
        actual.unsafeRunSync() shouldBe CommandFound(expected)
      }

      EventStatus.all.filterNot(status => status == TriplesGenerated) foreach { eventStatus =>
        s"return Not supported if the decoding failed because of wrong status: $eventStatus " in new TestCase {
          val triples             = eventPayloads.generateOne
          val maybeProcessingTime = eventProcessingTimes.generateOption
          val payloadStr          = json"""{ "schemaVersion": ${schemaVersion.value}, "payload": ${triples.value}  }"""
          val body = Multipart[IO](
            Vector(
              Part.formData[IO](
                "event",
                (json"""{ "status": ${eventStatus.value} }""" deepMerge maybeProcessingTime
                  .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
                  .getOrElse(Json.obj())).noSpaces,
                `Content-Type`(MediaType.application.json)
              ),
              Part.formData[IO]("payload", payloadStr.noSpaces)
            )
          )
          val request = Request[IO]().withEntity(body).withHeaders(body.headers)

          val actual =
            ToTriplesGenerated
              .factory(underTriplesGenerationGauge, awaitingTransformationGauge)
              .run((eventId, request))
          actual.unsafeRunSync() shouldBe NotSupported
        }
      }

      s"return Not supported if the decoding failed because of wrong request type" in new TestCase {
        val body    = jsons.generateOne
        val request = Request[IO]().withEntity(body)

        val actual =
          ToTriplesGenerated
            .factory(underTriplesGenerationGauge, awaitingTransformationGauge)
            .run((eventId, request))
        actual.unsafeRunSync() shouldBe NotSupported
      }

      s"return Payload Malformed if the decoding failed because of schema version is missing " in new TestCase {
        val triples             = eventPayloads.generateOne
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val payloadStr          = json"""{ "payload": ${triples.value}  }"""

        val body = Multipart[IO](
          Vector(
            Part.formData[IO](
              "event",
              (json"""{ "status": ${EventStatus.TriplesGenerated.value} }""" deepMerge maybeProcessingTime
                .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
                .getOrElse(Json.obj())).noSpaces,
              `Content-Type`(MediaType.application.json)
            ),
            Part.formData[IO]("payload", payloadStr.noSpaces)
          )
        )

        val request = Request[IO]().withEntity(body).withHeaders(body.headers)

        val actual =
          ToTriplesGenerated
            .factory(underTriplesGenerationGauge, awaitingTransformationGauge)
            .run((eventId, request))
        actual.unsafeRunSync() shouldBe PayloadMalformed(
          "Attempt to decode value on failed cursor: DownField(schemaVersion)"
        )
      }

      s"return Payload Malformed if the decoding failed because of missing payload " in new TestCase {
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val payloadStr          = json"""{ "schemaVersion": ${schemaVersion.value}}"""

        val body = Multipart[IO](
          Vector(
            Part.formData[IO](
              "event",
              (json"""{ "status": ${EventStatus.TriplesGenerated.value} }""" deepMerge maybeProcessingTime
                .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}  }""")
                .getOrElse(Json.obj())).noSpaces,
              `Content-Type`(MediaType.application.json)
            ),
            Part.formData[IO]("payload", payloadStr.noSpaces)
          )
        )

        val request = Request[IO]().withEntity(body).withHeaders(body.headers)

        val actual =
          ToTriplesGenerated
            .factory(underTriplesGenerationGauge, awaitingTransformationGauge)
            .run((eventId, request))
        actual.unsafeRunSync() shouldBe PayloadMalformed(
          "Attempt to decode value on failed cursor: DownField(payload)"
        )
      }

      "return PayloadMalformed if the decoding failed because no  status is present " in new TestCase {
        val triples    = eventPayloads.generateOne
        val payloadStr = json"""{ "schemaVersion": ${schemaVersion.value}, "payload": ${triples.value}  }"""

        val body = Multipart[IO](
          Vector(
            Part.formData[IO]("event", json"""{ }""".noSpaces, `Content-Type`(MediaType.application.json)),
            Part.formData[IO]("payload", payloadStr.noSpaces)
          )
        )
        val request = Request[IO]().withEntity(body).withHeaders(body.headers)

        val actual = ToTriplesGenerated
          .factory[IO](underTriplesGenerationGauge, awaitingTransformationGauge)
          .run((eventId, request))

        actual.unsafeRunSync() shouldBe PayloadMalformed("No status property in status change payload")
      }
    }

  }

  private trait TestCase {
    val awaitingTransformationGauge = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesGenerationGauge = mock[LabeledGauge[IO, projects.Path]]
    val histogram                   = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val currentTime    = mockFunction[Instant]
    val eventId        = compoundEventIds.generateOne
    val eventBatchDate = batchDates.generateOne
    val processingTime = eventProcessingTimes.generateSome

    val payload       = eventPayloads.generateOne
    val schemaVersion = projectSchemaVersions.generateOne
    val commandRunner = new StatusUpdatesRunnerImpl(transactor, histogram, TestLogger[IO]())
    val now           = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
