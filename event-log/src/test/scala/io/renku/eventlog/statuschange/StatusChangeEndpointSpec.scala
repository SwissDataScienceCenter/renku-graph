/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.statuschange

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.prometheus.client.Gauge
import io.renku.eventlog.DbEventLogGenerators.{eventMessages, eventPayloads}
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands._
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.graph.model.GraphModelGenerators._

class StatusChangeEndpointSpec
    extends AnyWordSpec
    with MockFactory
    with TableDrivenPropertyChecks
    with should.Matchers {

  "changeStatus" should {

    val scenarios = Table(
      "status"     -> "command builder",
      TriplesStore -> ToTriplesStore[IO](compoundEventIds.generateOne, underTriplesGenerationGauge),
      Skipped      -> ToSkipped[IO](compoundEventIds.generateOne, eventMessages.generateOne, underTriplesGenerationGauge),
      New          -> ToNew[IO](compoundEventIds.generateOne, awaitingTriplesGenerationGauge, underTriplesGenerationGauge),
      TriplesGenerated -> ToTriplesGenerated[IO](
        compoundEventIds.generateOne,
        eventPayloads.generateOne,
        projectSchemaVersions.generateOne,
        underTriplesGenerationGauge,
        awaitingTriplesTransformationGauge
      ),
      GenerationRecoverableFailure -> ToGenerationRecoverableFailure[IO](
        compoundEventIds.generateOne,
        eventMessages.generateOption,
        awaitingTriplesGenerationGauge,
        underTriplesGenerationGauge
      ),
      GenerationNonRecoverableFailure -> ToGenerationNonRecoverableFailure[IO](compoundEventIds.generateOne,
                                                                               eventMessages.generateOption,
                                                                               underTriplesGenerationGauge
      ),
      TransformationRecoverableFailure -> ToTransformationRecoverableFailure[IO](
        compoundEventIds.generateOne,
        eventMessages.generateOption,
        awaitingTriplesTransformationGauge,
        underTriplesTransformationGauge
      ),
      TransformationNonRecoverableFailure -> ToTransformationNonRecoverableFailure[IO](compoundEventIds.generateOne,
                                                                                       eventMessages.generateOption,
                                                                                       underTriplesTransformationGauge
      )
    )
    forAll(scenarios) { (status, command) =>
      "decode payload from the body, " +
        "perform status update " +
        s"and return $Ok if all went fine - $status status case" in new TestCase {

          command.status shouldBe status

          val eventId = command.eventId

          (commandsRunner.run _)
            .expects(command)
            .returning(Updated.pure[IO])

          val request = Request(
            Method.PATCH,
            uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
          ).withEntity(command.asJson)

          val response = changeStatus(eventId, request).unsafeRunSync()

          response.status                          shouldBe Ok
          response.contentType                     shouldBe Some(`Content-Type`(application.json))
          response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event status updated")

          logger.expectNoLogs()
        }

      "decode payload from the body, " +
        "perform status update " +
        s"and return $Conflict if no event gets updated - $status status case" in new TestCase {

          val eventId = command.eventId

          (commandsRunner.run _)
            .expects(command)
            .returning(UpdateResult.Conflict.pure[IO])

          val request = Request(
            Method.PATCH,
            uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
          ).withEntity(command.asJson)

          val response = changeStatus(eventId, request).unsafeRunSync()

          response.status                          shouldBe Conflict
          response.contentType                     shouldBe Some(`Content-Type`(application.json))
          response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event status cannot be updated")

          logger.expectNoLogs()
        }

      "decode payload from the body, " +
        "perform status update " +
        s"and return $InternalServerError if there was an error during update - $status status case" in new TestCase {

          val eventId = command.eventId

          val errorMessage = nonBlankStrings().generateOne
          (commandsRunner.run _)
            .expects(command)
            .returning(UpdateResult.Failure(errorMessage).pure[IO])

          val request = Request(
            Method.PATCH,
            uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
          ).withEntity(command.asJson)

          val response = changeStatus(eventId, request).unsafeRunSync()

          response.status                          shouldBe InternalServerError
          response.contentType                     shouldBe Some(`Content-Type`(application.json))
          response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage.value)

          logger.loggedOnly(Error(errorMessage.value))
        }
    }

    s"return $BadRequest if decoding payload fails" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload = json"""{}"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: $payload"
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest for $Skipped status with no message" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload = json"""{"status": ${Skipped.value}}"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("SKIPPED status needs a message")

      logger.expectNoLogs()
    }

    s"return $BadRequest for $TriplesGenerated status with no payload" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload =
        json"""{"status": ${TriplesGenerated.value}, "schema_version": ${projectSchemaVersions.generateOne.value} }"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("TRIPLES_GENERATED status needs a payload")

      logger.expectNoLogs()
    }

    s"return $BadRequest for $TriplesGenerated status with no schema version" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload = json"""{"status": ${TriplesGenerated.value}, "payload": ${eventPayloads.generateOne.value} }"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("TRIPLES_GENERATED status needs a schema_version")

      logger.expectNoLogs()
    }

    s"return $BadRequest for unsupported status" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload = json"""{"status": "GENERATING_TRIPLES"}"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
        "Transition to 'GENERATING_TRIPLES' status unsupported"
      )

      logger.expectNoLogs()
    }

    s"return $InternalServerError when updating event status fails" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val command: ChangeStatusCommand[IO] = ToTriplesStore(eventId, underTriplesGenerationGauge)
      val exception = exceptions.generateOne
      (commandsRunner.run _)
        .expects(command)
        .returning(exception.raiseError[IO, UpdateResult])

      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(command.asJson)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Event status update failed").asJson

      logger.loggedOnly(Error("Event status update failed", exception))
    }
  }

  private trait TestCase {
    val commandsRunner = mock[StatusUpdatesRunner[IO]]
    val logger         = TestLogger[IO]()
    val changeStatus = new StatusChangeEndpoint[IO](
      commandsRunner,
      awaitingTriplesGenerationGauge,
      underTriplesGenerationGauge,
      awaitingTriplesTransformationGauge,
      underTriplesTransformationGauge,
      logger
    ).changeStatus _
  }

  implicit val commandEncoder: Encoder[ChangeStatusCommand[IO]] = Encoder.instance[ChangeStatusCommand[IO]] {
    case command: ToNew[IO]                                 => json"""{
        "status": ${command.status.value}
      }"""
    case command: ToTriplesStore[IO]                        => json"""{
        "status": ${command.status.value}
      }"""
    case command: ToSkipped[IO]                             => json"""{
        "status": ${command.status.value},
        "message": ${command.message.value}
      }"""
    case command: ToTriplesGenerated[IO]                    => json"""{
        "status": ${command.status.value},
        "payload": ${command.payload.value},
        "schema_version": ${command.schemaVersion.value}
      }"""
    case command: ToGenerationRecoverableFailure[IO]        => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
    case command: ToGenerationNonRecoverableFailure[IO]     => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
    case command: ToTransformationRecoverableFailure[IO]    => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
    case command: ToTransformationNonRecoverableFailure[IO] => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
  }

  private lazy val awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path] = new GaugeStub
  private lazy val underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path] = new GaugeStub
  private lazy val awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path] = new GaugeStub
  private lazy val underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path] = new GaugeStub

  private class GaugeStub extends LabeledGauge[IO, projects.Path] {
    override def set(labelValue: (projects.Path, Double)) = IO.unit

    override def increment(labelValue: projects.Path) = IO.unit

    override def decrement(labelValue: projects.Path) = IO.unit

    override def reset() = IO.unit

    protected override def gauge = Gauge.build("name", "help").create()
  }
}
