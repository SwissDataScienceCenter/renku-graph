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

package ch.datascience.dbeventlog.statuschange

import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.dbeventlog.DbEventLogGenerators.eventMessages
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.statuschange.commands._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.prometheus.client.Gauge
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class StatusChangeEndpointSpec extends WordSpec with MockFactory with TableDrivenPropertyChecks {

  "changeStatus" should {

    val scenarios = Table(
      "status"     -> "command builder",
      New          -> ToTriplesStore[IO](compoundEventIds.generateOne, waitingEventsGauge),
      TriplesStore -> ToNew[IO](compoundEventIds.generateOne, waitingEventsGauge),
      RecoverableFailure -> ToRecoverableFailure[IO](compoundEventIds.generateOne,
                                                     eventMessages.generateOption,
                                                     waitingEventsGauge),
      NonRecoverableFailure -> ToNonRecoverableFailure[IO](compoundEventIds.generateOne,
                                                           eventMessages.generateOption,
                                                           waitingEventsGauge)
    )
    forAll(scenarios) { (status, command) =>
      "decode payload from the body, " +
        "perform status update " +
        s"and return $Ok if all went fine - $status status case" in new TestCase {

        val eventId = command.eventId

        (commandsRunner.run _)
          .expects(command)
          .returning(UpdateResult.Updated.pure[IO])

        val request = Request(
          Method.PATCH,
          uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
        ).withEntity(command.asJson)

        val response = changeStatus(eventId, request).unsafeRunSync()

        response.status                        shouldBe Ok
        response.contentType                   shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Event status updated")

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

        response.status                        shouldBe Conflict
        response.contentType                   shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Event status cannot be updated")

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

        response.status                        shouldBe InternalServerError
        response.contentType                   shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync shouldBe ErrorMessage(errorMessage.value)

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
      response.as[InfoMessage].unsafeRunSync shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: $payload"
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest for unsupported status" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val payload = json"""{"status": "PROCESSING"}"""
      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(payload)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                        shouldBe BadRequest
      response.contentType                   shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe ErrorMessage("Transition to 'PROCESSING' status unsupported")

      logger.expectNoLogs()
    }

    s"return $InternalServerError when updating event status fails" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val command: ChangeStatusCommand[IO] = ToTriplesStore(eventId, waitingEventsGauge)
      val exception = exceptions.generateOne
      (commandsRunner.run _)
        .expects(command)
        .returning(exception.raiseError[IO, UpdateResult])

      val request = Request(
        Method.PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      ).withEntity(command.asJson)

      val response = changeStatus(eventId, request).unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage("Event status update failed").asJson

      logger.loggedOnly(Error("Event status update failed", exception))
    }
  }

  private trait TestCase {
    val commandsRunner = mock[StatusUpdatesRunner[IO]]
    val logger         = TestLogger[IO]()
    val changeStatus   = new StatusChangeEndpoint[IO](commandsRunner, waitingEventsGauge, logger).changeStatus _
  }

  implicit val commandEncoder: Encoder[ChangeStatusCommand[IO]] = Encoder.instance[ChangeStatusCommand[IO]] {
    case command: ToNew[IO]                   => json"""{
        "status": ${command.status.value}
      }"""
    case command: ToTriplesStore[IO]          => json"""{
        "status": ${command.status.value}
      }"""
    case command: ToRecoverableFailure[IO]    => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
    case command: ToNonRecoverableFailure[IO] => json"""{
        "status": ${command.status.value}
      }""" deepMerge command.maybeMessage.map(m => json"""{"message": ${m.value}}""").getOrElse(Json.obj())
  }

  private lazy val waitingEventsGauge: LabeledGauge[IO, projects.Path] = new LabeledGauge[IO, projects.Path] {
    override def set(labelValue:       (projects.Path, Double)) = IO.unit
    override def increment(labelValue: projects.Path)           = IO.unit
    override def decrement(labelValue: projects.Path)           = IO.unit
    override def reset           = IO.unit
    protected override def gauge = Gauge.build("name", "help").create()
  }
}
