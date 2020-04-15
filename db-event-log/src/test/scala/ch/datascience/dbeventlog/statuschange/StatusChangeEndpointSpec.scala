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
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.EventStatus.TriplesStore
import ch.datascience.dbeventlog.statuschange.commands.{ChangeStatusCommand, ToTriplesStore, UpdateResult}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class StatusChangeEndpointSpec extends WordSpec with MockFactory {

  "changeStatus" should {

    "decode payload from the request, " +
      "perform request event status update " +
      s"and return $Ok if all went fine - $TriplesStore status case" in new TestCase {

      val command: ChangeStatusCommand = ToTriplesStore(eventId)
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

    "decode payload from the request, " +
      "perform request event status update " +
      s"and return $Conflict if no event gets updated - $TriplesStore status case" in new TestCase {

      val command: ChangeStatusCommand = ToTriplesStore(eventId)
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

    "decode payload from the request, " +
      "perform request event status update " +
      s"and return $InternalServerError if there was an error during update - $TriplesStore status case" in new TestCase {

      val errorMessage = nonBlankStrings().generateOne
      val command: ChangeStatusCommand = ToTriplesStore(eventId)
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

      logger.expectNoLogs()
    }

    s"return $BadRequest if decoding payload fails" in new TestCase {

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

    s"return $InternalServerError when updating event status fails" in new TestCase {

      val command: ChangeStatusCommand = ToTriplesStore(eventId)
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
    val eventId = compoundEventIds.generateOne

    val commandsRunner = mock[TestUpdateCommandsRunner]
    val logger         = TestLogger[IO]()
    val changeStatus   = new StatusChangeEndpoint[IO](commandsRunner, logger).changeStatus _
  }

  implicit val commandEncoder: Encoder[ChangeStatusCommand] = Encoder.instance[ChangeStatusCommand] {
    case command @ ToTriplesStore(_, _) => json"""{
        "status": ${command.status.value}
      }"""
  }

  private class TestUpdateCommandsRunner(transactor: DbTransactor[IO, EventLogDB])
      extends UpdateCommandsRunner[IO](transactor)
}
