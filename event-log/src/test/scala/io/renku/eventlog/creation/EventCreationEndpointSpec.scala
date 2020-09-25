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

package io.renku.eventlog.creation

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.creation.EventPersister.Result
import io.renku.eventlog.{Event, EventProject}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventCreationEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "addEvent" should {

    "decode an Event from the request, " +
      "create it in the Event Log " +
      s"and return $Created " +
      "if there's no such an event in the Log yet" in new TestCase {

        val event = events.generateOne
        (persister.storeNewEvent _)
          .expects(event)
          .returning(Result.Created.pure[IO])

        val request = Request(Method.POST, uri"events").withEntity(event.asJson)

        val response = addEvent(request).unsafeRunSync()

        response.status                          shouldBe Created
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event created")

        logger.loggedOnly(Info(s"Event ${event.compoundEventId}, projectPath = ${event.project.path} added"))
      }

    "decode an Event from the request, " +
      "create it in the Event Log " +
      s"and return $Ok " +
      "if such an event was already in the Log" in new TestCase {

        val event = events.generateOne
        (persister.storeNewEvent _)
          .expects(event)
          .returning(Result.Existed.pure[IO])

        val request = Request(Method.POST, uri"events").withEntity(event.asJson)

        val response = addEvent(request).unsafeRunSync()

        response.status                          shouldBe Ok
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event existed")

        logger.expectNoLogs()
      }

    s"return $BadRequest if decoding Event from the request fails" in new TestCase {

      val payload = jsons.generateOne
      val request = Request(Method.POST, uri"events").withEntity(payload)

      val response = addEvent(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: $payload"
      )

      logger.expectNoLogs()
    }

    s"return $InternalServerError when storing Event in the Log fails" in new TestCase {

      val event     = events.generateOne
      val exception = exceptions.generateOne
      (persister.storeNewEvent _)
        .expects(event)
        .returning(exception.raiseError[IO, EventPersister.Result])

      val request = Request(Method.POST, uri"events").withEntity(event.asJson)

      val response = addEvent(request).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Event creation failed").asJson

      logger.loggedOnly(Error("Event creation failed", exception))
    }
  }

  private trait TestCase {
    val persister = mock[EventPersister[IO]]
    val logger    = TestLogger[IO]()
    val addEvent  = new EventCreationEndpoint[IO](persister, logger).addEvent _
  }

  private implicit lazy val eventEncoder: Encoder[Event] = Encoder.instance[Event] { event =>
    json"""{
      "id":         ${event.id.value},
      "project":    ${event.project},
      "date":       ${event.date.value},
      "batchDate":  ${event.batchDate.value},
      "body":       ${event.body.value}
    }"""
  }

  private implicit lazy val projectEncoder: Encoder[EventProject] = Encoder.instance[EventProject] { project =>
    json"""{
      "id":   ${project.id.value},
      "path": ${project.path.value}
    }"""
  }
}
