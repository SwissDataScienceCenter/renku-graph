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

package io.renku.eventlog.eventdetails

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.InfoMessage._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Json
import io.circe.literal.JsonStringContext
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "getDetails" should {

    s"$Ok if the event is found in the event log " in new TestCase {

      (eventDetailsFinder.findDetails _).expects(eventId).returning(eventId.some.pure[IO])

      val response = eventDetailEndpoint.getDetails(eventId).unsafeRunSync()

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response
        .as[Json]
        .unsafeRunSync() shouldBe json"""{ "id":${eventId.id.value}, "project": {"id": ${eventId.projectId.value}} } """
    }

    s"$NotFound if the event is not found in the event log " in new TestCase {

      (eventDetailsFinder.findDetails _).expects(eventId).returning(Option.empty[CompoundEventId].pure[IO])

      val response = eventDetailEndpoint.getDetails(eventId).unsafeRunSync()

      response.status                          shouldBe NotFound
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event not found")
    }

    s"$InternalServerError if finding the details fails" in new TestCase {
      val exception = exceptions.generateOne

      (eventDetailsFinder.findDetails _).expects(eventId).returning(exception.raiseError[IO, Option[CompoundEventId]])

      val response = eventDetailEndpoint.getDetails(eventId).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Finding event details failed")

      logger.loggedOnly(Error("Finding event details failed", exception))
    }
  }

  private trait TestCase {
    val eventId = compoundEventIds.generateOne

    val logger = TestLogger[IO]()

    val eventDetailsFinder  = mock[EventDetailsFinder[IO]]
    val eventDetailEndpoint = new EventDetailsEndpointImpl[IO](eventDetailsFinder, logger)
  }
}
