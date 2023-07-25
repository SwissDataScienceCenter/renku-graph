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

package io.renku.eventlog.eventdetails

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import io.renku.graph.model.events.EventDetails
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "getDetails" should {

    s"$Ok if the event is found in the event log " in new TestCase {

      (eventDetailsFinder.findDetails _).expects(event.compoundEventId).returning(event.some.pure[IO])

      val response = eventDetailEndpoint.getDetails(event.compoundEventId).unsafeRunSync()

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response
        .as[Json]
        .unsafeRunSync() shouldBe json"""{ "id":${event.id.value}, "project": {"id": ${event.projectId.value}}, "body":${event.eventBody.value} } """
    }

    s"$NotFound if the event is not found in the event log " in new TestCase {

      (eventDetailsFinder.findDetails _)
        .expects(event.compoundEventId)
        .returning(Option.empty[EventDetails].pure[IO])

      val response = eventDetailEndpoint.getDetails(event.compoundEventId).unsafeRunSync()

      response.status                      shouldBe NotFound
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Info("Event not found")
    }

    s"$InternalServerError if finding the details fails" in new TestCase {
      val exception = exceptions.generateOne

      (eventDetailsFinder.findDetails _)
        .expects(event.compoundEventId)
        .returning(exception.raiseError[IO, Option[EventDetails]])

      val response = eventDetailEndpoint.getDetails(event.compoundEventId).unsafeRunSync()

      response.status                      shouldBe InternalServerError
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error("Finding event details failed")

      logger.loggedOnly(Error("Finding event details failed", exception))
    }
  }

  private trait TestCase {
    val event = eventDetails.generateOne

    private lazy val eventDetails: Gen[EventDetails] = for {
      eventId   <- compoundEventIds
      eventBody <- eventBodies
    } yield EventDetails(eventId, eventBody)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventDetailsFinder  = mock[EventDetailsFinder[IO]]
    val eventDetailEndpoint = new EventDetailsEndpointImpl[IO](eventDetailsFinder)
  }
}
