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

package io.renku.eventlog.eventspatching

import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.LabeledGauge
import io.circe.literal._
import io.renku.eventlog.DbEventLogGenerators.eventStatuses
import io.renku.eventlog.EventStatus
import io.renku.eventlog.EventStatus.New
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.implicits._
import org.http4s.headers.`Content-Type`
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

class EventsPatchingEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "triggerEventsPatching" should {

    s"trigger events re-scheduling and return $Accepted" in new TestCase {

      val patch = CaptureAll[EventsPatch[IO]]()
      (eventsPatcher.applyToAllEvents _)
        .expects(capture(patch))
        .returning(IO.unit)

      val request = Request[IO](Method.PATCH, uri"events").withEntity(payload(New))

      val response = triggerEventsPatching(request).unsafeRunSync()

      patch.value.getClass shouldBe StatusNewPatch(waitingEventsGauge, underProcessingGauge).getClass

      response.status                        shouldBe Accepted
      response.contentType                   shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Events patching triggered")

      logger.expectNoLogs()
    }

    s"return $Accepted regardless of the outcome of the call to events re-scheduling" in new TestCase {

      val exception = exceptions.generateOne
      (eventsPatcher.applyToAllEvents _)
        .expects(*)
        .returning(exception.raiseError[IO, Unit])

      val request = Request[IO](Method.PATCH, uri"events").withEntity(payload(New))

      val response = triggerEventsPatching(request).unsafeRunSync()

      response.status                        shouldBe Accepted
      response.contentType                   shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Events patching triggered")

      logger.expectNoLogs()
    }

    s"return $BadRequest if decoding the payload from the request fails" in new TestCase {

      val payload = jsons.generateOne
      val request = Request[IO](Method.PATCH, uri"events").withEntity(payload)

      val response = triggerEventsPatching(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: $payload"
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest for unsupported status" in new TestCase {

      val status  = eventStatuses.generateDifferentThan(New)
      val request = Request[IO](Method.PATCH, uri"events").withEntity(payload(status))

      val response = triggerEventsPatching(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync shouldBe ErrorMessage(
        s"Patching events to '$status' status unsupported"
      )

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    def payload(status: EventStatus) = json"""{"status": ${status.value}}"""

    val eventsPatcher        = mock[EventsPatcher[IO]]
    val waitingEventsGauge   = mock[LabeledGauge[IO, Path]]
    val underProcessingGauge = mock[LabeledGauge[IO, Path]]

    val logger = TestLogger[IO]()
    val triggerEventsPatching = new EventsPatchingEndpointImpl(
      eventsPatcher,
      waitingEventsGauge,
      underProcessingGauge,
      logger
    ).triggerEventsPatching _
  }
}
