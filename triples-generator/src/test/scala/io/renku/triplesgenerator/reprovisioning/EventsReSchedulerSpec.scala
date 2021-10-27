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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{Accepted, BadRequest}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventsReSchedulerSpec extends AnyWordSpec with IOSpec with ExternalServiceStubbing with should.Matchers {

  "triggerEventsReScheduling" should {

    s"succeed if posting to Event Log's events/status/NEW results with $Accepted" in new TestCase {

      stubFor {
        post(urlEqualTo("/events"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(json"""{"categoryName": "EVENTS_STATUS_CHANGE" ,"newStatus": "NEW"}""".spaces2))
          )
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.triggerEventsReScheduling().unsafeRunSync() shouldBe ((): Unit)
    }

    s"fail when posting to Event Log's events/status/NEW results in status different than $Accepted" in new TestCase {

      val message = "message"
      stubFor {
        post(urlEqualTo("/events"))
          .willReturn(badRequest().withBody(message))
      }

      intercept[Exception] {
        sender.triggerEventsReScheduling().unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events returned $BadRequest; body: $message"
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val sender      = new EventsReSchedulerImpl[IO](eventLogUrl)
  }
}
