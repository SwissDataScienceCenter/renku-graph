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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status.{Accepted, BadRequest}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventsReSchedulerSpec extends WordSpec with ExternalServiceStubbing {

  "triggerEventsReScheduling" should {

    s"succeed if posting to Event Log's events/status/NEW results with $Accepted" in new TestCase {

      stubFor {
        post("/events/status/NEW")
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.triggerEventsReScheduling.unsafeRunSync() shouldBe ((): Unit)
    }

    s"fail when posting to Event Log's events/status/NEW results in status different than $Accepted" in new TestCase {

      val message = "message"
      stubFor {
        post("/events/status/NEW")
          .willReturn(badRequest().withBody(message))
      }

      intercept[Exception] {
        sender.triggerEventsReScheduling.unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events/status/NEW returned $BadRequest; body: $message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val sender      = new IOEventsReScheduler(eventLogUrl, TestLogger())
  }
}
