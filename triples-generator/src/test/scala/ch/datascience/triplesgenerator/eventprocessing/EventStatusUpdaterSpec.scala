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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventStatusUpdaterSpec extends WordSpec with ExternalServiceStubbing {

  "markDone" should {

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/projects/${eventId.projectId}/status"))
            .withRequestBody(equalToJson(json"""{"status": "TRIPLES_STORE"}""".spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markDone(eventId).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/projects/${eventId.projectId}/status"))
          .withRequestBody(equalToJson(json"""{"status": "TRIPLES_STORE"}""".spaces2))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markDone(eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/projects/${eventId.projectId}/status returned $status; body: "
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventId = compoundEventIds.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val updater     = new IOEventStatusUpdater(eventLogUrl, TestLogger())
  }
}
