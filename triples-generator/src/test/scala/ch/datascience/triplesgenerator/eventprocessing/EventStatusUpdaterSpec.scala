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

import java.io.{PrintWriter, StringWriter}

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.exceptions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventStatusUpdaterSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "markNew" should {

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventNew(eventId).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withRequestBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventNew(eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markDone" should {

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(equalToJson(json"""{"status": "TRIPLES_STORE"}""".spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventDone(eventId).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withRequestBody(equalToJson(json"""{"status": "TRIPLES_STORE"}""".spaces2))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventDone(eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markEventFailedRecoverably" should {

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        val exception = exceptions.generateOne
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(
              equalToJson(json"""{"status": "RECOVERABLE_FAILURE", "message": ${asString(exception)}}""".spaces2)
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventFailedRecoverably(eventId, exception).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventFailedRecoverably(eventId, exceptions.generateOne).unsafeRunSync()
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markEventFailedNonRecoverably" should {

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        val exception = exceptions.generateOne
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(
              equalToJson(json"""{"status": "NON_RECOVERABLE_FAILURE", "message": ${asString(exception)}}""".spaces2)
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventFailedNonRecoverably(eventId, exception).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventFailedNonRecoverably(eventId, exceptions.generateOne).unsafeRunSync()
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markEventSkipped" should {

    val message = "MigrationEvent"

    Set(Ok, Conflict) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(
              equalToJson(json"""{"status": "SKIPPED", "message": $message}""".spaces2)
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventSkipped(eventId, message).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventSkipped(eventId, message).unsafeRunSync()
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventId = compoundEventIds.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val updater     = new IOEventStatusUpdater(eventLogUrl, TestLogger())
  }

  private def asString(exception: Exception): String = {
    val exceptionAsString = new StringWriter
    exception.printStackTrace(new PrintWriter(exceptionAsString))
    exceptionAsString.flush()
    exceptionAsString.toString.trim
  }
}
