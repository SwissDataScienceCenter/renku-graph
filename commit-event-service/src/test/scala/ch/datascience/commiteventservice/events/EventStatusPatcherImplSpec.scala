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

package ch.datascience.commiteventservice.events

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, patch, stubFor, urlEqualTo}
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import org.http4s.Status.{Accepted, Created, Ok}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
class EventStatusPatcherImplSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "sendDeletionStatus" should {
    "return unit if the status change was accepted" in new TestCase {
      stubFor {
        patch(urlEqualTo(s"/events/$eventId/$projectId"))
          .willReturn(aResponse().withStatus(Ok.code))
      }

      patcher.sendDeletionStatus(projectId, eventId).unsafeRunSync() shouldBe ()
    }

    "fail if the status change was not Ok" in new TestCase {
      stubFor {
        patch(urlEqualTo(s"/events/$eventId/$projectId"))
          .willReturn(
            aResponse().withStatus(
              Gen.oneOf(Gen.oneOf(Accepted, Created), clientErrorHttpStatuses, serverErrorHttpStatuses).generateOne.code
            )
          )
      }

      intercept[Exception] {
        patcher
          .sendDeletionStatus(projectId, eventId)
          .unsafeRunSync()
      } shouldBe a[Exception]
    }

    "fail if an connection error was encountered" in new TestCase {
      stubFor {
        patch(urlEqualTo(s"/events/$eventId/$projectId"))
          .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER))
      }

      intercept[Exception] {
        patcher
          .sendDeletionStatus(projectId, eventId)
          .unsafeRunSync()
      } shouldBe a[Exception]

    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val eventId     = commitIds.generateOne
    val logger      = TestLogger[IO]()
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val patcher     = new EventStatusPatcherImpl[IO](logger, eventLogUrl)
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)
}
