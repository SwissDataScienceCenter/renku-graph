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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration
package historytraversal

import ch.datascience.commiteventservice.events.categories.common.Generators._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status.{Created, NotFound, Ok}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventDetailsFinderSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "checkIfExists" should {

    s"return true when event log responds with Ok" in new TestCase {

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(aResponse().withStatus(Ok.code))
      }

      eventDetailsFinder.checkIfExists(event.project.id, event.id).unsafeRunSync() shouldBe true
    }

    s"return false when event log responds with NotFound" in new TestCase {

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(aResponse().withStatus(NotFound.code))
      }

      eventDetailsFinder.checkIfExists(event.project.id, event.id).unsafeRunSync() shouldBe false
    }

    s"fail when event log responds with other statuses" in new TestCase {

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(aResponse().withStatus(Created.code))
      }
      intercept[Exception] {
        eventDetailsFinder.checkIfExists(event.project.id, event.id).unsafeRunSync()
      }
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val event              = newCommitEvents.generateOne
    val eventLogUrl        = EventLogUrl(externalServiceBaseUrl)
    val eventDetailsFinder = new EventDetailsFinderImpl[IO](eventLogUrl, TestLogger())
  }
}
