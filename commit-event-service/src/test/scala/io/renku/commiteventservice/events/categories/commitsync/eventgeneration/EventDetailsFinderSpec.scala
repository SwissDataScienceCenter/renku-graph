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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{Created, NotFound, Ok}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

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

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger()
    val event              = newCommitEvents.generateOne
    val eventLogUrl        = EventLogUrl(externalServiceBaseUrl)
    val eventDetailsFinder = new EventDetailsFinderImpl[IO](eventLogUrl)
  }
}
