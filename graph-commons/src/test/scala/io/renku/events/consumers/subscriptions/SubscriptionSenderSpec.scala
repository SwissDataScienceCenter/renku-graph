/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.events.consumers.subscriptions

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionSenderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "postToEventLog" should {

    s"succeed if posting payload results with $Accepted" in new TestCase {

      stubFor {
        post("/subscriptions")
          .withRequestBody(equalToJson(payload.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.postToEventLog(payload).unsafeRunSync() shouldBe ()
    }

    "fail when posting the payload results in any other status" in new TestCase {

      val message = "message"
      stubFor {
        post("/subscriptions")
          .withRequestBody(equalToJson(payload.spaces2))
          .willReturn(badRequest().withBody(message))
      }

      intercept[Exception] {
        sender.postToEventLog(payload).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/subscriptions returned $BadRequest; body: $message"
    }
  }

  private trait TestCase {
    val payload = jsons.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val sender      = new SubscriptionSenderImpl[IO](eventLogUrl)
  }
}
