/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.client

import cats.effect.{ContextShift, IO}
import ch.datascience.config.ServiceUrl
import ch.datascience.control.Throttler
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Method.GET
import org.http4s.{Request, Response, Status}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IORestClientSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  "send" should {

    "succeed returning value calculated with the given response mapping rules " +
      "if the response matches the rules" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(ok("1"))
      }

      verifyThrottling()

      client.callRemote.unsafeRunSync() shouldBe 1
    }

    "fail if remote responds with status which does not match the response mapping rules" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(
            aResponse
              .withStatus(Status.NotFound.code)
              .withBody("some body")
          )
      }

      verifyThrottling()

      intercept[Exception] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET $hostUrl/resource returned ${Status.NotFound}; body: some body"
    }

    "fail if remote responds with a body which doesn't match the response mapping rules" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(ok("non int"))
      }

      verifyThrottling()

      intercept[Exception] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"""GET $hostUrl/resource returned ${Status.Ok}; error: For input string: "non int""""
    }

    "fail if remote responds with an empty body and status which doesn't match the response mapping rules" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(noContent())
      }

      verifyThrottling()

      intercept[Exception] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET $hostUrl/resource returned ${Status.NoContent}; body: "
    }

    "fail if there are connectivity problems" in {
      intercept[Exception] {
        new TestRestClient(ServiceUrl("http://localhost:1024"), Throttler.noThrottling).callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET http://localhost:1024/resource error: Connection refused"
    }
  }

  private trait TestCase {
    val throttler = mock[Throttler[IO, Any]]
    val client    = new TestRestClient(hostUrl, throttler)

    def verifyThrottling(): Unit =
      inSequence {
        (throttler.acquire _).expects().returning(IO.unit)
        (throttler.release _).expects().returning(IO.unit)
      }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)
  private val hostUrl = ServiceUrl(externalServiceBaseUrl)

  private class TestRestClient(hostUrl: ServiceUrl, throttler: Throttler[IO, Any]) extends IORestClient(throttler) {

    def callRemote: IO[Int] =
      for {
        uri         <- validateUri(s"$hostUrl/resource")
        accessToken <- send(request(GET, uri))(mapResponse)
      } yield accessToken

    private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Int]] = {
      case (Status.Ok, _, response) => response.as[String].map(_.toInt)
    }
  }
}
