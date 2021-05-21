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

package ch.datascience.http.client

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.ServiceUrl
import ch.datascience.control.Throttler
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.RestClientError._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.{ExecutionTimeRecorder, TestExecutionTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.prometheus.client.Histogram
import org.http4s.Method.{GET, POST}
import org.http4s.client.ConnectionFailure
import org.http4s.{multipart => _, _}
import MediaType._
import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.http.Fault._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

import java.net.ConnectException
import java.util.concurrent.TimeoutException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class RestClientSpec extends AnyWordSpec with ExternalServiceStubbing with MockFactory with should.Matchers {

  "send" should {

    "succeed returning value calculated with the given response mapping rules " +
      "if the response matches the rules" in new TestCase {

        stubFor {
          get("/resource")
            .willReturn(ok("1"))
        }

        verifyThrottling()

        client.callRemote.unsafeRunSync() shouldBe 1

        logger.loggedOnly(Warn(s"GET $hostUrl/resource finished${executionTimeRecorder.executionTimeInfo}"))
      }

    "succeed returning value calculated with the given response mapping rules " +
      "and do not measure execution time if Time Recorder not given" in new TestCase {

        stubFor {
          get("/resource")
            .willReturn(ok("1"))
        }

        verifyThrottling()

        override val client = new TestRestClient(hostUrl, throttler, logger, maybeTimeRecorder = None)

        client.callRemote.unsafeRunSync() shouldBe 1

        logger.expectNoLogs()
      }

    "succeed returning value calculated with the given response mapping rules and " +
      "log execution time along with the given request name if Time Recorder present" in new TestCase {

        stubFor {
          get("/resource")
            .willReturn(ok("1"))
        }

        verifyThrottling()

        val requestName: String Refined NonEmpty = "some request"
        client.callRemote(requestName).unsafeRunSync() shouldBe 1

        logger.loggedOnly(Warn(s"$requestName finished${executionTimeRecorder.executionTimeInfo}"))
      }

    "cause the given histogram to capture execution time - case with some given label" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(ok("1"))
      }

      verifyThrottling()

      val requestName: String Refined NonEmpty = "some request"
      client.callRemote(requestName).unsafeRunSync() shouldBe 1

      val Some(sample) = histogram.collect().asScala.flatMap(_.samples.asScala).lastOption
      sample.value               should be >= 0d
      sample.labelNames.asScala  should contain only histogramLabel.value
      sample.labelValues.asScala should contain only requestName.value
    }

    "cause the given histogram to capture execution time - case without label" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(ok("1"))
      }

      verifyThrottling()

      override val histogram = Histogram.build("histogram", "help").create()

      client.callRemote.unsafeRunSync() shouldBe 1

      val Some(sample) = histogram.collect().asScala.flatMap(_.samples.asScala).lastOption
      sample.value                 should be >= 0d
      sample.labelNames.asScala  shouldBe empty
      sample.labelValues.asScala shouldBe empty
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

      intercept[UnexpectedResponseException] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET $hostUrl/resource returned ${Status.NotFound}; body: some body"
    }

    "fail if remote responds with an empty body and status which doesn't match the response mapping rules" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(noContent())
      }

      verifyThrottling()

      intercept[UnexpectedResponseException] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET $hostUrl/resource returned ${Status.NoContent}; body: "
    }

    "fail if remote responds with a BAD_REQUEST and it's not mapped in the given response mapping rules" in new TestCase {

      val responseBody = nonBlankStrings().generateOne
      stubFor {
        get("/resource")
          .willReturn(aResponse.withStatus(Status.BadRequest.code).withBody(responseBody))
      }

      verifyThrottling()

      intercept[BadRequestException] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"GET $hostUrl/resource returned ${Status.BadRequest}; body: $responseBody"
    }

    "fail if remote responds with a body which causes exception during mapping" in new TestCase {

      stubFor {
        get("/resource")
          .willReturn(ok("non int"))
      }

      verifyThrottling()

      val exception = intercept[MappingException] {
        client.callRemote.unsafeRunSync()
      }

      exception.getMessage shouldBe s"""GET $hostUrl/resource returned ${Status.Ok}; error: For input string: "non int""""
      exception.getCause   shouldBe a[NumberFormatException]
    }

    "fail after retrying if there is a persistent connectivity problem" in {
      val logger = TestLogger[IO]()

      val exceptionMessage =
        "Error connecting to http://localhost:1024 using address localhost:1024 (unresolved: false)"

      val exception = intercept[ConnectivityException] {
        new TestRestClient(ServiceUrl("http://localhost:1024"), Throttler.noThrottling, logger, None).callRemote
          .unsafeRunSync()
      }
      exception.getMessage shouldBe s"GET http://localhost:1024/resource error: $exceptionMessage"
      exception.getCause   shouldBe a[ConnectionFailure]

      logger.loggedOnly(
        Warn(s"GET http://localhost:1024/resource timed out -> retrying attempt 1 error: $exceptionMessage"),
        Warn(s"GET http://localhost:1024/resource timed out -> retrying attempt 2 error: $exceptionMessage")
      )
    }

    Fault.values().filterNot(_ == MALFORMED_RESPONSE_CHUNK) foreach { fault =>
      s"fail after retrying if there is a persistent $fault problem" in new TestCase {

        stubFor {
          get("/resource")
            .willReturn(aResponse.withFault(fault))
        }

        verifyThrottling()

        val exceptionMessage = s"Failed to connect to endpoint: $hostUrl"

        val exception = intercept[ConnectivityException] {
          client.callRemote.unsafeRunSync()
        }
        exception.getMessage shouldBe s"GET $hostUrl/resource error: $exceptionMessage"
        exception.getCause   shouldBe a[ConnectException]

        logger.loggedOnly(
          Warn(s"GET $hostUrl/resource timed out -> retrying attempt 1 error: $exceptionMessage"),
          Warn(s"GET $hostUrl/resource timed out -> retrying attempt 2 error: $exceptionMessage")
        )
      }
    }

    "use the overridden idle timeout" in new TestCase {

      val idleTimeout = 500 millis

      stubFor {
        get("/resource")
          .willReturn(ok("1").withFixedDelay((idleTimeout.toMillis + 200).toInt))
      }

      val exception = intercept[ClientException] {
        new TestRestClient(hostUrl,
                           Throttler.noThrottling,
                           logger,
                           maybeTimeRecorder = None,
                           idleTimeoutOverride = idleTimeout.some
        ).callRemote.unsafeRunSync()
      }

      exception          shouldBe a[ClientException]
      exception.getCause shouldBe a[TimeoutException]
      exception.getMessage should not be empty
    }

    "use the overridden request timeout" in new TestCase {

      val requestTimeout = 500 millis

      stubFor {
        get("/resource")
          .willReturn(ok("1").withFixedDelay((requestTimeout.toMillis + 500).toInt))
      }

      val exception = intercept[ClientException] {
        new TestRestClient(hostUrl,
                           Throttler.noThrottling,
                           logger,
                           maybeTimeRecorder = None,
                           maybeRequestTimeoutOverride = requestTimeout.some
        ).callRemote.unsafeRunSync()
      }

      exception          shouldBe a[ClientException]
      exception.getCause shouldBe a[TimeoutException]
      exception.getMessage should not be empty
    }
  }

  "multipart builder" should {
    "successfully build a multipart request" in new TestCase {

      val jsonPart = nonEmptyStrings().generateOne -> jsons.generateOne
      val textPart = nonEmptyStrings().generateOne -> nonEmptyStrings().generateOne

      stubFor {
        post("/resource")
          .withMultipartRequestBody(
            aMultipart(jsonPart._1)
              .withBody(equalToJson(jsonPart._2.noSpaces))
              .withHeader("Content-Type", equalTo(s"${application.json.mainType}/${application.json.subType}"))
          )
          .withMultipartRequestBody(
            aMultipart(textPart._1)
              .withBody(equalTo(textPart._2))
              .withHeader("Content-Type", equalTo(s"${text.plain.mainType}/${text.plain.subType}"))
          )
          .withHeader(
            "Content-Type",
            containing(s"${multipart.`form-data`.mainType}/${multipart.`form-data`.subType}")
          )
          .willReturn(ok("1"))
      }

      verifyThrottling()

      client.callMultipartEndpoint(jsonPart, textPart).unsafeRunSync() shouldBe 1
      verify {
        postRequestedFor(urlEqualTo("/resource")).withoutHeader("Transfer-encoding")
      }
    }
  }

  private trait TestCase {
    val histogramLabel: String Refined NonEmpty = "label"
    val histogram             = Histogram.build("histogram", "help").labelNames(histogramLabel.value).create()
    val throttler             = mock[Throttler[IO, Any]]
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger, Some(histogram))
    val client                = new TestRestClient(hostUrl, throttler, logger, Some(executionTimeRecorder))

    def verifyThrottling() = inSequence {
      (throttler.acquire _).expects().returning(IO.unit)
      (throttler.release _).expects().returning(IO.unit)
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)
  private lazy val hostUrl = ServiceUrl(externalServiceBaseUrl)

  private class TestRestClient(hostUrl:                     ServiceUrl,
                               throttler:                   Throttler[IO, Any],
                               logger:                      Logger[IO],
                               maybeTimeRecorder:           Option[ExecutionTimeRecorder[IO]],
                               idleTimeoutOverride:         Option[Duration] = None,
                               maybeRequestTimeoutOverride: Option[Duration] = None
  ) extends RestClient(throttler,
                       logger,
                       maybeTimeRecorder,
                       retryInterval = 1 millisecond,
                       maxRetries = 2,
                       idleTimeoutOverride,
                       maybeRequestTimeoutOverride
      ) {

    def callRemote: IO[Int] =
      for {
        uri         <- validateUri(s"$hostUrl/resource")
        accessToken <- send(request(GET, uri))(mapResponse)
      } yield accessToken

    def callRemote(requestName: String Refined NonEmpty): IO[Int] =
      for {
        uri         <- validateUri(s"$hostUrl/resource")
        accessToken <- send(HttpRequest(request(GET, uri), requestName))(mapResponse)
      } yield accessToken

    def callMultipartEndpoint(jsonPart: (String, Json), textPart: (String, String)): IO[Int] = for {
      uri <- validateUri(s"$hostUrl/resource")
      response <-
        send(
          request(POST, uri).withMultipartBuilder
            .addPart(jsonPart._1, jsonPart._2)
            .addPart(textPart._1, textPart._2)
            .build()
        )(mapResponse)
    } yield response

    private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Int]] = {
      case (Status.Ok, _, response) => response.as[String].map(_.toInt)
    }
  }
}
