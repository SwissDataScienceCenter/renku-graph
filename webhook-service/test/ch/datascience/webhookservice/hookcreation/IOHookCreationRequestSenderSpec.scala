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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{IO, Sync}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators.projectIds
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.eventprocessing.routes.WebhookEventEndpoint
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOHookCreationRequestSenderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "createHook" should {

    "send relevant Json payload and 'PRIVATE-TOKEN' header (when Personal Access Token is given) " +
      "and return Unit if the remote responds with CREATED" in new TestCase {

      expectConfigProvider(returning = IO.pure(hookCreationConfig))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .withRequestBody(equalToJson(expectedBody))
          .willReturn(created())
      }

      sender.createHook(projectId, personalAccessToken, serializedHookToken).unsafeRunSync() shouldBe ((): Unit)
    }

    "send relevant Json payload and 'Authorization' header (when OAuth Access Token is given) " +
      "and return Unit if the remote responds with CREATED" in new TestCase {

      expectConfigProvider(returning = IO.pure(hookCreationConfig))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer $oauthAccessToken"))
          .withRequestBody(equalToJson(expectedBody))
          .willReturn(created())
      }

      sender.createHook(projectId, oauthAccessToken, serializedHookToken).unsafeRunSync() shouldBe ((): Unit)
    }

    "return an error if config cannot be read" in new TestCase {
      val exception = exceptions.generateOne
      expectConfigProvider(returning = IO.raiseError(exception))
      val accessToken = accessTokens.generateOne

      intercept[Exception] {
        sender.createHook(projectId, accessToken, serializedHookToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectConfigProvider(returning = IO.pure(hookCreationConfig))
      val accessToken = accessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withRequestBody(equalToJson(expectedBody))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        sender.createHook(projectId, accessToken, serializedHookToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither CREATED nor UNAUTHORIZED" in new TestCase {
      expectConfigProvider(returning = IO.pure(hookCreationConfig))
      val accessToken = accessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withRequestBody(equalToJson(expectedBody))
          .willReturn(badRequest().withBody("some message"))
      }

      intercept[Exception] {
        sender.createHook(projectId, accessToken, serializedHookToken).unsafeRunSync()
      }.getMessage shouldBe s"POST $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.BadRequest}; body: some message"
    }
  }

  private trait TestCase {
    val projectId           = projectIds.generateOne
    val serializedHookToken = serializedHookTokens.generateOne
    val gitLabUrl           = url(externalServiceBaseUrl)
    val selfUrl             = validatedUrls.generateOne
    val hookCreationConfig  = HookCreationConfig(gitLabUrl, selfUrl)

    lazy val expectedBody = Json
      .obj(
        "id"          -> Json.fromInt(projectId.value),
        "url"         -> Json.fromString(s"$selfUrl${WebhookEventEndpoint.processPushEvent().url}"),
        "push_events" -> Json.fromBoolean(true),
        "token"       -> Json.fromString(serializedHookToken.value)
      )
      .toString()

    val configProvider = mock[IOHookCreationConfigProvider]

    def expectConfigProvider(returning: IO[HookCreationConfig]) =
      (configProvider
        .get()(_: Sync[IO]))
        .expects(*)
        .returning(returning)

    val sender = new IOHookCreationRequestSender(configProvider)

    private def url(value: String) =
      RefType
        .applyRef[String Refined Url](value)
        .getOrElse(throw new IllegalArgumentException("Invalid url value"))
  }
}
