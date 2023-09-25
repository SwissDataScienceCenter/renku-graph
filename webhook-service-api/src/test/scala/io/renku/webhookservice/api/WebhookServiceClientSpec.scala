/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.api

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.{errorMessages, userAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.webhookservice.api.WebhookServiceClient.Result
import org.http4s.Status.{Created, InternalServerError, NotFound, Ok}
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class WebhookServiceClientSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with TableDrivenPropertyChecks
    with ExternalServiceStubbing {

  "createHook" should {

    forAll {
      Table(
        "status code" -> "result",
        Ok            -> HookCreationResult.Existed,
        Created       -> HookCreationResult.Created,
        NotFound      -> HookCreationResult.NotFound
      )
    } { (status, result) =>
      s"return $result if remote responds with $status" in {

        val projectId   = projectIds.generateOne
        val accessToken = userAccessTokens.generateOne

        stubFor {
          post(s"/projects/$projectId/webhooks")
            .willReturn(aResponse.withStatus(status.code))
        }

        client.createHook(projectId, accessToken).asserting(_ shouldBe Result.success(result))
      }
    }

    "return a failure in case of other responses" in {

      val projectId   = projectIds.generateOne
      val accessToken = userAccessTokens.generateOne
      val message     = errorMessages.generateOne

      stubFor {
        post(s"/projects/$projectId/webhooks")
          .willReturn(aResponse.withStatus(InternalServerError.code).withBody(message.asJson.spaces2))
      }

      client.createHook(projectId, accessToken).asserting(_ shouldBe Result.failure(message.show))
    }
  }

  private implicit val logger: Logger[IO] = TestLogger()
  private lazy val client = new WebhookServiceClientImpl[IO](externalServiceBaseUri)
}
