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

package ch.datascience.webhookservice.tokenrepository

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.tokenrepository.TokenRepositoryUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOAccessTokenRemoverSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "removeAccessToken" should {

    "succeed if removing token for the given projectId on a remote is successful" in new TestCase {

      stubFor {
        delete(s"/projects/$projectId/tokens")
          .willReturn(noContent())
      }

      tokenRemover.removeAccessToken(projectId).unsafeRunSync() shouldBe ((): Unit)
    }

    "return an Exception if remote client responds with a status other than NO_CONTENT" in new TestCase {
      val accessToken = accessTokens.generateOne

      val responseBody = ErrorMessage("some error").asJson.noSpaces
      stubFor {
        delete(s"/projects/$projectId/tokens")
          .willReturn(status(Status.BadGateway.code).withBody(responseBody))
      }

      intercept[Exception] {
        tokenRemover.removeAccessToken(projectId).unsafeRunSync()
      }.getMessage shouldBe s"DELETE $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.BadGateway}; body: $responseBody"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val context = MonadError[IO, Throwable]

    val tokenRepositoryUrl = TokenRepositoryUrl(externalServiceBaseUrl)
    val projectId          = projectIds.generateOne

    val tokenRemover = new IOAccessTokenRemover(tokenRepositoryUrl, TestLogger())
  }
}
