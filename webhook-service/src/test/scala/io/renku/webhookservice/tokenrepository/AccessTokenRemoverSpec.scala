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

package io.renku.webhookservice.tokenrepository

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.tokenrepository.TokenRepositoryUrl
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class AccessTokenRemoverSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "removeAccessToken" should {

    "succeed if removing token for the given projectId on a remote is successful" in new TestCase {

      stubFor {
        delete(s"/projects/$projectId/tokens")
          .withAccessToken(maybeAccessToken)
          .willReturn(noContent())
      }

      tokenRemover.removeAccessToken(projectId, maybeAccessToken).unsafeRunSync() shouldBe ()
    }

    "return an Exception if remote client responds with a status other than NO_CONTENT" in new TestCase {

      val responseBody = ErrorMessage("some error").asJson.noSpaces
      stubFor {
        delete(s"/projects/$projectId/tokens")
          .withAccessToken(maybeAccessToken)
          .willReturn(status(Status.BadGateway.code).withBody(responseBody))
      }

      intercept[Exception] {
        tokenRemover.removeAccessToken(projectId, maybeAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"DELETE $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.BadGateway}; body: $responseBody"
    }
  }

  private trait TestCase {

    val projectId        = projectIds.generateOne
    val maybeAccessToken = accessTokens.generateOption

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tokenRepositoryUrl = TokenRepositoryUrl(externalServiceBaseUrl)
    val tokenRemover       = new AccessTokenRemoverImpl[IO](tokenRepositoryUrl)
  }
}
