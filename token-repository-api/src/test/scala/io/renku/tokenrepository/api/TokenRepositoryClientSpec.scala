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

package io.renku.tokenrepository.api

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectIds, projectSlugs}
import io.renku.http.client.UrlEncoder
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.Uri
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class TokenRepositoryClientSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with ExternalServiceStubbing {

  private implicit val logger: Logger[IO] = TestLogger()
  private val client = new TokenRepositoryClientImpl[IO](Uri.unsafeFromString(externalServiceBaseUrl))

  "findAccessToken(projectId)" should {

    "succeed if fetching project access token returns OK with the payload" in {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/$projectId/tokens"))
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      client.findAccessToken(projectId).asserting(_ shouldBe accessToken.some)
    }

    "succeed and return None if fetching project access token returns NOT_FOUND" in {

      val projectId = projectIds.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/$projectId/tokens"))
          .willReturn(notFound())
      }

      client.findAccessToken(projectId).asserting(_ shouldBe None)
    }

    "failed if fetching the payload returns other status" in {

      val projectId = projectIds.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/$projectId/tokens"))
          .willReturn(badRequest())
      }

      client.findAccessToken(projectId).assertThrows[Exception]
    }
  }

  "findAccessToken(projectSlug)" should {

    "succeed if fetching project access token returns OK with the payload" in {

      val projectSlug = projectSlugs.generateOne
      val accessToken = accessTokens.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/${UrlEncoder.urlEncode(projectSlug.value)}/tokens"))
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      client.findAccessToken(projectSlug).asserting(_ shouldBe accessToken.some)
    }

    "succeed and return None if fetching project access token returns NOT_FOUND" in {

      val projectSlug = projectSlugs.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/${UrlEncoder.urlEncode(projectSlug.value)}/tokens"))
          .willReturn(notFound())
      }

      client.findAccessToken(projectSlug).asserting(_ shouldBe None)
    }

    "failed if fetching the payload returns other status" in {

      val projectSlug = projectSlugs.generateOne

      stubFor {
        get(urlEqualTo(s"/projects/${UrlEncoder.urlEncode(projectSlug.value)}/tokens"))
          .willReturn(badRequest())
      }

      client.findAccessToken(projectSlug).assertThrows[Exception]
    }
  }
}
