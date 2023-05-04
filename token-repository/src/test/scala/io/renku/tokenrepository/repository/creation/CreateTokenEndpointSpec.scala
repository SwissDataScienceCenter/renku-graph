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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s.Method.POST
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CreateTokenEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "createToken" should {

    "respond with NO_CONTENT if the Personal Access Token association was successful" in new TestCase {

      val accessToken = personalAccessTokens.generateOne
      givenTokenCreation(projectId, accessToken, returning = ().pure[IO])

      val request = Request(POST, uri"projects" / projectId / "tokens")
        .withEntity(accessToken.asJson)

      val response = endpoint.createToken(projectId, request).unsafeRunSync()

      response.status                                shouldBe Status.NoContent
      response.body.compile.toVector.unsafeRunSync() shouldBe empty

      logger.expectNoLogs()
    }

    "respond with NO_CONTENT if the OAuth Access Token association was successful" in new TestCase {

      val accessToken = userOAuthAccessTokens.generateOne
      givenTokenCreation(projectId, accessToken, returning = ().pure[IO])

      val request = Request(POST, uri"projects" / projectId / "tokens")
        .withEntity(accessToken.asJson)

      val response = endpoint.createToken(projectId, request).unsafeRunSync()

      response.status                                shouldBe Status.NoContent
      response.body.compile.toVector.unsafeRunSync() shouldBe empty

      logger.expectNoLogs()
    }

    "respond with BAD_REQUEST if the request body is invalid" in new TestCase {

      val request = Request[IO](POST, uri"projects" / projectId / "tokens")

      val response = endpoint.createToken(projectId, request).unsafeRunSync()

      response.status      shouldBe Status.BadRequest
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response
        .as[Json]
        .unsafeRunSync() shouldBe json"""{"message": "Malformed message body: Invalid JSON: empty body"}"""

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if Access Token association fails" in new TestCase {

      val accessToken = personalAccessTokens.generateOne
      val exception   = exceptions.generateOne
      givenTokenCreation(projectId, accessToken, returning = exception.raiseError[IO, Unit])

      val request = Request(POST, uri"projects" / projectId / "tokens")
        .withEntity(accessToken.asJson)

      val response = endpoint.createToken(projectId, request).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      val expectedMessage = s"Associating token with projectId: $projectId failed"
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": $expectedMessage}"""

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne

    private val tokensCreator = mock[TokensCreator[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val endpoint = new CreateTokenEndpointImpl[IO](tokensCreator)

    def givenTokenCreation(projectId: projects.GitLabId, accessToken: AccessToken, returning: IO[Unit]) =
      (tokensCreator
        .create(_: GitLabId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(returning)
  }
}
