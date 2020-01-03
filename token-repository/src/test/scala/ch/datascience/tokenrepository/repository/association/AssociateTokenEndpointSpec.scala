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

package ch.datascience.tokenrepository.repository.association

import cats.MonadError
import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Json
import io.circe.literal._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class AssociateTokenEndpointSpec extends WordSpec with MockFactory {

  "associateToken" should {

    "respond with NO_CONTENT if the Personal Access Token association was successful" in new TestCase {

      val accessToken = personalAccessTokens.generateOne
      (tokensAssociator
        .associate(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(()))

      val request = Request(Method.PUT, uri"projects" / projectId.toString / "tokens")
        .withEntity(json"""{"personalAccessToken": ${accessToken.value}}""")

      val response = associateToken(projectId, request).unsafeRunSync()

      response.status                              shouldBe Status.NoContent
      response.body.compile.toVector.unsafeRunSync shouldBe empty

      logger.expectNoLogs()
    }

    "respond with NO_CONTENT if the OAuth Access Token association was successful" in new TestCase {

      val accessToken = oauthAccessTokens.generateOne

      (tokensAssociator
        .associate(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(()))

      val request = Request(Method.PUT, uri"projects" / projectId.toString / "tokens")
        .withEntity(json"""{"oauthAccessToken": ${accessToken.value}}""")

      val response = associateToken(projectId, request).unsafeRunSync()

      response.status                              shouldBe Status.NoContent
      response.body.compile.toVector.unsafeRunSync shouldBe empty

      logger.expectNoLogs()
    }

    "respond with BAD_REQUEST if the request body is invalid" in new TestCase {

      val request = Request[IO](Method.PUT, uri"projects" / projectId.toString / "tokens")

      val response = associateToken(projectId, request).unsafeRunSync()

      response.status                 shouldBe Status.BadRequest
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe json"""{"message": "Malformed message body: Invalid JSON: empty body"}"""

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if Access Token association fails" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      val exception = exceptions.generateOne
      (tokensAssociator
        .associate(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.raiseError(exception))

      val request = Request(Method.PUT, uri"projects" / projectId.toString / "tokens")
        .withEntity(json"""{"personalAccessToken": ${accessToken.value}}""")

      val response = associateToken(projectId, request).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      val expectedMessage = s"Associating token with projectId: $projectId failed"
      response.as[Json].unsafeRunSync shouldBe json"""{"message": $expectedMessage}"""

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {

    val context = MonadError[IO, Throwable]

    val projectId = projectIds.generateOne

    val tokensAssociator = mock[IOTokenAssociator]
    val logger           = TestLogger[IO]()
    val associateToken   = new AssociateTokenEndpoint[IO](tokensAssociator, logger).associateToken _
  }
}
