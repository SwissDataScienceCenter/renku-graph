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

package ch.datascience.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Json
import org.http4s.headers.`Content-Type`
import org.http4s.{MediaType, Method, Request, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class FetchTokenEndpointSpec extends WordSpec with MockFactory {

  "fetchToken" should {

    "respond with OK with the oauth token if one is found in the repository" in new TestCase {

      val accessToken = oauthAccessTokens.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val request = Request[IO](Method.GET, Uri.uri("projects") / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                 shouldBe Status.Ok
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj("oauthAccessToken" -> Json.fromString(accessToken.value))

      logger.expectNoLogs()
    }

    "respond with OK with the personal access token if one is found in the repository" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val request = Request[IO](Method.GET, Uri.uri("projects") / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                 shouldBe Status.Ok
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj("personalAccessToken" -> Json.fromString(accessToken.value))

      logger.expectNoLogs()
    }

    "respond with NOT_FOUND if there is not token in the repository" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.none[IO, AccessToken])

      val request = Request[IO](Method.GET, Uri.uri("projects") / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj(
        "message" -> Json.fromString(s"Token for projectId: $projectId not found"))

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if finding token in the repository fails" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      val exception = exceptions.generateOne
      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT(IO.raiseError[Option[AccessToken]](exception)))

      val request = Request[IO](Method.GET, Uri.uri("projects") / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj(
        "message" -> Json.fromString(s"Finding token for projectId: $projectId failed")
      )

      logger.loggedOnly(Error(s"Finding token for projectId: $projectId failed", exception))
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val tokensFinder = mock[IOTokenFinder]
    val logger       = TestLogger[IO]()
    val fetchToken   = new FetchTokenEndpoint[IO](tokensFinder, logger).fetchToken _
  }
}
