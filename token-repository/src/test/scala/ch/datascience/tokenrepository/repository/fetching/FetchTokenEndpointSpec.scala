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

package ch.datascience.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Json
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import ch.datascience.http.client.UrlEncoder.urlEncode

class FetchTokenEndpointSpec extends WordSpec with MockFactory {

  "fetchToken" should {

    "respond with OK with the oauth token if one is found in the repository" in new TestCase {
      import endpoint._

      val accessToken = oauthAccessTokens.generateOne
      val projectId   = projectIds.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val request = Request[IO](Method.GET, uri"projects" / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                 shouldBe Status.Ok
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj("oauthAccessToken" -> Json.fromString(accessToken.value))

      logger.expectNoLogs()
    }

    "respond with OK with the personal access token if one is found in the repository" in new TestCase {
      import endpoint._

      val accessToken = personalAccessTokens.generateOne
      val projectId   = projectIds.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val request = Request[IO](Method.GET, uri"projects" / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                 shouldBe Status.Ok
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj("personalAccessToken" -> Json.fromString(accessToken.value))

      logger.expectNoLogs()
    }

    "respond with OK with the token if one is found in the repository for the given project path" in new TestCase {
      import endpoint._

      val accessToken = oauthAccessTokens.generateOne
      val projectPath = projectPaths.generateOne

      (tokensFinder
        .findToken(_: ProjectPath))
        .expects(projectPath)
        .returning(OptionT.some[IO](accessToken))

      val request = Request[IO](Method.GET, uri"projects" / urlEncode(projectPath.toString) / "tokens")

      val response = fetchToken(projectPath).unsafeRunSync()

      response.status                 shouldBe Status.Ok
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj("oauthAccessToken" -> Json.fromString(accessToken.value))

      logger.expectNoLogs()
    }

    "respond with NOT_FOUND if there is no token in the repository" in new TestCase {
      import endpoint._

      val accessToken = personalAccessTokens.generateOne
      val projectId   = projectIds.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.none[IO, AccessToken])

      val request = Request[IO](Method.GET, uri"projects" / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj(
        "message" -> Json.fromString(s"Token for project: $projectId not found")
      )

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if finding token in the repository fails" in new TestCase {
      import endpoint._

      val accessToken = personalAccessTokens.generateOne
      val projectId   = projectIds.generateOne

      val exception = exceptions.generateOne
      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT(IO.raiseError[Option[AccessToken]](exception)))

      val request = Request[IO](Method.GET, uri"projects" / projectId.toString / "tokens")

      val response = fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe Json.obj(
        "message" -> Json.fromString(s"Finding token for project: $projectId failed")
      )

      logger.loggedOnly(Error(s"Finding token for project: $projectId failed", exception))
    }
  }

  private trait TestCase {
    val tokensFinder = mock[IOTokenFinder]
    val logger       = TestLogger[IO]()
    val endpoint     = new FetchTokenEndpoint[IO](tokensFinder, logger)
  }
}
