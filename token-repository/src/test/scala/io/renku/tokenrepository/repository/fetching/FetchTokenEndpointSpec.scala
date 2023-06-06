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

package io.renku.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken._
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class FetchTokenEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "fetchToken" should {

    "respond with OK with the oauth token if one is found in the repository" in new TestCase {
      import endpoint._

      val accessToken: AccessToken = userOAuthAccessTokens.generateOne
      val projectId = projectIds.generateOne

      (tokensFinder
        .findToken(_: GitLabId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                   shouldBe Status.Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe accessToken.asJson

      logger.expectNoLogs()
    }

    "respond with OK with the personal access token if one is found in the repository" in new TestCase {
      import endpoint._

      val accessToken: AccessToken = personalAccessTokens.generateOne
      val projectId = projectIds.generateOne

      (tokensFinder
        .findToken(_: GitLabId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val response = fetchToken(projectId).unsafeRunSync()

      response.status                   shouldBe Status.Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe accessToken.asJson

      logger.expectNoLogs()
    }

    "respond with OK with the token if one is found in the repository for the given project path" in new TestCase {
      import endpoint._

      val accessToken: AccessToken = userOAuthAccessTokens.generateOne
      val projectPath = projectPaths.generateOne

      (tokensFinder
        .findToken(_: projects.Path))
        .expects(projectPath)
        .returning(OptionT.some[IO](accessToken))

      val response = fetchToken(projectPath).unsafeRunSync()

      response.status                   shouldBe Status.Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe accessToken.asJson

      logger.expectNoLogs()
    }

    "respond with NOT_FOUND if there is no token in the repository" in new TestCase {
      import endpoint._

      val projectId = projectIds.generateOne

      (tokensFinder
        .findToken(_: GitLabId))
        .expects(projectId)
        .returning(OptionT.none[IO, AccessToken])

      val response = fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe Json.obj(
        "message" -> Json.fromString(s"Token for project: $projectId not found")
      )

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if finding token in the repository fails" in new TestCase {
      import endpoint._

      val projectId = projectIds.generateOne

      val exception = exceptions.generateOne
      (tokensFinder
        .findToken(_: GitLabId))
        .expects(projectId)
        .returning(OptionT(IO.raiseError[Option[AccessToken]](exception)))

      val response = endpoint.fetchToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe Json.obj(
        "message" -> Json.fromString(s"Finding token for project: $projectId failed")
      )

      logger.loggedOnly(Error(s"Finding token for project: $projectId failed", exception))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tokensFinder = mock[TokenFinder[IO]]
    val endpoint     = new FetchTokenEndpointImpl[IO](tokensFinder)
  }
}
