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
import ch.datascience.clients.AccessToken
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectId
import ch.datascience.http.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.Json
import org.http4s.dsl.io._
import org.http4s.{Method, Request, Status, Uri}
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

      val response = endpoint.call(
        Request(Method.GET, Uri.uri("projects") / projectId.toString / "tokens")
      )

      response.status     shouldBe Status.Ok
      response.body[Json] shouldBe Json.obj("oauthAccessToken" -> Json.fromString(accessToken.value))

      logger.loggedOnly(Info(s"Token for projectId: $projectId found"))
    }

    "respond with OK with the personal access token if one is found in the repository" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some[IO](accessToken))

      val response = endpoint.call(
        Request(Method.GET, Uri.uri("projects") / projectId.toString / "tokens")
      )

      response.status     shouldBe Status.Ok
      response.body[Json] shouldBe Json.obj("personalAccessToken" -> Json.fromString(accessToken.value))

      logger.loggedOnly(Info(s"Token for projectId: $projectId found"))
    }

    "respond with NOT_FOUND if there is not token in the repository" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.none[IO, AccessToken])

      val response = endpoint.call(
        Request(Method.GET, Uri.uri("projects") / projectId.toString / "tokens")
      )

      response.status     shouldBe Status.NotFound
      response.body[Json] shouldBe Json.obj("message" -> Json.fromString(s"Token for projectId: $projectId not found"))

      logger.loggedOnly(Info(s"Token for projectId: $projectId not found"))
    }

    "respond with INTERNAL_SERVER_ERROR if finding token in the repository fails" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      val exception = exceptions.generateOne
      (tokensFinder
        .findToken(_: ProjectId))
        .expects(projectId)
        .returning(OptionT(IO.raiseError[Option[AccessToken]](exception)))

      val response = endpoint.call(
        Request(Method.GET, Uri.uri("projects") / projectId.toString / "tokens")
      )

      response.status shouldBe Status.InternalServerError
      response.body[Json] shouldBe Json.obj(
        "message" -> Json.fromString(s"Finding token for projectId: $projectId failed")
      )

      logger.loggedOnly(Error(s"Finding token for projectId: $projectId failed", exception))
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val tokensFinder = mock[IOTokenFinder]
    val logger       = TestLogger[IO]()
    val endpoint     = new FetchTokenEndpoint[IO](tokensFinder, logger).fetchToken.orNotFound
  }
}
