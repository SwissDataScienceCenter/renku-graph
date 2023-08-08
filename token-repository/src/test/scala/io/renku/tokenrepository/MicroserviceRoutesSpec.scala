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

package io.renku.tokenrepository

import cats.data.OptionT
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.{httpStatuses, userAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security._
import io.renku.http.server.version
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.creation.CreateTokenEndpoint
import io.renku.tokenrepository.repository.{deletion, fetching}
import org.http4s.Method._
import org.http4s.Status._
import org.http4s._
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "GET /ping" should {

    "return OK with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "GET /metrics" should {

    "return OK with some prometheus metrics" in new TestCase {
      val response = routes.call(Request(GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /projects/:id/tokens" should {

    s"return $Ok when a valid projectId is given" in new TestCase {

      givenDBReady()

      val projectId = projectIds.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.GitLabId)(_: projects.GitLabId => OptionT[IO, AccessToken]))
        .expects(projectId, *)
        .returning(IO.pure(Response[IO](Ok)))

      routes.call(Request(GET, uri"/projects" / projectId.toString / "tokens")).status shouldBe Ok
    }

    s"return $ServiceUnavailable for a valid projectId when DB migration is ongoing" in new TestCase {
      routes
        .call(Request(GET, uri"/projects" / projectIds.generateOne.toString / "tokens"))
        .status shouldBe ServiceUnavailable
    }

    s"return $Ok when a valid projectSlug is given" in new TestCase {

      givenDBReady()

      val projectSlug = projectSlugs.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.Slug)(_: projects.Slug => OptionT[IO, AccessToken]))
        .expects(projectSlug, *)
        .returning(IO.pure(Response[IO](Ok)))

      routes.call(Request(GET, uri"/projects" / projectSlug.toString / "tokens")).status shouldBe Ok
    }

    s"return $ServiceUnavailable for a valid Project Slug when DB migration is ongoing" in new TestCase {
      routes
        .call(Request(GET, uri"/projects" / projectSlugs.generateOne.toString / "tokens"))
        .status shouldBe ServiceUnavailable
    }
  }

  "POST /projects/:id/tokens" should {

    s"return $Ok when a valid projectId given" in new TestCase {

      givenDBReady()

      val projectId = projectIds.generateOne
      val request   = Request[IO](POST, uri"/projects" / projectId.toString / "tokens")

      (createEndpoint
        .createToken(_: projects.GitLabId, _: Request[IO]))
        .expects(projectId, request)
        .returning(IO.pure(Response[IO](NoContent)))

      (routes call request).status shouldBe NoContent
    }

    s"return $ServiceUnavailable for a valid projectId when DB migration is ongoing" in new TestCase {
      routes
        .call(Request(POST, uri"/projects" / projectIds.generateOne.toString / "tokens"))
        .status shouldBe ServiceUnavailable
    }
  }

  "DELETE /projects/:id/tokens" should {

    s"return $Ok when a valid projectId given" in new TestCase {

      givenDBReady()

      val projectId   = projectIds.generateOne
      val accessToken = userAccessTokens.generateOne

      (deleteEndpoint
        .deleteToken(_: projects.GitLabId, _: Option[AccessToken]))
        .expects(projectId, accessToken.some)
        .returning(IO.pure(Response[IO](NoContent)))

      (routes call Request[IO](DELETE, uri"/projects" / projectId / "tokens")
        .withHeaders(accessToken.toHeader)).status shouldBe NoContent
    }

    s"return $Ok for a valid projectId when no access token in the header" in new TestCase {

      givenDBReady()

      val projectId = projectIds.generateOne

      (deleteEndpoint
        .deleteToken(_: projects.GitLabId, _: Option[AccessToken]))
        .expects(projectId, Option.empty[UserAccessToken])
        .returning(IO.pure(Response[IO](NoContent)))

      (routes call Request[IO](DELETE, uri"/projects" / projectId / "tokens")).status shouldBe NoContent
    }

    s"return $ServiceUnavailable for a valid projectId when DB migration is ongoing" in new TestCase {
      routes
        .call(Request[IO](DELETE, uri"/projects" / projectIds.generateOne / "tokens"))
        .status shouldBe ServiceUnavailable
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      (routes call Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {

    val fetchEndpoint         = mock[fetching.FetchTokenEndpoint[IO]]
    val createEndpoint        = mock[CreateTokenEndpoint[IO]]
    val deleteEndpoint        = mock[deletion.DeleteTokenEndpoint[IO]]
    private val routesMetrics = TestRoutesMetrics()
    private val versionRoutes = mock[version.Routes[IO]]
    private val microserviceRoutes = new MicroserviceRoutesImpl[IO](
      fetchEndpoint,
      createEndpoint,
      deleteEndpoint,
      routesMetrics,
      versionRoutes,
      Ref.unsafe[IO, Boolean](false)
    )
    val routes = microserviceRoutes.routes.map(_.or(notAvailableResponse))

    val versionEndpointResponse = Response[IO](httpStatuses.generateOne)
    (versionRoutes.apply _)
      .expects()
      .returning {
        import org.http4s.dsl.io.{GET => _, _}
        HttpRoutes.of[IO] { case GET -> Root / "version" => versionEndpointResponse.pure[IO] }
      }

    def givenDBReady(): Unit =
      microserviceRoutes.notifyDBReady().unsafeRunSync()
  }
}
