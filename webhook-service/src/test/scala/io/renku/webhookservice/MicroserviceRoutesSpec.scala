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

package io.renku.webhookservice

import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.{authUsers, httpStatuses}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import io.renku.webhookservice.hookcreation.HookCreationEndpoint
import io.renku.webhookservice.hookdeletion.HookDeletionEndpoint
import io.renku.webhookservice.hookvalidation.HookValidationEndpoint
import org.http4s._
import org.http4s.Method.GET
import org.http4s.Status._
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "GET /ping" should {

    "return OK with 'pong' body" in new TestCase {

      val response = routes.call(
        Request(Method.GET, uri"/ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "GET /metrics" should {

    "return OK with prometheus metrics" in new TestCase {

      val response = routes.call(
        Request(Method.GET, uri"/metrics")
      )

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "POST webhooks/events" should {

    "pass the request to the endpoint and return the received response" in new TestCase {

      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      val request        = Request[IO](Method.POST, uri"/webhooks/events")
      (webhookEventsEndpoint
        .processPushEvent(_: Request[IO]))
        .expects(request)
        .returning(IO.pure(Response[IO](responseStatus)))

      routes.call(request).status shouldBe responseStatus
    }
  }

  "DELETE webhooks/events" should {

    "return response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      val request        = Request[IO](Method.DELETE, uri"/projects" / projectId / "webhooks")
      (hookDeletionEndpoint
        .deleteHook(_: projects.GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.pure(Response[IO](responseStatus)))

      routes.call(request).status shouldBe responseStatus
    }
  }

  "GET projects/:id/events/status" should {

    "return Ok response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.GET, uri"/projects" / projectId / "events" / "status")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (eventStatusEndpoint
        .fetchProcessingStatus(_: projects.GitLabId))
        .expects(projectId)
        .returning(IO.pure(Response[IO](responseStatus)))

      routes.call(request).status shouldBe responseStatus
    }

    "return NotFound when no :id path parameter given" in new TestCase {
      routes.call(Request(Method.GET, uri"/projects/")).status shouldBe NotFound
    }

    "return Unauthorized when user is not authorized" in new TestCase {

      override val authenticationResponse = OptionT.none[IO, AuthUser]

      val request = Request[IO](Method.GET, uri"/projects" / projectIds.generateOne / "events" / "status")

      routes.call(request).status shouldBe Unauthorized
    }

    "return response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.POST, uri"/projects" / projectId / "webhooks")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (hookCreationEndpoint
        .createHook(_: projects.GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.pure(Response[IO](responseStatus)))

      routes.call(request).status shouldBe responseStatus
    }
  }

  "POST projects/:id/webhooks" should {

    "return Unauthorized when the user is not authorized" in new TestCase {

      override val authenticationResponse = OptionT.none[IO, AuthUser]

      val projectId = projectIds.generateOne
      val request   = Request[IO](Method.POST, uri"/projects" / projectId / "webhooks")

      routes.call(request).status shouldBe Unauthorized
    }
  }

  "POST projects/:id/webhooks/validation" should {

    "return response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.POST, uri"/projects" / projectId / "webhooks" / "validation")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (hookValidationEndpoint
        .validateHook(_: projects.GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.pure(Response[IO](responseStatus)))

      routes.call(request).status shouldBe responseStatus
    }

    "return Unauthorized when user is not authorized" in new TestCase {

      override val authenticationResponse = OptionT.none[IO, AuthUser]

      val projectId = projectIds.generateOne
      val request   = Request[IO](Method.POST, uri"/projects" / projectId / "webhooks" / "validation")

      routes.call(request).status shouldBe Unauthorized
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      (routes call Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {

    val authUser               = authUsers.generateOne
    val authenticationResponse = OptionT.some[IO](authUser)
    val webhookEventsEndpoint  = mock[webhookevents.Endpoint[IO]]
    val hookCreationEndpoint   = mock[HookCreationEndpoint[IO]]
    val hookDeletionEndpoint   = mock[HookDeletionEndpoint[IO]]
    val hookValidationEndpoint = mock[HookValidationEndpoint[IO]]
    val eventStatusEndpoint    = mock[eventstatus.Endpoint[IO]]
    private val routesMetrics  = TestRoutesMetrics()
    private val versionRoutes  = mock[version.Routes[IO]]
    lazy val routes = new MicroserviceRoutes[IO](
      webhookEventsEndpoint,
      hookCreationEndpoint,
      hookValidationEndpoint,
      hookDeletionEndpoint,
      eventStatusEndpoint,
      givenAuthMiddleware(returning = authenticationResponse),
      routesMetrics,
      versionRoutes
    ).routes.map(_.or(notAvailableResponse))

    val versionEndpointResponse = Response[IO](httpStatuses.generateOne)
    (versionRoutes.apply _)
      .expects()
      .returning {
        import org.http4s.dsl.io.{GET => _, _}
        HttpRoutes.of[IO] { case GET -> Root / "version" => versionEndpointResponse.pure[IO] }
      }
  }
}
