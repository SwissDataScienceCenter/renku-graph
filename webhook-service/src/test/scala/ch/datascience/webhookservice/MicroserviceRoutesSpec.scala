/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice

import cats.data.OptionT
import cats.effect.{Clock, IO}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.interpreters.TestRoutesMetrics
import ch.datascience.webhookservice.eventprocessing.{HookEventEndpoint, ProcessingStatusEndpoint}
import ch.datascience.webhookservice.hookcreation.HookCreationEndpoint
import ch.datascience.webhookservice.hookvalidation.HookValidationEndpoint
import org.http4s.Status._
import org.http4s._
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {

      val response = routes.call(
        Request(Method.GET, uri"ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(Method.GET, uri"metrics")
      )

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    "define a POST webhooks/events endpoint returning response from the endpoint" in new TestCase {

      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      val request        = Request[IO](Method.POST, uri"webhooks/events")
      (hookEventEndpoint
        .processPushEvent(_: Request[IO]))
        .expects(request)
        .returning(IO.pure(Response[IO](responseStatus)))

      val response = routes.call(request)

      response.status shouldBe responseStatus
    }

    "define a GET projects/:id/events/status endpoint returning response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.GET, uri"projects" / projectId.toString / "events" / "status")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (processingStatusEndpoint
        .fetchProcessingStatus(_: projects.Id))
        .expects(projectId)
        .returning(IO.pure(Response[IO](responseStatus)))

      val response = routes.call(request)

      response.status shouldBe responseStatus
    }

    s"define a GET projects/:id/events/status endpoint returning $NotFound when no :id path parameter given" in new TestCase {
      routes.call(Request(Method.GET, uri"projects/")).status shouldBe NotFound
    }

    "define a POST projects/:id/webhooks endpoint returning response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.POST, uri"projects" / projectId.toString / "webhooks")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (hookCreationEndpoint
        .createHook(_: projects.Id, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.pure(Response[IO](responseStatus)))

      val response = routes.call(request)

      response.status shouldBe responseStatus
    }

    s"define a POST projects/:id/webhooks endpoint returning $Unauthorized when user is not authorized" in new TestCase {
      override val authenticationResponse = OptionT.none[IO, AuthUser]

      val projectId = projectIds.generateOne
      val request   = Request[IO](Method.POST, uri"projects" / projectId.toString / "webhooks")

      routes.call(request).status shouldBe Unauthorized
    }

    "define a POST projects/:id/webhooks/validation endpoint returning response from the endpoint" in new TestCase {

      val projectId      = projectIds.generateOne
      val request        = Request[IO](Method.POST, uri"projects" / projectId.toString / "webhooks" / "validation")
      val responseStatus = Gen.oneOf(Ok, BadRequest).generateOne
      (hookValidationEndpoint
        .validateHook(_: projects.Id, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.pure(Response[IO](responseStatus)))

      val response = routes.call(request)

      response.status shouldBe responseStatus
    }

    s"define a POST projects/:id/webhooks/validation endpoint returning $Unauthorized when user is not authorized" in new TestCase {
      override val authenticationResponse = OptionT.none[IO, AuthUser]

      val projectId = projectIds.generateOne
      val request   = Request[IO](Method.POST, uri"projects" / projectId.toString / "webhooks" / "validation")

      routes.call(request).status shouldBe Unauthorized
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val authUser                 = authUsers.generateOne
    val authenticationResponse   = OptionT.some[IO](authUser)
    val hookEventEndpoint        = mock[HookEventEndpoint[IO]]
    val hookCreationEndpoint     = mock[HookCreationEndpoint[IO]]
    val hookValidationEndpoint   = mock[HookValidationEndpoint[IO]]
    val processingStatusEndpoint = mock[ProcessingStatusEndpoint[IO]]
    val routesMetrics            = TestRoutesMetrics()
    lazy val routes = new MicroserviceRoutes[IO](
      hookEventEndpoint,
      hookCreationEndpoint,
      hookValidationEndpoint,
      processingStatusEndpoint,
      givenAuthMiddleware(returning = authenticationResponse),
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }
}
