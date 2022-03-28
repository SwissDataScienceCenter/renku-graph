/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.httpStatuses
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.server.version
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.association.AssociateTokenEndpoint
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

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(Request(GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    s"define a GET /projects/:id/tokens endpoint returning $Ok when a valid projectId is given" in new TestCase {

      val projectId = projectIds.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.Id)(_: projects.Id => OptionT[IO, AccessToken]))
        .expects(projectId, *)
        .returning(IO.pure(Response[IO](Ok)))

      routes.call(Request(GET, uri"/projects" / projectId.toString / "tokens")).status shouldBe Ok
    }

    s"define a GET /projects/:id/tokens endpoint returning $Ok when a valid projectPath is given" in new TestCase {

      val projectPath = projectPaths.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.Path)(_: projects.Path => OptionT[IO, AccessToken]))
        .expects(projectPath, *)
        .returning(IO.pure(Response[IO](Ok)))

      routes.call(Request(GET, uri"/projects" / projectPath.toString / "tokens")).status shouldBe Ok
    }

    s"define a PUT /projects/:id/tokens endpoint returning $Ok when a valid projectId given" in new TestCase {

      val projectId = projectIds.generateOne
      val request   = Request[IO](PUT, uri"/projects" / projectId.toString / "tokens")

      (associateEndpoint
        .associateToken(_: projects.Id, _: Request[IO]))
        .expects(projectId, request)
        .returning(IO.pure(Response[IO](NoContent)))

      (routes call request).status shouldBe NoContent
    }

    s"define a DELETE /projects/:id/tokens endpoint returning $Ok when a valid projectId given" in new TestCase {

      val projectId = projectIds.generateOne
      (deleteEndpoint
        .deleteToken(_: projects.Id))
        .expects(projectId)
        .returning(IO.pure(Response[IO](NoContent)))

      (routes call Request[IO](DELETE, uri"/projects" / projectId.toString / "tokens")).status shouldBe NoContent
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      (routes call Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {

    val fetchEndpoint     = mock[fetching.FetchTokenEndpoint[IO]]
    val associateEndpoint = mock[AssociateTokenEndpoint[IO]]
    val deleteEndpoint    = mock[deletion.DeleteTokenEndpoint[IO]]
    val routesMetrics     = TestRoutesMetrics()
    val versionRoutes     = mock[version.Routes[IO]]
    val routes = new MicroserviceRoutes[IO](
      fetchEndpoint,
      associateEndpoint,
      deleteEndpoint,
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
