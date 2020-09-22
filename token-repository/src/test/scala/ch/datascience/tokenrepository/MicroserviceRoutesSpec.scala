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

package ch.datascience.tokenrepository

import cats.data.OptionT
import cats.effect.{Clock, IO}

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestRoutesMetrics
import ch.datascience.tokenrepository.repository.association.IOAssociateTokenEndpoint
import ch.datascience.tokenrepository.repository.deletion.IODeleteTokenEndpoint
import ch.datascience.tokenrepository.repository.fetching.IOFetchTokenEndpoint
import org.http4s.Status._
import org.http4s._
import org.http4s.implicits._
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

    s"define a GET /projects/:id/tokens endpoint returning $Ok when a valid projectId is given" in new TestCase {
      import fetchEndpoint._

      val projectId = projectIds.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.Id)(_: projects.Id => OptionT[IO, AccessToken]))
        .expects(projectId, findById)
        .returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"projects" / projectId.toString / "tokens")
      )

      response.status shouldBe Ok
    }

    s"define a GET /projects/:id/tokens endpoint returning $Ok when a valid projectPath is given" in new TestCase {
      import fetchEndpoint._

      val projectPath = projectPaths.generateOne

      (fetchEndpoint
        .fetchToken(_: projects.Path)(_: projects.Path => OptionT[IO, AccessToken]))
        .expects(projectPath, findByPath)
        .returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"projects" / projectPath.toString / "tokens")
      )

      response.status shouldBe Ok
    }

    s"define a PUT /projects/:id/tokens endpoint returning $Ok when a valid projectId given" in new TestCase {

      val projectId = projectIds.generateOne
      val request   = Request[IO](Method.PUT, uri"projects" / projectId.toString / "tokens")

      (associateEndpoint
        .associateToken(_: projects.Id, _: Request[IO]))
        .expects(projectId, request)
        .returning(IO.pure(Response[IO](NoContent)))

      val response = routes call request

      response.status shouldBe NoContent
    }

    s"define a DELETE /projects/:id/tokens endpoint returning $Ok when a valid projectId given" in new TestCase {

      val projectId = projectIds.generateOne
      (deleteEndpoint
        .deleteToken(_: projects.Id))
        .expects(projectId)
        .returning(IO.pure(Response[IO](NoContent)))

      val response = routes call Request[IO](Method.DELETE, uri"projects" / projectId.toString / "tokens")

      response.status shouldBe NoContent
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val fetchEndpoint     = mock[IOFetchTokenEndpoint]
    val associateEndpoint = mock[IOAssociateTokenEndpoint]
    val deleteEndpoint    = mock[IODeleteTokenEndpoint]
    val routesMetrics     = TestRoutesMetrics()
    val routes = new MicroserviceRoutes[IO](
      fetchEndpoint,
      associateEndpoint,
      deleteEndpoint,
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }
}
