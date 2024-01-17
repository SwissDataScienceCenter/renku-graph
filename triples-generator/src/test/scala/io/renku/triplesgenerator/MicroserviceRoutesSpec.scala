/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.http.client.HttpClientGenerators.httpStatuses
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.http.RenkuEntityCodec
import io.renku.http.server.EndpointTester._
import io.renku.http.server.version
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.EventEndpoint
import org.http4s.MediaType.application
import org.http4s.Method.{GET, POST}
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with RenkuEntityCodec {

  "POST /events" should {

    "return Accepted for success" in new TestCase {

      val request        = Request[IO](POST, uri"/events")
      val expectedStatus = Accepted
      (eventEndpoint.processEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      routes.call(request).status shouldBe expectedStatus
    }
  }

  "GET /ping" should {

    "return Ok with 'pong' response" in new TestCase {

      val response = routes.call(Request(Method.GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "POST /projects" should {

    "return Ok with message in response" in new TestCase {

      val request      = Request[IO](Method.POST, uri"/projects")
      val responseInfo = Message.Info("Project created")

      givenProjectCreateEndpoint(request, returning = Response[IO](Ok).withEntity(responseInfo))

      val response = routes.call(request)

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe responseInfo
    }
  }

  "PATCH /projects/:slug" should {

    "return Ok with message in response" in new TestCase {

      val slug         = projectSlugs.generateOne
      val request      = Request[IO](Method.PATCH, uri"/projects" / slug)
      val responseInfo = Message.Info("Project updated")

      givenProjectUpdateEndpoint(slug, request, returning = Response[IO](Ok).withEntity(responseInfo))

      val response = routes.call(request)

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe responseInfo
    }

    "return the default response for illegal slug" in new TestCase {
      routes.call(Request[IO](Method.PATCH, uri"/projects" / "illegal-slug")).status shouldBe ServiceUnavailable
    }
  }

  "GET /metrics" should {

    "return Ok with some prometheus metrics" in new TestCase {

      val response = routes.call(Request(Method.GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      (routes call Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {
    val eventEndpoint                 = mock[EventEndpoint[IO]]
    private val projectCreateEndpoint = mock[projects.create.Endpoint[IO]]
    private val projectUpdateEndpoint = mock[projects.update.Endpoint[IO]]
    private val routesMetrics         = TestRoutesMetrics()
    private val versionRoutes         = mock[version.Routes[IO]]
    val routes = new MicroserviceRoutes[IO](eventEndpoint,
                                            projectCreateEndpoint,
                                            projectUpdateEndpoint,
                                            routesMetrics,
                                            versionRoutes,
                                            ConfigFactory.empty()
    ).routes
      .map(_.or(notAvailableResponse))

    val versionEndpointResponse = Response[IO](httpStatuses.generateOne)
    (versionRoutes.apply _)
      .expects()
      .returning {
        import org.http4s.dsl.io.{GET => _, _}
        HttpRoutes.of[IO] { case GET -> Root / "version" => versionEndpointResponse.pure[IO] }
      }

    def givenProjectUpdateEndpoint(slug: model.projects.Slug, request: Request[IO], returning: Response[IO]) =
      (projectUpdateEndpoint.`PATCH /projects/:slug` _)
        .expects(slug, request)
        .returning(returning.pure[IO])

    def givenProjectCreateEndpoint(request: Request[IO], returning: Response[IO]) =
      (projectCreateEndpoint.`POST /projects` _)
        .expects(request)
        .returning(returning.pure[IO])
  }
}
