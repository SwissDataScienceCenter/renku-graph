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

package io.renku.http.server

import cats.effect._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpPorts
import io.renku.http.server.EndpointTester._
import io.renku.testtools.IOSpec
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HttpServerSpec extends AnyWordSpec with IOSpec with Http4sDsl[IO] with should.Matchers with BeforeAndAfter {

  "run" should {

    "create an http server to serve the given routes" in new TestCase {

      client
        .run(Request[IO](Method.GET, baseUri / "resource"))
        .use { response =>
          IO {
            response.status                     shouldBe Status.Ok
            response.as[String].unsafeRunSync() shouldBe "response"
          }
        }
        .unsafeRunSync()

    }

    "create an http server which responds with NOT_FOUND and JSON body for non-existing resource" in new TestCase {

      client
        .run(Request[IO](Method.GET, baseUri / "non-existing"))
        .use { response =>
          IO {
            response.status                   shouldBe Status.NotFound
            response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
            response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Resource not found"}"""
          }
        }
        .unsafeRunSync()

    }
  }

  private trait TestCase {
    private lazy val port = httpPorts.generateOne
    lazy val baseUri: Uri = Uri.unsafeFromString(s"http://localhost:$port")
    private lazy val routes = HttpRoutes.of[IO] { case GET -> Root / "resource" => Ok("response") }
    val client: Client[IO] = Client.fromHttpApp(HttpServer[IO](port.value, routes).httpApp)
  }
}
