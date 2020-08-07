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

package ch.datascience.http.server

import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpPorts
import ch.datascience.http.server.EndpointTester._
import io.circe.Json
import io.circe.literal._
import org.http4s._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class HttpServerSpec extends WordSpec with Http4sDsl[IO] {

  "run" should {

    "create an http server to serve the given routes" in {

      val response = execute(Request[IO](Method.GET, baseUri / "resource"))

      response.status                     shouldBe Status.Ok
      response.as[String].unsafeRunSync() shouldBe "response"
    }

    "create an http server which responds with NOT_FOUND and JSON body for non-existing resource" in {

      val response = execute(Request[IO](Method.GET, baseUri / "non-existing"))

      response.status                 shouldBe Status.NotFound
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe json"""{"message": "Resource not found"}"""
    }
  }

  private def execute(request: Request[IO]): Response[IO] =
    BlazeClientBuilder[IO](global).resource
      .use { httpClient =>
        httpClient.run(request).use { response =>
          IO.pure(response)
        }
      }
      .unsafeRunSync()

  private implicit val timer:        Timer[IO]        = IO.timer(global)
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  val port            = httpPorts.generateOne
  private val baseUri = Uri.unsafeFromString(s"http://localhost:$port")
  private val routes = HttpRoutes.of[IO] {
    case GET -> Root / "resource" => Ok("response")
  }
  new HttpServer[IO](port.value, routes).run.unsafeRunAsyncAndForget()
}
