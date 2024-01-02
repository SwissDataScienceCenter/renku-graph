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

package io.renku.http.server

import cats.effect._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpPorts
import io.renku.http.server.EndpointTester._
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class HttpServerSpec extends AsyncWordSpec with CustomAsyncIOSpec with Http4sDsl[IO] with should.Matchers {

  "run" should {

    "create an http server to serve the given routes" in {
      client
        .run(Request[IO](Method.GET, baseUri / "resource"))
        .use { response =>
          for {
            _ <- response.as[String].asserting(_ shouldBe "response")
            _ = response.status shouldBe Status.Ok
          } yield ()
        }
    }

    "create an http server which responds with NOT_FOUND and JSON body for non-existing resource" in {
      client
        .run(Request[IO](Method.GET, baseUri / "non-existing"))
        .use { response =>
          for {
            _ <- response.as[Message].asserting(_ shouldBe Message.Info("Resource not found"))
            _ = response.status      shouldBe Status.NotFound
            _ = response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
          } yield ()
        }
    }
  }

  private lazy val port = httpPorts.generateOne
  private lazy val baseUri: Uri = Uri.unsafeFromString(s"http://localhost:$port")
  private lazy val routes = HttpRoutes.of[IO] { case GET -> Root / "resource" => Ok("response") }
  private lazy val client: Client[IO] = Client.fromHttpApp(HttpServer[IO](port, routes).httpApp)
}
