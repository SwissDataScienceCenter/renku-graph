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

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server

import scala.concurrent.duration._

object SimpleServer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    server(port"8088").useForever.as(ExitCode.Success)

  val routes: HttpRoutes[IO] =
    HttpRoutes.of { case GET -> Root =>
      IO.sleep(50.seconds).flatMap(_ => Ok())
    }

  def server(port: Port): Resource[IO, Server] =
    EmberServerBuilder
      .default[IO]
      .withPort(port)
      .withHost(host"0.0.0.0")
      .withHttpApp(routes.orNotFound)
      .build
}
