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

import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import org.http4s.blaze.server._
import org.http4s.server.ServerBuilder
import org.http4s.{HttpRoutes, Request, Response}

trait HttpServer[F[_]] {
  val serverBuilder: ServerBuilder[F]
  def run(): F[ExitCode]
}

object HttpServer {
  def apply[F[_]: Async](serverPort: Int, serviceRoutes: HttpRoutes[F]): HttpServer[F] =
    new HttpServerImpl[F](serverPort, serviceRoutes)
}

class HttpServerImpl[F[_]: Async](serverPort: Int, serviceRoutes: HttpRoutes[F]) extends HttpServer[F] {

  import QueryParameterTools.resourceNotFound

  lazy val serverBuilder: ServerBuilder[F] = BlazeServerBuilder[F]
    .bindHttp(serverPort, "0.0.0.0")
    .withHttpApp(serviceRoutes.orNotFound)

  def run(): F[ExitCode] = serverBuilder.serve.compile.drain.as(ExitCode.Success)

  private implicit class RoutesOps(routes: HttpRoutes[F]) {
    def orNotFound: Kleisli[F, Request[F], Response[F]] = Kleisli {
      routes.run(_).getOrElseF(resourceNotFound)
    }
  }
}
