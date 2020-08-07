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

import cats.data.Kleisli
import cats.effect._
import cats.implicits._
import ch.datascience.controllers.InfoMessage
import ch.datascience.controllers.InfoMessage._
import org.http4s.server.blaze._
import org.http4s.{HttpRoutes, Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class HttpServer[F[_]: ConcurrentEffect](
    serverPort:    Int,
    serviceRoutes: HttpRoutes[F]
)(implicit timer:  Timer[F], executionContext: ExecutionContext) {

  def run: F[ExitCode] =
    BlazeServerBuilder[F](executionContext)
      .bindHttp(serverPort, "0.0.0.0")
      .withHttpApp(serviceRoutes.orNotFound)
      .serve
      .compile
      .drain
      .as(ExitCode.Success)

  private implicit class RoutesOps(routes: HttpRoutes[F]) {
    def orNotFound: Kleisli[F, Request[F], Response[F]] =
      Kleisli { a =>
        routes
          .run(a)
          .getOrElse(Response(Status.NotFound) withEntity InfoMessage("Resource not found"))
      }
  }
}
