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

package io.renku.graph.acceptancetests.tooling

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

class RestClientImpl(implicit logger: Logger[IO])
    extends RestClient[IO, RestClientImpl](Throttler.noThrottling, retryInterval = 500 millis, maxRetries = 1) {

  def GET(url: String)(implicit ioRuntime: IORuntime): IO[Response[IO]] =
    for {
      uri      <- validateUri(url)
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield response

  def GET(url: String, accessToken: AccessToken)(implicit ioRuntime: IORuntime): IO[Response[IO]] =
    for {
      uri      <- validateUri(url)
      response <- send(request(Method.GET, uri, accessToken))(mapResponse)
    } yield response

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Response[IO]]] = {
    case (_, _, response) => IO.pure(response)
  }
}
