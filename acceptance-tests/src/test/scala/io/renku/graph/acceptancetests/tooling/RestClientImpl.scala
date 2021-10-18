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

package io.renku.graph.acceptancetests.tooling

import cats.effect.{ConcurrentEffect, IO, Timer}
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.interpreters.TestLogger
import org.http4s._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class RestClientImpl()(implicit
    executionContext: ExecutionContext,
    concurrentEffect: ConcurrentEffect[IO],
    timer:            Timer[IO]
) extends RestClient[IO, RestClientImpl](Throttler.noThrottling,
                                         TestLogger(),
                                         retryInterval = 500 millis,
                                         maxRetries = 1
    ) {

  def GET(url: String): Response[IO] = {
    for {
      uri      <- validateUri(url)
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String, accessToken: AccessToken): Response[IO] = {
    for {
      uri      <- validateUri(url)
      response <- send(request(Method.GET, uri, accessToken))(mapResponse)
    } yield response
  }.unsafeRunSync()

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Response[IO]]] = {
    case (_, _, response) => IO.pure(response)
  }
}
