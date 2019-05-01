/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.tooling

import cats.effect.{ContextShift, IO}
import ch.datascience.control.Throttler
import ch.datascience.http.client.{AccessToken, IORestClient}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Url
import org.http4s.Status.Ok

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

object WebhookServiceClient {
  def apply()(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): ServiceClient =
    new ServiceClient {
      override val baseUrl: String Refined Url = "http://localhost:9001"
    }
}

object TokenRepositoryClient {
  def apply()(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): ServiceClient =
    new ServiceClient {
      override val baseUrl: String Refined Url = "http://localhost:9003"
    }
}

abstract class ServiceClient(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends IORestClient(Throttler.noThrottling) {

  import ServiceClient.ServiceReadiness
  import ServiceClient.ServiceReadiness._
  import cats.implicits._
  import org.http4s.{Method, Request, Response, Status}

  val baseUrl: String Refined Url

  def POST(url: String, maybeAccessToken: Option[AccessToken]): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.POST, uri, maybeAccessToken))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String, maybeAccessToken: Option[AccessToken]): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.GET, uri, maybeAccessToken))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def ping: IO[ServiceReadiness] = {
    for {
      uri      <- validateUri(s"$baseUrl/ping")
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield
      if (response.status == Ok) ServiceUp
      else ServiceDown
  } recover {
    case NonFatal(_) => ServiceDown
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Response[IO]]] = {
    case (_, _, response) => IO.pure(response)
  }
}

object ServiceClient {
  sealed trait ServiceReadiness
  object ServiceReadiness {
    final case object ServiceUp   extends ServiceReadiness
    final case object ServiceDown extends ServiceReadiness
  }
}
