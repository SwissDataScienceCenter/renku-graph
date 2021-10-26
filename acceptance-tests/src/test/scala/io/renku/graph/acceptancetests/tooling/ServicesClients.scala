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

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Url
import io.circe.Json
import io.circe.literal._
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, BasicAuthCredentials, RestClient}
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.model.HookToken
import org.http4s.Status.Ok
import org.http4s.{Header, Method, Response}
import org.scalatest.matchers.should
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

object WebhookServiceClient {

  def apply()(implicit logger: Logger[IO]) = new WebhookServiceClient

  class WebhookServiceClient(implicit logger: Logger[IO]) extends ServiceClient with should.Matchers {
    import io.circe.Json

    override val baseUrl: String Refined Url = "http://localhost:9001"

    def POST(url: String, hookToken: HookToken, payload: Json)(implicit ioRuntime: IORuntime): Response[IO] = {
      for {
        hookTokenCrypto    <- HookTokenCrypto[IO]()
        encryptedHookToken <- hookTokenCrypto.encrypt(hookToken)
        tokenHeader        <- IO.pure(Header.Raw(ci"X-Gitlab-Token", encryptedHookToken.value))
        uri                <- validateUri(s"$baseUrl/$url")
        response           <- send(request(Method.POST, uri) withHeaders tokenHeader withEntity payload)(mapResponse)
      } yield response
    }.unsafeRunSync()
  }
}

object CommitEventServiceClient {
  def apply()(implicit logger: Logger[IO]): ServiceClient = new ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9006"
  }
}

object TriplesGeneratorClient {
  def apply()(implicit logger: Logger[IO]): ServiceClient = new ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9002"
  }
}

object TokenRepositoryClient {
  def apply()(implicit logger: Logger[IO]): ServiceClient = new ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9003"
  }
}

object KnowledgeGraphClient {
  import sangria.ast.Document

  def apply()(implicit logger: Logger[IO]): KnowledgeGraphClient = new KnowledgeGraphClient

  class KnowledgeGraphClient(implicit logger: Logger[IO]) extends ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9004"

    def POST(query: Document, variables: Map[String, String] = Map.empty, maybeAccessToken: Option[AccessToken] = None)(
        implicit ioRuntime: IORuntime
    ): Response[IO] = {
      for {
        uri      <- validateUri(s"$baseUrl/knowledge-graph/graphql")
        payload  <- preparePayload(query, variables)
        response <- send(request(Method.POST, uri, maybeAccessToken) withEntity payload)(mapResponse)
      } yield response
    }.unsafeRunSync()

    private def preparePayload(query: Document, variables: Map[String, String]): IO[Json] = IO {
      variables match {
        case vars if vars.isEmpty =>
          json"""{
            "query": ${query.source.getOrElse("")}
          }                                                                     
          """
        case nonEmptyVars =>
          json"""{
            "query": ${query.source.getOrElse("")},
            "variables": $nonEmptyVars 
          }                                                                     
          """
      }
    }
  }
}

object EventLogClient {

  def apply()(implicit logger: Logger[IO]): EventLogClient = new EventLogClient

  class EventLogClient(implicit logger: Logger[IO]) extends ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9005"

    def fetchProcessingStatus(projectId: projects.Id)(implicit ioRuntime: IORuntime): Response[IO] =
      GET(s"processing-status?project-id=$projectId")
  }
}

abstract class ServiceClient(implicit logger: Logger[IO])
    extends RestClient[IO, ServiceClient](Throttler.noThrottling, retryInterval = 500 millis, maxRetries = 1) {

  import ServiceClient.ServiceReadiness
  import ServiceClient.ServiceReadiness._
  import cats.syntax.all._
  import org.http4s.circe.jsonEncoderOf
  import org.http4s.{EntityEncoder, Method, Request, Response, Status}
  protected implicit val jsonEntityEncoder: EntityEncoder[IO, Json] = jsonEncoderOf[IO, Json]

  val baseUrl: String Refined Url

  def POST(url: String, maybeAccessToken: Option[AccessToken])(implicit ioRuntime: IORuntime): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.POST, uri, maybeAccessToken))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def PUT(url:   String, payload: Json, maybeAccessToken: Option[AccessToken])(implicit
      ioRuntime: IORuntime
  ): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.PUT, uri, maybeAccessToken) withEntity payload)(mapResponse)
    } yield response
  }.unsafeRunSync()

  def DELETE(url: String, basicAuth: BasicAuthCredentials)(implicit ioRuntime: IORuntime): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.DELETE, uri, basicAuth))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String, accessToken: AccessToken)(implicit ioRuntime: IORuntime): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.GET, uri, maybeAccessToken = Some(accessToken)))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String)(implicit ioRuntime: IORuntime): Response[IO] = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def ping: IO[ServiceReadiness] = {
    for {
      uri      <- validateUri(s"$baseUrl/ping")
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield
      if (response.status == Ok) ServiceUp
      else ServiceDown
  } recover { case NonFatal(_) =>
    ServiceDown
  }

  protected lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Response[IO]]] = {
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
