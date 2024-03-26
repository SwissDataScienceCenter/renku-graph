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

package io.renku.graph.acceptancetests.tooling

import cats.Monad
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Temporal}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Url
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import io.renku.control.Throttler
import io.renku.graph.acceptancetests.tooling.ServiceClient.ClientResponse
import io.renku.graph.model.events.{EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.{AccessToken, BasicAuthCredentials, GitLabClient, RestClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.model.HookToken
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.{Accepted, Ok}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.implicits._
import org.http4s.{Header, Headers, Method, Request, Response, Status, Uri}
import org.scalatest.Assertions.fail
import org.scalatest.matchers.should
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

object WebhookServiceClient {

  def apply()(implicit logger: Logger[IO]) = new WebhookServiceClient

  class WebhookServiceClient(implicit logger: Logger[IO]) extends ServiceClient with should.Matchers {
    import io.circe.Json

    override val baseUrl: String Refined Url = "http://localhost:9001"

    def POST(url: String, hookToken: HookToken, payload: Json)(implicit ioRuntime: IORuntime): ClientResponse = {
      for {
        hookTokenCrypto    <- HookTokenCrypto[IO]()
        encryptedHookToken <- hookTokenCrypto.encrypt(hookToken)
        tokenHeader        <- IO.pure(Header.Raw(ci"X-Gitlab-Token", encryptedHookToken.value))
        uri                <- validateUri(s"$baseUrl/$url")
        response           <- send(request(Method.POST, uri) withHeaders tokenHeader withEntity payload)(mapResponse)
      } yield response
    }.unsafeRunSync()

    def `POST projects/:id/webhooks`(projectId: projects.GitLabId, accessToken: AccessToken)(implicit
        ior: IORuntime
    ): ClientResponse =
      POST((uri"projects" / projectId / "webhooks").renderString, accessToken)

    def `GET projects/:id/events/status`(projectId: projects.GitLabId, accessToken: AccessToken)(implicit
        ior: IORuntime
    ): ClientResponse =
      GET((uri"projects" / projectId / "events" / "status").renderString, accessToken)
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

  def apply()(implicit logger: Logger[IO]): KnowledgeGraphClient = new KnowledgeGraphClient

  class KnowledgeGraphClient(implicit logger: Logger[IO]) extends ServiceClient {
    override val baseUrl: String Refined Url = "http://localhost:9004"

    def `GET /knowledge-graph/ontology`(implicit ioRuntime: IORuntime): (Status, Headers, String) = {
      val responseMapping: PartialFunction[(Status, Request[IO], Response[IO]), IO[(Status, Headers, String)]] = {
        case (status, _, response) =>
          response
            .as[String]
            .map(pageBody => (status, response.headers, pageBody))
      }

      for {
        uri      <- validateUri(s"$baseUrl/knowledge-graph/ontology/index-en.html")
        response <- send(request(Method.GET, uri))(responseMapping)
      } yield response
    }.unsafeRunSync()
  }
}

object EventLogClient {
  final case class ProjectEvent(
      id:      EventId,
      project: ProjectEvent.Project,
      status:  EventStatus
  )
  object ProjectEvent {

    final case class Project(id: projects.GitLabId, slug: projects.Slug)
    object Project {
      implicit val jsonDecoder: Decoder[Project] = deriveDecoder
    }

    implicit val jsonDecoder: Decoder[ProjectEvent] = deriveDecoder
  }

  def apply()(implicit logger: Logger[IO]): EventLogClient = new EventLogClient

  class EventLogClient(implicit logger: Logger[IO]) extends ServiceClient {

    override val baseUrl: String Refined Url = "http://localhost:9005"

    def waitForReadiness: IO[Unit] =
      Monad[IO].whileM_(isRunning)(Temporal[IO] sleep (100 millis))

    def sendEvent(event: Json)(implicit ioRuntime: IORuntime): Unit = {
      for {
        uri <- validateUri(s"$baseUrl/events")
        req <- createRequest(uri, event)
        _   <- send(req) { case (Accepted, _, _) => ().pure[IO] }
      } yield ()
    }.unsafeRunSync()

    def getEvents(project: Either[projects.GitLabId, projects.Slug]): IO[List[ProjectEvent]] =
      for {
        uri <- validateUri(s"$baseUrl/events").map(uri =>
                 project.fold(id => uri.withQueryParam("project-id", id.value),
                              slug => uri.withQueryParam("project-slug", slug.value)
                 )
               )
        req = request(Method.GET, uri)
        r <- send(req) { case (Status.Ok, _, resp) =>
               resp.as[List[ProjectEvent]]
             }
      } yield r

    private def createRequest(uri: Uri, event: Json) =
      request(Method.POST, uri).withMultipartBuilder
        .addPart("event", event)
        .build()

    private def isRunning = getStatus.flatMap { jsonBody =>
      jsonBody.hcursor
        .downField("isMigrating")
        .as[Boolean]
        .fold(_ => new Exception("Could not decode status").raiseError[IO, Boolean], isMigrating => IO(isMigrating))
    }

    private def getStatus = for {
      uri      <- validateUri(s"$baseUrl/migration-status")
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield response.jsonBody
  }
}

abstract class ServiceClient(implicit logger: Logger[IO])
    extends RestClient[IO, ServiceClient](Throttler.noThrottling, retryInterval = 500 millis, maxRetries = 1)
    with RenkuEntityCodec {

  import ServiceClient.ServiceReadiness
  import ServiceClient.ServiceReadiness._
  import cats.syntax.all._
  import org.http4s.{Method, Request, Response, Status}

  val baseUrl: String Refined Url

  def POST(url: String, accessToken: AccessToken)(implicit ioRuntime: IORuntime): ClientResponse = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(GitLabClient.request[IO](Method.POST, uri, accessToken.some))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def POST(url: String, payload: Json, maybeAccessToken: Option[AccessToken])(implicit
      ioRuntime: IORuntime
  ): Status = {
    validateUri(s"$baseUrl/$url") >>=
      (url =>
        send(GitLabClient.request[IO](Method.POST, url, maybeAccessToken).withEntity(payload)) { case (status, _, _) =>
          status.pure[IO]
        }
      )
  }.unsafeRunSync()

  def PUT(url: String, payload: Json, maybeAccessToken: Option[AccessToken])(implicit
      ioRuntime: IORuntime
  ): Status = {
    for {
      uri <- validateUri(s"$baseUrl/$url")
      status <- send(GitLabClient.request[IO](Method.PUT, uri, maybeAccessToken) withEntity payload) {
                  case (status, _, _) =>
                    status.pure[IO]
                }
    } yield status
  }.unsafeRunSync()

  def DELETE(url: String, basicAuth: BasicAuthCredentials)(implicit ioRuntime: IORuntime): ClientResponse = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.DELETE, uri, basicAuth))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def DELETE(url: String, accessToken: AccessToken)(implicit ioRuntime: IORuntime): ClientResponse = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(GitLabClient.request[IO](Method.DELETE, uri, accessToken.some))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String, accessToken: AccessToken)(implicit ioRuntime: IORuntime): ClientResponse = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(GitLabClient.request[IO](Method.GET, uri, accessToken.some))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def GET(url: String)(implicit ioRuntime: IORuntime): ClientResponse = {
    for {
      uri      <- validateUri(s"$baseUrl/$url")
      response <- send(request(Method.GET, uri))(mapResponse)
    } yield response
  }.unsafeRunSync()

  def ping: IO[ServiceReadiness] = {
    for {
      uri    <- validateUri(s"$baseUrl/ping")
      status <- send(request(Method.GET, uri)) { case (status, _, _) => status.pure[IO] }
    } yield
      if (status == Ok) ServiceUp
      else ServiceDown
  } recover { case NonFatal(_) => ServiceDown }

  protected lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ClientResponse]] = {
    case (status, _, response) => response.as[Json].map(json => ClientResponse(status, json, response.headers))
  }
}

object ServiceClient {
  sealed trait ServiceReadiness
  object ServiceReadiness {
    final case object ServiceUp   extends ServiceReadiness
    final case object ServiceDown extends ServiceReadiness
  }

  case class ClientResponse(status: Status, jsonBody: Json, headers: Headers) {

    def headerLink(rel: String): String =
      headers.headers
        .find(_.value contains s"""rel="$rel"""")
        .map { header =>
          val value = header.value
          value.substring(value.lastIndexOf("<") + 1, value.lastIndexOf(">"))
        }
        .getOrElse(fail(s"""No link with the rel="$rel""""))
  }
}
