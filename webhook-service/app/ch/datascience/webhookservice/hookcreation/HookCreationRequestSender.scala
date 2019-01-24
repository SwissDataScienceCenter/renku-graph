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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.crypto.HookTokenCrypto.HookAuthToken
import ch.datascience.webhookservice.model.AccessToken
import ch.datascience.webhookservice.model.AccessToken._
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private abstract class HookCreationRequestSender[Interpretation[_]] {
  def createHook(
      projectId:     ProjectId,
      accessToken:   AccessToken,
      hookAuthToken: HookAuthToken
  ): Interpretation[Unit]
}

@Singleton
private class IOHookCreationRequestSender @Inject()(configProvider: IOHookCreationConfigProvider)(
    implicit executionContext:                                      ExecutionContext)
    extends HookCreationRequestSender[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.eventprocessing.routes.WebhookEventEndpoint
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe.Json
  import org.http4s.AuthScheme.Bearer
  import org.http4s.Credentials.Token
  import org.http4s.Method.POST
  import org.http4s.Status.{Created, Unauthorized}
  import org.http4s.circe._
  import org.http4s.client.blaze.BlazeClientBuilder
  import org.http4s.headers.Authorization
  import org.http4s.{Header, Headers, Request, Response, Uri}

  private implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  private val F = implicitly[ConcurrentEffect[IO]]

  def createHook(projectId: ProjectId, accessToken: AccessToken, hookAuthToken: HookAuthToken): IO[Unit] =
    for {
      config <- configProvider.get()
      uri    <- F.fromEither(Uri.fromString(s"${config.gitLabUrl}/api/v4/projects/$projectId/hooks"))
      payload = createPayload(projectId, hookAuthToken, config.selfUrl)
      request = createRequest(uri, accessToken, payload)
      result <- send(request)
    } yield result

  private def createPayload(projectId: ProjectId, hookAuthToken: HookAuthToken, selfUrl: HookCreationConfig.HostUrl) =
    Json.obj(
      "id"          -> Json.fromInt(projectId.value),
      "url"         -> Json.fromString(s"$selfUrl${WebhookEventEndpoint.processPushEvent().url}"),
      "push_events" -> Json.fromBoolean(true),
      "token"       -> Json.fromString(hookAuthToken.value)
    )

  private def createRequest(uri: Uri, accessToken: AccessToken, payload: Json) =
    Request[IO](
      method  = POST,
      uri     = uri,
      headers = authHeader(accessToken)
    ).withEntity(payload)

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers(Authorization(Token(Bearer, token)))
  }

  private def send(request: Request[IO]) = BlazeClientBuilder[IO](executionContext).resource.use { httpClient =>
    httpClient.fetch[Unit](request) { response =>
      response.status match {
        case Created      => F.pure(())
        case Unauthorized => F.raiseError(UnauthorizedException)
        case _            => raiseError(request, response)
      }
    }
  }

  private def raiseError(request: Request[IO], response: Response[IO]): IO[Unit] =
    for {
      bodyAsString <- response.as[String]
      _ <- F.raiseError {
            new RuntimeException(
              s"${request.method} ${request.uri} returned ${response.status}; body: ${bodyAsString.split('\n').map(_.trim.filter(_ >= ' ')).mkString}")
          }
    } yield ()
}
