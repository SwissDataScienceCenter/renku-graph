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

package ch.datascience.http.client

import cats.implicits._
import cats.effect.{ContextShift, IO}
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client.RestClientError.{MappingError, UnexpectedResponseError}
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

abstract class IORestClient(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO]
) {

  protected def validateUri(uri: String): IO[Uri] =
    IO.fromEither(Uri.fromString(uri))

  protected def request(method: Method, uri: Uri): Request[IO] =
    Request[IO](
      method = method,
      uri    = uri
    )

  protected def request(method: Method, uri: Uri, accessToken: AccessToken): Request[IO] =
    Request[IO](
      method  = method,
      uri     = uri,
      headers = authHeader(accessToken)
    )

  protected def request(method: Method, uri: Uri, basicAuth: BasicAuth): Request[IO] =
    Request[IO](
      method  = method,
      uri     = uri,
      headers = basicAuthHeader(basicAuth)
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers(Authorization(Token(Bearer, token)))
  }

  private def basicAuthHeader(basicAuth: BasicAuth): Headers =
    Headers(Authorization(BasicCredentials(basicAuth.username.value, basicAuth.password.value)))

  protected def send[ResultType](request: Request[IO])(
      mapResponse:                        PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]): IO[ResultType] =
    BlazeClientBuilder[IO](executionContext).resource.use { httpClient =>
      httpClient
        .fetch[ResultType](request) { response =>
          (mapResponse orElse raiseUnexpectedResponseError)(response.status, request, response)
            .recoverWith(mappingError(request, response))
        }
        .recoverWith(connectionError(request))
    }

  private def raiseUnexpectedResponseError[T]: PartialFunction[(Status, Request[IO], Response[IO]), IO[T]] = {
    case (_, request, response) =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          IO.raiseError(UnexpectedResponseError(ExceptionMessage(request, response, bodyAsString)))
        }
  }

  private def mappingError[T](request: Request[IO], response: Response[IO]): PartialFunction[Throwable, IO[T]] = {
    case error: RestClientError => IO.raiseError(error)
    case NonFatal(cause) => IO.raiseError(MappingError(ExceptionMessage(request, response, cause), cause))
  }

  private def connectionError[T](request: Request[IO]): PartialFunction[Throwable, IO[T]] = {
    case error: RestClientError => IO.raiseError(error)
    case NonFatal(cause) =>
      IO.raiseError(new RuntimeException(ExceptionMessage(request, cause), cause))
  }

  private object ExceptionMessage {

    def apply(request: Request[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} error: ${toSingleLine(cause.getMessage)}"

    def apply(request: Request[IO], response: Response[IO], responseBody: String): String =
      s"${request.method} ${request.uri} returned ${response.status}; body: ${toSingleLine(responseBody)}"

    def apply(request: Request[IO], response: Response[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} returned ${response.status}; error: ${toSingleLine(cause.getMessage)}"

    private def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }
}
