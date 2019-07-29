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

import java.net.ConnectException

import IORestClient._
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client.RestClientError.{MappingError, UnexpectedResponseError}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.chrisdavenport.log4cats.Logger
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

abstract class IORestClient[ThrottlingTarget](
    throttler:               Throttler[IO, ThrottlingTarget],
    logger:                  Logger[IO],
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]) {

  protected lazy val validateUri: String => IO[Uri] = IORestClient.validateUri

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

  protected def request(method: Method, uri: Uri, maybeAccessToken: Option[AccessToken]): Request[IO] =
    maybeAccessToken match {
      case Some(accessToken) => request(method, uri, accessToken)
      case _                 => request(method, uri)
    }

  protected def request(method: Method, uri: Uri, basicAuth: BasicAuthCredentials): Request[IO] =
    Request[IO](
      method  = method,
      uri     = uri,
      headers = Headers.of(basicAuthHeader(basicAuth))
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers.of(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers.of(Authorization(Token(Bearer, token)))
  }

  private def basicAuthHeader(basicAuth: BasicAuthCredentials): Header =
    Authorization(BasicCredentials(basicAuth.username.value, basicAuth.password.value))

  protected def send[ResultType](request: Request[IO])(mapResponse: ResponseMapping[ResultType]): IO[ResultType] =
    BlazeClientBuilder[IO](executionContext).resource.use { httpClient =>
      for {
        _          <- throttler.acquire
        callResult <- callRemote(httpClient, request, mapResponse, attempt = 1)
        _          <- throttler.release
      } yield callResult
    }

  private def callRemote[ResultType](httpClient:  Client[IO],
                                     request:     Request[IO],
                                     mapResponse: ResponseMapping[ResultType],
                                     attempt:     Int) =
    httpClient
      .fetch[ResultType](request)(processResponse(request, mapResponse))
      .recoverWith(connectionError(httpClient, request, mapResponse, attempt))

  private def processResponse[ResultType](request: Request[IO], mapResponse: ResponseMapping[ResultType])(
      response:                                    Response[IO]) =
    (mapResponse orElse raiseUnexpectedResponseError)(response.status, request, response)
      .recoverWith(mappingError(request, response))

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

  private def connectionError[T](httpClient:  Client[IO],
                                 request:     Request[IO],
                                 mapResponse: ResponseMapping[T],
                                 attempt:     Int): PartialFunction[Throwable, IO[T]] = {
    case error: RestClientError =>
      throttler.release *> IO.raiseError(error)
    case NonFatal(cause) =>
      cause match {
        case exception: ConnectException if attempt <= maxRetries.value =>
          logger.warn(ExceptionMessage(request, s"timed out -> retrying attempt $attempt", exception))
          timer.sleep(retryInterval) *> callRemote(httpClient, request, mapResponse, attempt + 1)
        case other =>
          throttler.release *> IO.raiseError(new Exception(ExceptionMessage(request, other), other))
      }
  }

  private type ResponseMapping[ResultType] = PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]

  protected object ExceptionMessage {

    def apply(request: Request[IO], message: String, cause: Throwable): String =
      s"${request.method} ${request.uri} $message error: ${toSingleLine(cause.getMessage)}"

    def apply(request: Request[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} error: ${toSingleLine(cause.getMessage)}"

    def apply(request: Request[IO], response: Response[IO], responseBody: String): String =
      s"${request.method} ${request.uri} returned ${response.status}; body: ${toSingleLine(responseBody)}"

    def apply(request: Request[IO], response: Response[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} returned ${response.status}; error: ${toSingleLine(cause.getMessage)}"

    def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }
}

object IORestClient {
  import eu.timepit.refined.auto._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  val SleepAfterConnectionIssue:        FiniteDuration          = 10 seconds
  val MaxRetriesAfterConnectionTimeout: Int Refined NonNegative = 10

  def validateUri(uri: String): IO[Uri] =
    IO.fromEither(Uri.fromString(uri))
}
