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

package ch.datascience.http.client

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client.IORestClient._
import ch.datascience.http.client.RestClientError._
import ch.datascience.logging.ExecutionTimeRecorder
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import io.chrisdavenport.log4cats.Logger
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Status.BadRequest
import org.http4s._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.{Client, ConnectionFailure}
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

abstract class IORestClient[ThrottlingTarget](
    throttler:               Throttler[IO, ThrottlingTarget],
    logger:                  Logger[IO],
    maybeTimeRecorder:       Option[ExecutionTimeRecorder[IO]] = None,
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]) {

  import HttpRequest._

  protected lazy val validateUri: String => IO[Uri] = IORestClient.validateUri

  protected def request(method: Method, uri: Uri): Request[IO] =
    Request[IO](
      method = method,
      uri = uri
    )

  protected def request(method: Method, uri: Uri, accessToken: AccessToken): Request[IO] =
    Request[IO](
      method = method,
      uri = uri,
      headers = authHeader(accessToken)
    )

  protected def request(method: Method, uri: Uri, maybeAccessToken: Option[AccessToken]): Request[IO] =
    maybeAccessToken match {
      case Some(accessToken) => request(method, uri, accessToken)
      case _                 => request(method, uri)
    }

  protected def request(method: Method, uri: Uri, basicAuth: BasicAuthCredentials): Request[IO] =
    Request[IO](
      method = method,
      uri = uri,
      headers = Headers.of(basicAuthHeader(basicAuth))
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers.of(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers.of(Authorization(Token(Bearer, token)))
  }

  private def basicAuthHeader(basicAuth: BasicAuthCredentials): Header =
    Authorization(BasicCredentials(basicAuth.username.value, basicAuth.password.value))

  protected def send[ResultType](request: Request[IO])(mapResponse: ResponseMapping[ResultType]): IO[ResultType] =
    send(HttpRequest(request))(mapResponse)

  protected def send[ResultType](request: HttpRequest)(mapResponse: ResponseMapping[ResultType]): IO[ResultType] =
    httpClientBuilder.resource.use { httpClient =>
      for {
        _          <- throttler.acquire()
        callResult <- measureExecutionTime(callRemote(httpClient, request, mapResponse, attempt = 1), request)
        _          <- throttler.release()
      } yield callResult
    }

  private def httpClientBuilder: BlazeClientBuilder[IO] = {
    val clientBuilder = BlazeClientBuilder[IO](executionContext)
    requestTimeoutOverride map clientBuilder.withRequestTimeout getOrElse clientBuilder
  }

  private def measureExecutionTime[ResultType](block: => IO[ResultType], request: HttpRequest): IO[ResultType] =
    maybeTimeRecorder match {
      case None => block
      case Some(timeRecorder) =>
        timeRecorder
          .measureExecutionTime(block, request.toHistogramLabel)
          .map(timeRecorder.logExecutionTime(withMessage = LogMessage(request, "finished")))
    }

  private def callRemote[ResultType](httpClient:  Client[IO],
                                     request:     HttpRequest,
                                     mapResponse: ResponseMapping[ResultType],
                                     attempt:     Int
  ): IO[ResultType] =
    httpClient
      .run(request.request)
      .use(response => processResponse(request.request, mapResponse)(response))
      .recoverWith(connectionError(httpClient, request, mapResponse, attempt))

  private def processResponse[ResultType](request: Request[IO], mapResponse: ResponseMapping[ResultType])(
      response:                                    Response[IO]
  ) =
    (mapResponse orElse raiseBadRequest orElse raiseUnexpectedResponse)(response.status, request, response)
      .recoverWith(mappingError(request, response))

  private def raiseBadRequest[T]: PartialFunction[(Status, Request[IO], Response[IO]), IO[T]] = {
    case (_, request, response) if response.status == BadRequest =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          IO.raiseError(BadRequestException(LogMessage(request, response, bodyAsString)))
        }
  }

  private def raiseUnexpectedResponse[T]: PartialFunction[(Status, Request[IO], Response[IO]), IO[T]] = {
    case (_, request, response) =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          IO.raiseError(UnexpectedResponseException(LogMessage(request, response, bodyAsString)))
        }
  }

  private def mappingError[T](request: Request[IO], response: Response[IO]): PartialFunction[Throwable, IO[T]] = {
    case error: RestClientError => IO.raiseError(error)
    case NonFatal(cause) => IO.raiseError(MappingException(LogMessage(request, response, cause), cause))
  }

  private def connectionError[T](httpClient:  Client[IO],
                                 request:     HttpRequest,
                                 mapResponse: ResponseMapping[T],
                                 attempt:     Int
  ): PartialFunction[Throwable, IO[T]] = {
    case error: RestClientError => throttler.release() flatMap (_ => error.raiseError[IO, T])
    case NonFatal(cause) =>
      cause match {
        case exception: ConnectionFailure if attempt <= maxRetries.value =>
          for {
            _      <- logger.warn(LogMessage(request.request, s"timed out -> retrying attempt $attempt", exception))
            _      <- timer sleep retryInterval
            result <- callRemote(httpClient, request, mapResponse, attempt + 1)
          } yield result
        case other =>
          throttler.release() flatMap (_ =>
            ConnectivityException(LogMessage(request.request, other), other).raiseError[IO, T]
          )
      }
  }

  private type ResponseMapping[ResultType] = PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]

  private implicit class HttpRequestOps(request: HttpRequest) {
    lazy val toHistogramLabel: Option[String Refined NonEmpty] = request match {
      case UnnamedRequest(_)     => None
      case NamedRequest(_, name) => Some(name)
    }
  }

  protected object LogMessage {

    def apply(request: HttpRequest, message: String): String =
      request match {
        case UnnamedRequest(request) => s"${request.method} ${request.uri} $message"
        case NamedRequest(_, name)   => s"$name $message"
      }

    def apply(request: Request[IO], message: String, cause: Throwable): String =
      s"${request.method} ${request.uri} $message error: ${toSingleLine(cause)}"

    def apply(request: Request[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} error: ${toSingleLine(cause)}"

    def apply(request: Request[IO], response: Response[IO], responseBody: String): String =
      s"${request.method} ${request.uri} returned ${response.status}; body: ${toSingleLine(responseBody)}"

    def apply(request: Request[IO], response: Response[IO], cause: Throwable): String =
      s"${request.method} ${request.uri} returned ${response.status}; error: ${toSingleLine(cause)}"

    def toSingleLine(exception: Throwable): String =
      Option(exception.getMessage) map toSingleLine getOrElse exception.getClass.getSimpleName

    def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }
}

object IORestClient {
  import eu.timepit.refined.auto._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  val SleepAfterConnectionIssue:        FiniteDuration          = 10 seconds
  val MaxRetriesAfterConnectionTimeout: Int Refined NonNegative = 10

  def validateUri(uri: String): IO[Uri] = IO.fromEither(Uri.fromString(uri))
}
