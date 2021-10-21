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

package io.renku.http.client

import cats.MonadThrow
import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import fs2.Stream
import io.circe.Json
import io.renku.control.Throttler
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.http.client.RestClient._
import io.renku.http.client.RestClientError._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.tinytypes.ByteArrayTinyType
import io.renku.tinytypes.contenttypes.ZippedContent
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Status.BadRequest
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.pipeline.Command
import org.http4s.client.{Client, ConnectionFailure}
import org.http4s.headers.{Authorization, `Content-Disposition`, `Content-Type`}
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import java.net.{ConnectException, SocketException}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

abstract class RestClient[F[_]: Async: Logger, ThrottlingTarget](
    throttler:              Throttler[F, ThrottlingTarget],
    maybeTimeRecorder:      Option[ExecutionTimeRecorder[F]] = None,
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeoutOverride:    Option[Duration] = None,
    requestTimeoutOverride: Option[Duration] = None
) {

  import HttpRequest._

  protected lazy val validateUri: String => F[Uri] = RestClient.validateUri[F]

  protected def request(method: Method, uri: Uri): Request[F] = Request[F](
    method = method,
    uri = uri
  )

  protected def request(method: Method, uri: Uri, accessToken: AccessToken): Request[F] =
    Request[F](
      method = method,
      uri = uri,
      headers = authHeader(accessToken)
    )

  protected def request(method: Method, uri: Uri, maybeAccessToken: Option[AccessToken]): Request[F] =
    maybeAccessToken match {
      case Some(accessToken) => request(method, uri, accessToken)
      case _                 => request(method, uri)
    }

  protected def secureRequest(method: Method, uri: Uri)(implicit
      maybeAccessToken:               Option[AccessToken]
  ): Request[F] = request(method, uri, maybeAccessToken)

  protected def request(method: Method, uri: Uri, basicAuth: BasicAuthCredentials): Request[F] =
    Request[F](
      method = method,
      uri = uri,
      headers = Headers(basicAuthHeader(basicAuth))
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers(Header.Raw(ci"PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers(Authorization(Token(Bearer, token)))
  }

  private def basicAuthHeader(basicAuth: BasicAuthCredentials) =
    Authorization(BasicCredentials(basicAuth.username.value, basicAuth.password.value))

  protected def send[ResultType](request: Request[F])(mapResponse: ResponseMapping[ResultType]): F[ResultType] =
    send(HttpRequest(request))(mapResponse)

  protected def send[ResultType](
      request:   HttpRequest[F]
  )(mapResponse: ResponseMapping[ResultType]): F[ResultType] =
    httpClientBuilder.resource.use { httpClient =>
      for {
        _          <- throttler.acquire()
        callResult <- measureExecutionTime(callRemote(httpClient, request, mapResponse, attempt = 1), request)
        _          <- throttler.release()
      } yield callResult
    }

  private def httpClientBuilder: BlazeClientBuilder[F] = {
    val clientBuilder      = BlazeClientBuilder[F]
    val updatedIdleTimeout = idleTimeoutOverride map clientBuilder.withIdleTimeout getOrElse clientBuilder
    requestTimeoutOverride map updatedIdleTimeout.withRequestTimeout getOrElse updatedIdleTimeout
  }

  private def measureExecutionTime[ResultType](block: => F[ResultType], request: HttpRequest[F]): F[ResultType] =
    maybeTimeRecorder match {
      case None => block
      case Some(timeRecorder) =>
        timeRecorder
          .measureExecutionTime(block, request.toHistogramLabel)
          .map(timeRecorder.logExecutionTime(withMessage = LogMessage(request, "finished")))
    }

  private def callRemote[ResultType](httpClient:  Client[F],
                                     request:     HttpRequest[F],
                                     mapResponse: ResponseMapping[ResultType],
                                     attempt:     Int
  ): F[ResultType] =
    httpClient
      .run(request.request)
      .use(response => processResponse(request.request, mapResponse)(response))
      .recoverWith(connectionError(httpClient, request, mapResponse, attempt))

  private def processResponse[ResultType](request: Request[F], mapResponse: ResponseMapping[ResultType])(
      response:                                    Response[F]
  ): F[ResultType] =
    (mapResponse orElse raiseBadRequest orElse raiseUnexpectedResponse)((response.status, request, response))
      .recoverWith(mappingError(request, response))

  private def raiseBadRequest[T]: PartialFunction[(Status, Request[F], Response[F]), F[T]] = {
    case (_, request, response) if response.status == BadRequest =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          MonadThrow[F].raiseError(BadRequestException(LogMessage(request, response, bodyAsString)))
        }
  }

  private def raiseUnexpectedResponse[T]: PartialFunction[(Status, Request[F], Response[F]), F[T]] = {
    case (_, request, response) =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          MonadThrow[F].raiseError(
            UnexpectedResponseException(response.status, LogMessage(request, response, bodyAsString))
          )
        }
  }

  private def mappingError[T](request: Request[F], response: Response[F]): PartialFunction[Throwable, F[T]] = {
    case error: RestClientError => MonadThrow[F].raiseError(error)
    case NonFatal(cause: InvalidMessageBodyFailure) =>
      val exception = new Exception(List(cause, cause.getCause()).map(_.getMessage()).mkString("; "), cause)
      MonadThrow[F].raiseError(MappingException(LogMessage(request, response, exception), exception))
    case NonFatal(cause) =>
      MonadThrow[F].raiseError(MappingException(LogMessage(request, response, cause), cause))
  }

  private def connectionError[T](httpClient:  Client[F],
                                 request:     HttpRequest[F],
                                 mapResponse: ResponseMapping[T],
                                 attempt:     Int
  ): PartialFunction[Throwable, F[T]] = {
    case error: RestClientError => throttler.release() >> error.raiseError[F, T]
    case NonFatal(exception @ (_: ConnectionFailure | _: ConnectException | _: Command.EOF.type | _: SocketException))
        if attempt <= maxRetries.value =>
      for {
        _      <- Logger[F].warn(LogMessage(request.request, s"timed out -> retrying attempt $attempt", exception))
        _      <- Temporal[F] sleep retryInterval
        result <- callRemote(httpClient, request, mapResponse, attempt + 1)
      } yield result
    case NonFatal(exception @ (_: ConnectionFailure | _: ConnectException | _: Command.EOF.type | _: SocketException))
        if attempt > maxRetries.value =>
      throttler.release() >> ConnectivityException(LogMessage(request.request, exception), exception).raiseError[F, T]
    case NonFatal(exception) =>
      throttler.release() >> ClientException(LogMessage(request.request, exception), exception).raiseError[F, T]
  }

  private type ResponseMapping[ResultType] =
    PartialFunction[(Status, Request[F], Response[F]), F[ResultType]]

  private implicit class HttpRequestOps(request: HttpRequest[F]) {
    lazy val toHistogramLabel: Option[String Refined NonEmpty] = request match {
      case UnnamedRequest(_)     => None
      case NamedRequest(_, name) => Some(name)
    }
  }

  protected object LogMessage {

    def apply(request: HttpRequest[F], message: String): String =
      request match {
        case UnnamedRequest(request) => s"${request.method} ${request.uri} $message"
        case NamedRequest(_, name)   => s"$name $message"
      }

    def apply(request: Request[F], message: String, cause: Throwable): String =
      s"${request.method} ${request.uri} $message error: ${toSingleLine(cause)}"

    def apply(request: Request[F], cause: Throwable): String =
      s"${request.method} ${request.uri} error: ${toSingleLine(cause)}"

    def apply(request: Request[F], response: Response[F], responseBody: String): String =
      s"${request.method} ${request.uri} returned ${response.status}; body: ${toSingleLine(responseBody)}"

    def apply(request: Request[F], response: Response[F], cause: Throwable): String =
      s"${request.method} ${request.uri} returned ${response.status}; error: ${toSingleLine(cause)}"

    def toSingleLine(exception: Throwable): String =
      Option(exception.getMessage) map toSingleLine getOrElse exception.getClass.getSimpleName

    def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }

  implicit class RequestOps(request: Request[F]) {
    lazy val withMultipartBuilder: MultipartBuilder = new MultipartBuilder(request)

    def withParts(parts: Vector[Part[F]]): Request[F] =
      new MultipartBuilder(request, parts).build()

    class MultipartBuilder private[RequestOps] (request: Request[F], parts: Vector[Part[F]] = Vector.empty[Part[F]]) {
      def addPart[PartType](name: String, value: PartType)(implicit
          encoder:                PartEncoder[PartType]
      ): MultipartBuilder =
        new MultipartBuilder(request, encoder.encode[F](name, value) +: parts)

      def maybeAddPart[PartType](name: String, maybeValue: Option[PartType])(implicit
          encoder:                     PartEncoder[PartType]
      ): MultipartBuilder = maybeValue
        .map(addPart(name, _))
        .getOrElse(this)

      def build(): Request[F] = {
        val multipart = Multipart[F](parts)
        request
          .withEntity(multipart)
          .withHeaders(multipart.headers.headers.filterNot(_.name == CIString("transfer-encoding")))
      }
    }
  }
}

object RestClient {

  import eu.timepit.refined.auto._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  val SleepAfterConnectionIssue:        FiniteDuration          = 10 seconds
  val MaxRetriesAfterConnectionTimeout: Int Refined NonNegative = 10

  def validateUri[F[_]: MonadThrow](uri: String): F[Uri] =
    MonadThrow[F].fromEither(Uri.fromString(uri))

  trait PartEncoder[-PartType] {
    def encode[F[_]](name: String, value: PartType): Part[F]
  }

  implicit object JsonPartEncoder extends PartEncoder[Json] {

    override def encode[F[_]](name: String, value: Json): Part[F] = Part
      .formData[F](name, encodeValue(value), contentType)

    private def encodeValue(value: Json): String = value.noSpaces

    private val contentType: `Content-Type` = `Content-Type`(MediaType.application.json)
  }

  implicit object StringPartEncoder extends PartEncoder[String] {

    override def encode[F[_]](name: String, value: String): Part[F] = Part
      .formData[F](name, encodeValue(value), contentType)

    private def encodeValue(value: String): String = value

    private val contentType: `Content-Type` = `Content-Type`(MediaType.text.plain)
  }

  implicit object ZipPartEncoder extends PartEncoder[ByteArrayTinyType with ZippedContent] {

    override def encode[F[_]](name: String, value: ByteArrayTinyType with ZippedContent): Part[F] = Part(
      Headers(
        `Content-Disposition`("form-data", Map(CIString("name") -> name)),
        Header.Raw(CIString("Content-Transfer-Encoding"), "binary"),
        `Content-Type`(MediaType.application.zip)
      ),
      body = Stream.emits(value.value)
    )
  }
}
