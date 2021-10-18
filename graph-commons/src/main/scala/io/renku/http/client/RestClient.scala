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
import cats.effect.{ConcurrentEffect, Timer}
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
import org.http4s.blaze.pipeline.Command
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.{Client, ConnectionFailure}
import org.http4s.headers.{Authorization, `Content-Disposition`, `Content-Type`}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.util.CaseInsensitiveString
import org.typelevel.log4cats.Logger

import java.net.ConnectException
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

abstract class RestClient[Interpretation[_]: ConcurrentEffect: Timer, ThrottlingTarget](
    throttler:               Throttler[Interpretation, ThrottlingTarget],
    logger:                  Logger[Interpretation],
    maybeTimeRecorder:       Option[ExecutionTimeRecorder[Interpretation]] = None,
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeoutOverride:     Option[Duration] = None,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext) {

  import HttpRequest._

  protected lazy val validateUri: String => Interpretation[Uri] = RestClient.validateUri[Interpretation]

  protected def request(method: Method, uri: Uri): Request[Interpretation] =
    Request[Interpretation](
      method = method,
      uri = uri
    )

  protected def request(method: Method, uri: Uri, accessToken: AccessToken): Request[Interpretation] =
    Request[Interpretation](
      method = method,
      uri = uri,
      headers = authHeader(accessToken)
    )

  protected def request(method: Method, uri: Uri, maybeAccessToken: Option[AccessToken]): Request[Interpretation] =
    maybeAccessToken match {
      case Some(accessToken) => request(method, uri, accessToken)
      case _                 => request(method, uri)
    }

  protected def secureRequest(method: Method, uri: Uri)(implicit
      maybeAccessToken:               Option[AccessToken]
  ): Request[Interpretation] = request(method, uri, maybeAccessToken)

  protected def request(method: Method, uri: Uri, basicAuth: BasicAuthCredentials): Request[Interpretation] =
    Request[Interpretation](
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

  protected def send[ResultType](request: Request[Interpretation])(
      mapResponse:                        ResponseMapping[ResultType]
  ): Interpretation[ResultType] =
    send(HttpRequest(request))(mapResponse)

  protected def send[ResultType](
      request:   HttpRequest[Interpretation]
  )(mapResponse: ResponseMapping[ResultType]): Interpretation[ResultType] =
    httpClientBuilder.resource.use { httpClient =>
      for {
        _          <- throttler.acquire()
        callResult <- measureExecutionTime(callRemote(httpClient, request, mapResponse, attempt = 1), request)
        _          <- throttler.release()
      } yield callResult
    }

  private def httpClientBuilder: BlazeClientBuilder[Interpretation] = {
    val clientBuilder      = BlazeClientBuilder[Interpretation](executionContext)
    val updatedIdleTimeout = idleTimeoutOverride map clientBuilder.withIdleTimeout getOrElse clientBuilder
    requestTimeoutOverride map updatedIdleTimeout.withRequestTimeout getOrElse updatedIdleTimeout
  }

  private def measureExecutionTime[ResultType](block:   => Interpretation[ResultType],
                                               request: HttpRequest[Interpretation]
  ): Interpretation[ResultType] =
    maybeTimeRecorder match {
      case None => block
      case Some(timeRecorder) =>
        timeRecorder
          .measureExecutionTime(block, request.toHistogramLabel)
          .map(timeRecorder.logExecutionTime(withMessage = LogMessage(request, "finished")))
    }

  private def callRemote[ResultType](httpClient:  Client[Interpretation],
                                     request:     HttpRequest[Interpretation],
                                     mapResponse: ResponseMapping[ResultType],
                                     attempt:     Int
  ): Interpretation[ResultType] =
    httpClient
      .run(request.request)
      .use(response => processResponse(request.request, mapResponse)(response))
      .recoverWith(connectionError(httpClient, request, mapResponse, attempt))

  private def processResponse[ResultType](request: Request[Interpretation], mapResponse: ResponseMapping[ResultType])(
      response:                                    Response[Interpretation]
  ): Interpretation[ResultType] =
    (mapResponse orElse raiseBadRequest orElse raiseUnexpectedResponse)((response.status, request, response))
      .recoverWith(mappingError(request, response))

  private def raiseBadRequest[T]
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[T]] = {
    case (_, request, response) if response.status == BadRequest =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          MonadThrow[Interpretation].raiseError(BadRequestException(LogMessage(request, response, bodyAsString)))
        }
  }

  private def raiseUnexpectedResponse[T]
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[T]] = {
    case (_, request, response) =>
      response
        .as[String]
        .flatMap { bodyAsString =>
          MonadThrow[Interpretation].raiseError(
            UnexpectedResponseException(response.status, LogMessage(request, response, bodyAsString))
          )
        }
  }

  private def mappingError[T](request:  Request[Interpretation],
                              response: Response[Interpretation]
  ): PartialFunction[Throwable, Interpretation[T]] = {
    case error: RestClientError => MonadThrow[Interpretation].raiseError(error)
    case NonFatal(cause: InvalidMessageBodyFailure) =>
      val exception = new Exception(List(cause, cause.getCause()).map(_.getMessage()).mkString("; "), cause)
      MonadThrow[Interpretation].raiseError(MappingException(LogMessage(request, response, exception), exception))
    case NonFatal(cause) =>
      MonadThrow[Interpretation].raiseError(MappingException(LogMessage(request, response, cause), cause))
  }

  private def connectionError[T](httpClient:  Client[Interpretation],
                                 request:     HttpRequest[Interpretation],
                                 mapResponse: ResponseMapping[T],
                                 attempt:     Int
  ): PartialFunction[Throwable, Interpretation[T]] = {
    case error: RestClientError => throttler.release() >> error.raiseError[Interpretation, T]
    case NonFatal(cause) =>
      cause match {
        case exception @ (_: ConnectionFailure | _: ConnectException | _: Command.EOF.type)
            if attempt <= maxRetries.value =>
          for {
            _      <- logger.warn(LogMessage(request.request, s"timed out -> retrying attempt $attempt", exception))
            _      <- Timer[Interpretation] sleep retryInterval
            result <- callRemote(httpClient, request, mapResponse, attempt + 1)
          } yield result
        case exception @ (_: ConnectionFailure | _: ConnectException | _: Command.EOF.type)
            if attempt > maxRetries.value =>
          throttler.release() >> ConnectivityException(LogMessage(request.request, exception), exception)
            .raiseError[Interpretation, T]
        case other =>
          throttler.release() >> ClientException(LogMessage(request.request, other), other)
            .raiseError[Interpretation, T]
      }
  }

  private type ResponseMapping[ResultType] =
    PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[ResultType]]

  private implicit class HttpRequestOps(request: HttpRequest[Interpretation]) {
    lazy val toHistogramLabel: Option[String Refined NonEmpty] = request match {
      case UnnamedRequest(_)     => None
      case NamedRequest(_, name) => Some(name)
    }
  }

  protected object LogMessage {

    def apply(request: HttpRequest[Interpretation], message: String): String =
      request match {
        case UnnamedRequest(request) => s"${request.method} ${request.uri} $message"
        case NamedRequest(_, name)   => s"$name $message"
      }

    def apply(request: Request[Interpretation], message: String, cause: Throwable): String =
      s"${request.method} ${request.uri} $message error: ${toSingleLine(cause)}"

    def apply(request: Request[Interpretation], cause: Throwable): String =
      s"${request.method} ${request.uri} error: ${toSingleLine(cause)}"

    def apply(request: Request[Interpretation], response: Response[Interpretation], responseBody: String): String =
      s"${request.method} ${request.uri} returned ${response.status}; body: ${toSingleLine(responseBody)}"

    def apply(request: Request[Interpretation], response: Response[Interpretation], cause: Throwable): String =
      s"${request.method} ${request.uri} returned ${response.status}; error: ${toSingleLine(cause)}"

    def toSingleLine(exception: Throwable): String =
      Option(exception.getMessage) map toSingleLine getOrElse exception.getClass.getSimpleName

    def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }

  implicit class RequestOps(request: Request[Interpretation]) {
    lazy val withMultipartBuilder: MultipartBuilder = new MultipartBuilder(request)

    def withParts(parts: Vector[Part[Interpretation]]): Request[Interpretation] =
      new MultipartBuilder(request, parts).build()

    class MultipartBuilder private[RequestOps] (request: Request[Interpretation],
                                                parts:   Vector[Part[Interpretation]] = Vector.empty[Part[Interpretation]]
    ) {
      def addPart[PartType](name: String, value: PartType)(implicit
          encoder:                PartEncoder[PartType]
      ): MultipartBuilder =
        new MultipartBuilder(request, encoder.encode[Interpretation](name, value) +: parts)

      def maybeAddPart[PartType](name: String, maybeValue: Option[PartType])(implicit
          encoder:                     PartEncoder[PartType]
      ): MultipartBuilder = maybeValue
        .map(addPart(name, _))
        .getOrElse(this)

      def build(): Request[Interpretation] = {
        val multipart = Multipart[Interpretation](parts)
        request
          .withEntity(multipart)
          .withHeaders(multipart.headers.filterNot(_.name == CaseInsensitiveString("transfer-encoding")))
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

  def validateUri[Interpretation[_]: MonadThrow](uri: String): Interpretation[Uri] =
    MonadThrow[Interpretation].fromEither(Uri.fromString(uri))

  trait PartEncoder[-PartType] {
    def encode[Interpretation[_]](name: String, value: PartType): Part[Interpretation]
  }

  implicit object JsonPartEncoder extends PartEncoder[Json] {

    override def encode[Interpretation[_]](name: String, value: Json): Part[Interpretation] = Part
      .formData[Interpretation](name, encodeValue(value), contentType)

    private def encodeValue(value: Json): String = value.noSpaces

    private val contentType: `Content-Type` = `Content-Type`(MediaType.application.json)
  }

  implicit object StringPartEncoder extends PartEncoder[String] {

    override def encode[Interpretation[_]](name: String, value: String): Part[Interpretation] = Part
      .formData[Interpretation](name, encodeValue(value), contentType)

    private def encodeValue(value: String): String = value

    private val contentType: `Content-Type` = `Content-Type`(MediaType.text.plain)
  }

  implicit object ZipPartEncoder extends PartEncoder[ByteArrayTinyType with ZippedContent] {

    override def encode[Interpretation[_]](name:  String,
                                           value: ByteArrayTinyType with ZippedContent
    ): Part[Interpretation] = Part(
      Headers(
        `Content-Disposition`("form-data", Map("name" -> name)) ::
          Header("Content-Transfer-Encoding", "binary") ::
          `Content-Type`(MediaType.application.zip) :: Nil
      ),
      body = Stream.emits(value.value)
    )
  }
}
