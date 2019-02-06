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

package ch.datascience.clients

import cats.effect.{ConcurrentEffect, ContextShift, IO}
import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext

abstract class IORestClient(
    implicit executionContext: ExecutionContext
) {

  private implicit val cs: ContextShift[IO]     = IO.contextShift(executionContext)
  protected val F:         ConcurrentEffect[IO] = implicitly[ConcurrentEffect[IO]]

  protected def validateUri(uri: String): IO[Uri] =
    F.fromEither(Uri.fromString(uri))

  protected def request(method: Method, uri: Uri, accessToken: AccessToken): Request[IO] =
    Request[IO](
      method  = method,
      uri     = uri,
      headers = authHeader(accessToken)
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers(Authorization(Token(Bearer, token)))
  }

  protected def send[ResultType](request: Request[IO])(
      mapResponse:                        (Request[IO], Response[IO]) => IO[ResultType]): IO[ResultType] =
    BlazeClientBuilder[IO](executionContext).resource.use { httpClient =>
      httpClient.fetch[ResultType](request)(mapResponse(request, _))
    }

  protected def raiseError[T](request: Request[IO], response: Response[IO]): IO[T] =
    response
      .as[String]
      .flatMap { bodyAsString =>
        F.raiseError {
          new RuntimeException(
            exceptionMessage(request, response, MessageDetails(bodyAsString)),
          )
        }
      }

  protected def contextToError[T](request: Request[IO], response: Response[IO])(cause: Throwable): IO[T] =
    F.raiseError {
      new RuntimeException(
        exceptionMessage(request, response, MessageDetails(cause)),
        cause
      )
    }

  private sealed trait MessageDetails
  private object MessageDetails {

    def apply(cause: Throwable): MessageDetails = ExceptionMessage(cause.getMessage)
    def apply(body:  String):    MessageDetails = ResponseBody(body)

    final case class ExceptionMessage(message: String) extends MessageDetails
    final case class ResponseBody(message:     String) extends MessageDetails
  }

  private def exceptionMessage(request: Request[IO], response: Response[IO], messageDetails: MessageDetails): String = {
    import MessageDetails._

    def toSingleLine(string: String): String = string.split('\n').map(_.trim.filter(_ >= ' ')).mkString

    val details = messageDetails match {
      case ExceptionMessage(message) => s"error: ${toSingleLine(message)}"
      case ResponseBody(message)     => s"body: ${toSingleLine(message)}"
    }

    s"${request.method} ${request.uri} returned ${response.status}; $details"
  }
}
