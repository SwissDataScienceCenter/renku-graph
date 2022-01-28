/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.events.producers

import cats.Eval
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.events.EventRequestContent
import io.renku.graph.config.EventLogUrl
import io.renku.http.client.RestClient
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import org.http4s.Method.POST
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, NotFound, ServiceUnavailable}
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait EventSender[F[_]] {
  def sendEvent(eventContent: EventRequestContent.NoPayload, errorMessage: String): F[Unit]

  def sendEvent[PayloadType](eventContent: EventRequestContent.WithPayload[PayloadType], errorMessage: String)(implicit
      partEncoder:                         RestClient.PartEncoder[PayloadType]
  ): F[Unit]
}

class EventSenderImpl[F[_]: Async: Logger](
    eventLogUrl:            EventLogUrl,
    onErrorSleep:           FiniteDuration,
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient[F, Any](Throttler.noThrottling,
                             retryInterval = retryInterval,
                             maxRetries = maxRetries,
                             requestTimeoutOverride = requestTimeoutOverride
    )
    with EventSender[F] {

  override def sendEvent(eventContent: EventRequestContent.NoPayload, errorMessage: String): F[Unit] =
    for {
      uri <- validateUri(s"$eventLogUrl/events")
      request = createRequest(uri, eventContent)
      sendingResult <-
        send(request)(responseMapping)
          .recoverWith(retryOnServerError(Eval.always(sendEvent(eventContent, errorMessage)), errorMessage))
    } yield sendingResult

  override def sendEvent[PayloadType](eventContent: EventRequestContent.WithPayload[PayloadType], errorMessage: String)(
      implicit partEncoder:                         RestClient.PartEncoder[PayloadType]
  ): F[Unit] = for {
    uri <- validateUri(s"$eventLogUrl/events")
    request = createRequest(uri, eventContent)
    _ <- send(request)(responseMapping)
           .recoverWith(retryOnServerError(Eval.always(sendEvent(eventContent, errorMessage)), errorMessage))
  } yield ()

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent.NoPayload) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", eventRequestContent.event)
      .build()

  private def createRequest[PayloadType](uri: Uri, eventRequestContent: EventRequestContent.WithPayload[PayloadType])(
      implicit partEncoder:                   RestClient.PartEncoder[PayloadType]
  ) = request(POST, uri).withMultipartBuilder
    .addPart("event", eventRequestContent.event)
    .addPart("payload", eventRequestContent.payload)
    .build()

  private def retryOnServerError(
      retry:        Eval[F[Unit]],
      errorMessage: String
  ): PartialFunction[Throwable, F[Unit]] = {
    case exception @ UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, _) =>
      waitAndRetry(retry, exception, errorMessage)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(retry, exception, errorMessage)
  }

  private def waitAndRetry(retry: Eval[F[Unit]], exception: Throwable, errorMessage: String) = for {
    _      <- Logger[F].error(exception)(errorMessage)
    _      <- Temporal[F] sleep onErrorSleep
    result <- retry.value
  } yield result

  private lazy val responseMapping: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = {
    case (Accepted | NotFound, _, _) => ().pure[F]
  }
}

object EventSender {
  def apply[F[_]: Async: Logger]: F[EventSender[F]] = for {
    eventLogUrl <- EventLogUrl[F]()
  } yield new EventSenderImpl(eventLogUrl, onErrorSleep = 15 seconds)
}
