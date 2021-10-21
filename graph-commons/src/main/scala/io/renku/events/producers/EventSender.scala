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
import scala.language.postfixOps

trait EventSender[Interpretation[_]] {
  def sendEvent(eventContent: EventRequestContent.NoPayload, errorMessage: String): Interpretation[Unit]

  def sendEvent[PayloadType](eventContent: EventRequestContent.WithPayload[PayloadType], errorMessage: String)(implicit
      partEncoder:                         RestClient.PartEncoder[PayloadType]
  ): Interpretation[Unit]
}

class EventSenderImpl[Interpretation[_]: Async: Logger](
    eventLogUrl:            EventLogUrl,
    onErrorSleep:           FiniteDuration,
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient[Interpretation, Any](Throttler.noThrottling,
                                          retryInterval = retryInterval,
                                          maxRetries = maxRetries,
                                          requestTimeoutOverride = requestTimeoutOverride
    )
    with EventSender[Interpretation] {

  override def sendEvent(eventContent: EventRequestContent.NoPayload, errorMessage: String): Interpretation[Unit] =
    for {
      uri <- validateUri(s"$eventLogUrl/events")
      request = createRequest(uri, eventContent)
      sendingResult <-
        send(request)(responseMapping)
          .recoverWith(retryOnServerError(Eval.always(sendEvent(eventContent, errorMessage)), errorMessage))
    } yield sendingResult

  override def sendEvent[PayloadType](eventContent: EventRequestContent.WithPayload[PayloadType], errorMessage: String)(
      implicit partEncoder:                         RestClient.PartEncoder[PayloadType]
  ): Interpretation[Unit] = for {
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
      retry:        Eval[Interpretation[Unit]],
      errorMessage: String
  ): PartialFunction[Throwable, Interpretation[Unit]] = {
    case exception @ UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, _) =>
      waitAndRetry(retry, exception, errorMessage)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(retry, exception, errorMessage)
  }

  private def waitAndRetry(retry: Eval[Interpretation[Unit]], exception: Throwable, errorMessage: String) = for {
    _      <- Logger[Interpretation].error(exception)(errorMessage)
    _      <- Temporal[Interpretation] sleep onErrorSleep
    result <- retry.value
  } yield result

  private lazy val responseMapping
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted | NotFound, _, _) => ().pure[Interpretation]
  }
}

object EventSender {
  def apply[Interpretation[_]: Async: Logger]: Interpretation[EventSender[Interpretation]] = for {
    eventLogUrl <- EventLogUrl[Interpretation]()
  } yield new EventSenderImpl(eventLogUrl, onErrorSleep = 15 seconds)
}
