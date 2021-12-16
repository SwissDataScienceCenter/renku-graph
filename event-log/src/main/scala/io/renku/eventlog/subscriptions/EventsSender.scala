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

package io.renku.eventlog.subscriptions

import cats.effect.Async
import cats.{MonadThrow, Show}
import io.renku.control.Throttler
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.events.CategoryName
import io.renku.http.client.RestClient
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait EventsSender[F[_], CategoryEvent] {
  def sendEvent(subscriptionUrl: SubscriberUrl, categoryEvent: CategoryEvent): F[SendingResult]
}

private class EventsSenderImpl[F[_]: Async: Logger, CategoryEvent](
    categoryName:           CategoryName,
    categoryEventEncoder:   EventEncoder[CategoryEvent],
    retryInterval:          FiniteDuration = 1 second,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient[F, EventsSender[F, CategoryEvent]](Throttler.noThrottling,
                                                        retryInterval = retryInterval,
                                                        requestTimeoutOverride = requestTimeoutOverride
    )
    with EventsSender[F, CategoryEvent] {

  import SendingResult._
  import cats.syntax.all._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.{Request, Response, Status}

  override def sendEvent(subscriberUrl: SubscriberUrl, event: CategoryEvent): F[SendingResult] = {
    for {
      uri <- validateUri(subscriberUrl.value)
      sendingResult <-
        send(request(POST, uri).withParts(categoryEventEncoder.encodeParts(event)))(mapResponse)
    } yield sendingResult
  } recoverWith exceptionToSendingResult(subscriberUrl, event)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[SendingResult]] = {
    case (Accepted, _, _)           => Delivered.pure[F].widen[SendingResult]
    case (TooManyRequests, _, _)    => TemporarilyUnavailable.pure[F].widen[SendingResult]
    case (ServiceUnavailable, _, _) => TemporarilyUnavailable.pure[F].widen[SendingResult]
    case (NotFound, _, _)           => TemporarilyUnavailable.pure[F].widen[SendingResult] // to mitigate k8s problems
    case (BadGateway, _, _)         => TemporarilyUnavailable.pure[F].widen[SendingResult] // to mitigate k8s problems
  }

  private def exceptionToSendingResult(subscriberUrl: SubscriberUrl,
                                       event:         CategoryEvent
  ): PartialFunction[Throwable, F[SendingResult]] = {
    case _:         ConnectivityException => Misdelivered.pure[F].widen[SendingResult]
    case exception: ClientException =>
      Logger[F].error(exception)(s"$categoryName: sending $event to $subscriberUrl failed") >> TemporarilyUnavailable
        .pure[F]
        .widen[SendingResult]
  }
}

private object EventsSender {
  def apply[F[_]: Async: Logger, CategoryEvent](
      categoryName:         CategoryName,
      categoryEventEncoder: EventEncoder[CategoryEvent]
  ): F[EventsSender[F, CategoryEvent]] = MonadThrow[F].catchNonFatal {
    new EventsSenderImpl(categoryName, categoryEventEncoder)
  }

  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    case object Delivered              extends SendingResult
    case object TemporarilyUnavailable extends SendingResult
    case object Misdelivered           extends SendingResult

    implicit lazy val show: Show[SendingResult] = Show.fromToString
  }
}
