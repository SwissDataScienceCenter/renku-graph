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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.RestClientError.ConnectivityException
import org.typelevel.log4cats.Logger
import io.renku.eventlog.subscriptions.EventsSender.SendingResult

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

private trait EventsSender[Interpretation[_], CategoryEvent] {
  def sendEvent(subscriptionUrl: SubscriberUrl, categoryEvent: CategoryEvent): Interpretation[SendingResult]
}

private object EventsSender {
  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    case object Delivered              extends SendingResult
    case object TemporarilyUnavailable extends SendingResult
    case object Misdelivered           extends SendingResult
  }
}

private class EventsSenderImpl[CategoryEvent](
    categoryName:         CategoryName,
    categoryEventEncoder: EventEncoder[CategoryEvent],
    logger:               Logger[IO],
    retryInterval:        FiniteDuration = 1 second,
    idleTimeoutOverride:  Option[Duration] = None
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling,
                       logger,
                       retryInterval = retryInterval,
                       idleTimeoutOverride = idleTimeoutOverride
    )
    with EventsSender[IO, CategoryEvent] {

  import SendingResult._
  import cats.effect._
  import cats.syntax.all._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.{Request, Response, Status}

  def sendEvent(subscriberUrl: SubscriberUrl, categoryEvent: CategoryEvent): IO[SendingResult] = {
    for {
      uri <- validateUri(subscriberUrl.value)
      sendingResult <-
        send(
          request(POST, uri).withMultipartBuilder
            .addPart("event", categoryEventEncoder.encodeEvent(categoryEvent))
            .maybeAddPart("payload", categoryEventEncoder.encodePayload(categoryEvent))
            .build()
        )(mapResponse)
    } yield sendingResult
  } recoverWith exceptionToSendingResult

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[SendingResult]] = {
    case (Accepted, _, _)           => Delivered.pure[IO]
    case (TooManyRequests, _, _)    => TemporarilyUnavailable.pure[IO]
    case (ServiceUnavailable, _, _) => TemporarilyUnavailable.pure[IO]
    case (NotFound, _, _)           => TemporarilyUnavailable.pure[IO] // to mitigate k8s problems
    case (BadGateway, _, _)         => TemporarilyUnavailable.pure[IO] // to mitigate k8s problems
  }

  private lazy val exceptionToSendingResult: PartialFunction[Throwable, IO[SendingResult]] = {
    case _: ConnectivityException => Misdelivered.pure[IO]
    case cause =>
      logger.error(cause)(s"$categoryName: sending event failed") >> TemporarilyUnavailable.pure[IO]
  }
}

private object IOEventsSender {
  def apply[CategoryEvent](
      categoryName:         CategoryName,
      categoryEventEncoder: EventEncoder[CategoryEvent],
      logger:               Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventsSender[IO, CategoryEvent]] = IO {
    new EventsSenderImpl(categoryName, categoryEventEncoder, logger)
  }
}
