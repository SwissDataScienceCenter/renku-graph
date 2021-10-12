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

package ch.datascience.events.producers

import cats.Eval
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.events.EventRequestContent
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.http.client.RestClient
import ch.datascience.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.http4s.Method.POST
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, NotFound, ServiceUnavailable}
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.language.postfixOps

trait EventSender[Interpretation[_]] {
  def sendEvent(eventContent: EventRequestContent, errorMessage: String): Interpretation[Unit]
}

class EventSenderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation],
    onErrorSleep:            FiniteDuration,
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, Any](Throttler.noThrottling,
                                            logger,
                                            retryInterval = retryInterval,
                                            maxRetries = maxRetries,
                                            requestTimeoutOverride = requestTimeoutOverride
    )
    with EventSender[Interpretation] {

  def sendEvent(eventContent: EventRequestContent, errorMessage: String): Interpretation[Unit] = {

    def retryOnServerError(retry: Eval[Interpretation[Unit]]): PartialFunction[Throwable, Interpretation[Unit]] = {
      case exception @ UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, _) =>
        waitAndRetry(retry, exception)
      case exception @ (_: ConnectivityException | _: ClientException) =>
        waitAndRetry(retry, exception)
    }

    def waitAndRetry(retry: Eval[Interpretation[Unit]], exception: Throwable) = for {
      _      <- logger.error(exception)(errorMessage)
      _      <- Timer[Interpretation] sleep onErrorSleep
      result <- retry.value
    } yield result

    for {
      uri <- validateUri(s"$eventLogUrl/events")
      request = createRequest(uri, eventContent)
      sendingResult <-
        send(request)(responseMapping)
          .recoverWith(retryOnServerError(Eval.always(sendEvent(eventContent, errorMessage))))
    } yield sendingResult

  }

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", eventRequestContent.event)
      .maybeAddPart("payload", eventRequestContent.maybePayload)
      .build()

  private lazy val responseMapping
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted | NotFound, _, _) => ().pure[Interpretation]
  }
}

object EventSender {
  def apply(
      logger:    Logger[IO]
  )(implicit ec: ExecutionContext, ce: ConcurrentEffect[IO], timer: Timer[IO]): IO[EventSender[IO]] =
    for {
      eventlogUrl <- EventLogUrl()
    } yield new EventSenderImpl(eventlogUrl, logger, onErrorSleep = 15 seconds)
}
