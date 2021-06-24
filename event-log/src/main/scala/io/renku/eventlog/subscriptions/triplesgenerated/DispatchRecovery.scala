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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.Eval
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{TransformationNonRecoverableFailure, TriplesGenerated}
import ch.datascience.http.client.RestClient
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.circe.literal._
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.{EventMessage, subscriptions}
import org.http4s.Method.POST
import org.http4s.Status.{BadGateway, GatewayTimeout, NotFound, Ok, ServiceUnavailable}
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private class DispatchRecoveryImpl[Interpretation[_]: ConcurrentEffect: Timer](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation],
    onErrorSleep:            FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, Any](Throttler.noThrottling, logger)
    with subscriptions.DispatchRecovery[Interpretation, TriplesGeneratedEvent] {

  override def returnToQueue(event: TriplesGeneratedEvent): Interpretation[Unit] = sendEventStatusChange(
    EventRequestContent(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id.value},
        "project": {
          "id":   ${event.id.projectId.value},
          "path": ${event.projectPath.value}
        },
        "newStatus": ${TriplesGenerated.value}
      }"""),
    TriplesGenerated
  )

  override def recover(
      url:   SubscriberUrl,
      event: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    sendEventStatusChange(
      EventRequestContent(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id.value},
        "project": {
          "id":   ${event.id.projectId.value},
          "path": ${event.projectPath.value}
        },
        "newStatus": ${TransformationNonRecoverableFailure.value},
        "message":   ${EventMessage(exception).value}
      }"""),
      TransformationNonRecoverableFailure
    ) >> logger.error(exception)(
      s"${SubscriptionCategory.name}: $event, url = $url -> $TransformationNonRecoverableFailure"
    )
  }

  private def sendEventStatusChange(eventContent: EventRequestContent, newStatus: EventStatus): Interpretation[Unit] =
    for {
      uri <- validateUri(s"$eventLogUrl/events")
      request = createRequest(uri, eventContent)
      sendingResult <-
        send(request)(responseMapping)
          .recoverWith(retryOnServerError(Eval.always(sendEventStatusChange(eventContent, newStatus)), newStatus))
    } yield sendingResult

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent) =
    request(POST, uri).withMultipartBuilder
      .addPart("event", eventRequestContent.event)
      .maybeAddPart("payload", eventRequestContent.maybePayload)
      .build()

  private def retryOnServerError(retry:     Eval[Interpretation[Unit]],
                                 newStatus: EventStatus
  ): PartialFunction[Throwable, Interpretation[Unit]] = {
    case exception @ UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, _) =>
      waitAndRetry(retry, exception, newStatus)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(retry, exception, newStatus)
  }

  private def waitAndRetry(retry: Eval[Interpretation[Unit]], exception: Throwable, newStatus: EventStatus) = for {
    _      <- logger.error(exception)(s"${SubscriptionCategory.name}: Marking event as $newStatus failed")
    _      <- Timer[Interpretation] sleep onErrorSleep
    result <- retry.value
  } yield result

  private lazy val responseMapping
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Ok, _, _)       => ().pure[Interpretation]
    case (NotFound, _, _) => ().pure[Interpretation]
  }
}

private object DispatchRecovery {

  private val OnErrorSleep: FiniteDuration = 1 seconds

  def apply(logger:     Logger[IO])(implicit
      effect:           ConcurrentEffect[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[DispatchRecovery[IO, TriplesGeneratedEvent]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new DispatchRecoveryImpl[IO](eventLogUrl, logger, OnErrorSleep)
}
