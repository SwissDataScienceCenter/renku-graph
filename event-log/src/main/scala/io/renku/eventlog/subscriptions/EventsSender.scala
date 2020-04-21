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

package io.renku.eventlog.subscriptions

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.RestClientError.ConnectivityException
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.subscriptions.EventsSender.SendingResult

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait EventsSender[Interpretation[_]] {
  def sendEvent(subscriptionUrl: SubscriptionUrl, id: CompoundEventId, body: EventBody): Interpretation[SendingResult]
}

private object EventsSender {
  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    case object Delivered    extends SendingResult
    case object ServiceBusy  extends SendingResult
    case object Misdelivered extends SendingResult
  }
}

private class IOEventsSender(
    logger:         Logger[IO]
)(implicit ME:      MonadError[IO, Throwable],
  executionContext: ExecutionContext,
  contextShift:     ContextShift[IO],
  timer:            Timer[IO])
    extends IORestClient(Throttler.noThrottling, logger)
    with EventsSender[IO] {

  import SendingResult._
  import cats.effect._
  import cats.implicits._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.circe._
  import org.http4s.{Request, Response, Status}

  def sendEvent(subscriptionUrl: SubscriptionUrl, id: CompoundEventId, body: EventBody): IO[SendingResult] = {
    for {
      uri           <- validateUri(subscriptionUrl.value)
      sendingResult <- send(request(POST, uri).withEntity((id -> body).asJson))(mapResponse)
    } yield sendingResult
  } recoverWith connectivityException(to = Misdelivered)

  private implicit lazy val entityEncoder: Encoder[(CompoundEventId, EventBody)] =
    Encoder.instance[(CompoundEventId, EventBody)] {
      case (id, body) => json"""{
        "id":      ${id.id.value},
        "project": {
          "id":    ${id.projectId.value}
        },
        "body":    ${body.value}
      }"""
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[SendingResult]] = {
    case (Accepted, _, _)           => Delivered.pure[IO]
    case (TooManyRequests, _, _)    => ServiceBusy.pure[IO]
    case (NotFound, _, _)           => Misdelivered.pure[IO]
    case (BadGateway, _, _)         => Misdelivered.pure[IO]
    case (ServiceUnavailable, _, _) => Misdelivered.pure[IO]
  }

  private def connectivityException(to: SendingResult): PartialFunction[Throwable, IO[SendingResult]] = {
    case _: ConnectivityException => to.pure[IO]
  }
}

private object IOEventsSender {
  def apply(
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[EventsSender[IO]] = IO {
    new IOEventsSender(logger)
  }
}
