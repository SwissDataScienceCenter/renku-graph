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
import cats.effect.Effect
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventStatus
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class SubscriptionsEndpoint[Interpretation[_]: Effect](
    subscriptions: Subscriptions[Interpretation],
    logger:        Logger[Interpretation]
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import EventStatus.{New, RecoverableFailure}
  import SubscriptionsEndpoint._
  import cats.syntax.all._
  import ch.datascience.controllers.InfoMessage._
  import ch.datascience.controllers.{ErrorMessage, InfoMessage}
  import org.http4s.circe._
  import org.http4s.{EntityDecoder, Request, Response}

  def addSubscription(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      urlAndStatuses <- request.as[UrlAndStatuses] recoverWith badRequest
      (subscriberUrl, statuses) = urlAndStatuses
      _        <- badRequestIf(statuses, not = New, RecoverableFailure)
      _        <- subscriptions add subscriberUrl
      response <- Accepted(InfoMessage("Subscription added"))
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[UrlAndStatuses]] = {
    case NonFatal(exception) => ME.raiseError(BadRequestError(exception))
  }

  private def badRequestIf(statuses: Set[EventStatus], not: EventStatus*): Interpretation[Unit] =
    if (statuses != not.toSet) ME.raiseError {
      BadRequestError(s"Subscriptions to $New and $RecoverableFailure status supported only")
    }
    else ME.unit

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case exception: BadRequestError =>
      BadRequest {
        Option(exception.getCause) map ErrorMessage.apply getOrElse ErrorMessage(exception.getMessage)
      }
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Adding subscriber URL failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit lazy val eventEntityDecoder: EntityDecoder[Interpretation, UrlAndStatuses] =
    jsonOf[Interpretation, UrlAndStatuses]
}

object SubscriptionsEndpoint {
  import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
  import io.circe.Decoder

  sealed trait BadRequestError extends Throwable

  object BadRequestError {
    def apply(message: String):    BadRequestError = new Exception(message) with BadRequestError
    def apply(cause:   Throwable): BadRequestError = new Exception(cause) with BadRequestError
  }

  type UrlAndStatuses = (SubscriberUrl, Set[EventStatus])

  implicit val payloadDecoder: Decoder[UrlAndStatuses] = cursor =>
    for {
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl](stringDecoder(SubscriberUrl))
      statuses      <- cursor.downField("statuses").as[List[EventStatus]]
    } yield subscriberUrl -> statuses.toSet
}

object IOSubscriptionsEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      subscriptions:       Subscriptions[IO],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[SubscriptionsEndpoint[IO]] = IO {
    new SubscriptionsEndpoint[IO](subscriptions, logger)
  }
}
