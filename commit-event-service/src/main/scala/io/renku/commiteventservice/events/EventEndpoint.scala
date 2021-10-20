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

package io.renku.commiteventservice.events

import cats.MonadThrow
import cats.data.EitherT
import cats.data.EitherT.right
import cats.effect.kernel.Concurrent
import cats.syntax.all._
import io.circe.Json
import io.renku.events.EventRequestContent
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import io.renku.http.InfoMessage._
import io.renku.http.{ErrorMessage, InfoMessage}
import org.http4s.dsl.Http4sDsl
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}

import scala.util.control.NonFatal

trait EventEndpoint[Interpretation[_]] {
  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class EventEndpointImpl[Interpretation[_]: Concurrent](
    eventConsumersRegistry: EventConsumersRegistry[Interpretation]
) extends Http4sDsl[Interpretation]
    with EventEndpoint[Interpretation] {

  import org.http4s.circe._

  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      multipart    <- toMultipart(request)
      eventJson    <- toEvent(multipart)
      maybePayload <- getPayload(multipart)
      eventRequest = maybePayload match {
                       case Some(payload) => EventRequestContent.WithPayload(eventJson, payload)
                       case None          => EventRequestContent.NoPayload(eventJson)
                     }
      result <- right[Response[Interpretation]](
                  eventConsumersRegistry.handle(eventRequest) >>= toHttpResult
                )
    } yield result
  }.merge recoverWith { case NonFatal(error) =>
    toHttpResult(EventSchedulingResult.SchedulingError(error))
  }

  private lazy val toHttpResult: EventSchedulingResult => Interpretation[Response[Interpretation]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
  }

  private def toMultipart(
      request: Request[Interpretation]
  ): EitherT[Interpretation, Response[Interpretation], Multipart[Interpretation]] = EitherT {
    request
      .as[Multipart[Interpretation]]
      .map(_.asRight[Response[Interpretation]])
      .recoverWith { case NonFatal(_) =>
        BadRequest(ErrorMessage("Not multipart request")).map(_.asLeft[Multipart[Interpretation]])
      }
  }

  private def toEvent(multipart: Multipart[Interpretation]): EitherT[Interpretation, Response[Interpretation], Json] =
    EitherT {
      multipart.parts
        .find(_.name.contains("event"))
        .map(_.as[Json].map(_.asRight[Response[Interpretation]]).recoverWith { case NonFatal(_) =>
          BadRequest(ErrorMessage("Malformed event body")).map(_.asLeft[Json])
        })
        .getOrElse(BadRequest(ErrorMessage("Missing event part")).map(_.asLeft[Json]))
    }

  private def getPayload(
      multipart: Multipart[Interpretation]
  ): EitherT[Interpretation, Response[Interpretation], Option[String]] = EitherT {
    multipart.parts
      .find(_.name.contains("payload"))
      .map {
        _.as[String].map(_.some.asRight[Response[Interpretation]]).recoverWith { case NonFatal(_) =>
          BadRequest(ErrorMessage("Malformed event payload")).map(_.asLeft[Option[String]])
        }
      }
      .getOrElse(Option.empty[String].asRight[Response[Interpretation]].pure[Interpretation])
  }
}

object EventEndpoint {
  def apply[Interpretation[_]: MonadThrow: Concurrent](
      subscriptionsRegistry: EventConsumersRegistry[Interpretation]
  ): Interpretation[EventEndpoint[Interpretation]] = MonadThrow[Interpretation].catchNonFatal {
    new EventEndpointImpl[Interpretation](subscriptionsRegistry)
  }
}
