/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.Concurrent
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.data.Message
import io.renku.events.EventRequestContent
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}

import scala.util.control.NonFatal

trait EventEndpoint[F[_]] {
  def processEvent(request: Request[F]): F[Response[F]]
}

class EventEndpointImpl[F[_]: Concurrent](eventConsumersRegistry: EventConsumersRegistry[F])
    extends Http4sDsl[F]
    with EventEndpoint[F] {

  implicit val textDec: org.http4s.EntityDecoder[F, String] = org.http4s.EntityDecoder.text[F]

  def processEvent(request: Request[F]): F[Response[F]] = {
    for {
      multipart    <- toMultipart(request)
      eventJson    <- toEvent(multipart)
      maybePayload <- getPayload(multipart)
      eventRequest = maybePayload match {
                       case Some(payload) => EventRequestContent.WithPayload(eventJson, payload)
                       case None          => EventRequestContent.NoPayload(eventJson)
                     }
      result <- right[Response[F]](
                  eventConsumersRegistry.handle(eventRequest) >>= toHttpResult
                )
    } yield result
  }.merge recoverWith { case NonFatal(error) =>
    toHttpResult(EventSchedulingResult.SchedulingError(error))
  }

  private lazy val toHttpResult: EventSchedulingResult => F[Response[F]] = {
    case EventSchedulingResult.Accepted                   => Accepted(Message.Info("Event accepted"))
    case EventSchedulingResult.Busy                       => TooManyRequests(Message.Info("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType       => BadRequest(Message.Error("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest(reason)         => BadRequest(Message.Error.unsafeApply(reason))
    case EventSchedulingResult.ServiceUnavailable(reason) => ServiceUnavailable(Message.Error.unsafeApply(reason))
    case EventSchedulingResult.SchedulingError(_) => InternalServerError(Message.Error("Failed to schedule event"))
  }

  private def toMultipart(request: Request[F]): EitherT[F, Response[F], Multipart[F]] = EitherT {
    request
      .as[Multipart[F]]
      .map(_.asRight[Response[F]])
      .recoverWith { case NonFatal(_) =>
        BadRequest(Message.Error("Not multipart request")).map(_.asLeft[Multipart[F]])
      }
  }

  private def toEvent(multipart: Multipart[F]): EitherT[F, Response[F], Json] = EitherT {
    multipart.parts
      .find(_.name.contains("event"))
      .map(_.as[Json].map(_.asRight[Response[F]]).recoverWith { case NonFatal(_) =>
        BadRequest(Message.Error("Malformed event body")).map(_.asLeft[Json])
      })
      .getOrElse(BadRequest(Message.Error("Missing event part")).map(_.asLeft[Json]))
  }

  private def getPayload(multipart: Multipart[F]): EitherT[F, Response[F], Option[String]] = EitherT {
    multipart.parts
      .find(_.name.contains("payload"))
      .map {
        _.as[String].map(_.some.asRight[Response[F]]).recoverWith { case NonFatal(_) =>
          BadRequest(Message.Error("Malformed event payload")).map(_.asLeft[Option[String]])
        }
      }
      .getOrElse(Option.empty[String].asRight[Response[F]].pure[F])
  }
}

object EventEndpoint {
  def apply[F[_]: MonadThrow: Concurrent](
      subscriptionsRegistry: EventConsumersRegistry[F]
  ): F[EventEndpoint[F]] = MonadThrow[F].catchNonFatal {
    new EventEndpointImpl[F](subscriptionsRegistry)
  }
}
