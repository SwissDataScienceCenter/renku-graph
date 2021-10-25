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

package io.renku.triplesgenerator.events

import cats.MonadThrow
import cats.data.EitherT
import cats.data.EitherT.right
import cats.effect.Async
import io.circe.Json
import io.renku.events.EventRequestContent
import io.renku.events.EventRequestContent.WithPayload
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.http.ErrorMessage
import io.renku.triplesgenerator.reprovisioning.ReProvisioningStatus
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{Request, Response}

import scala.util.control.NonFatal

trait EventEndpoint[F[_]] {
  def processEvent(request: Request[F]): F[Response[F]]
}

class EventEndpointImpl[F[_]: Async](
    eventConsumersRegistry: EventConsumersRegistry[F],
    reProvisioningStatus:   ReProvisioningStatus[F]
) extends Http4sDsl[F]
    with EventEndpoint[F] {

  import cats.syntax.all._
  import io.renku.http.InfoMessage
  import io.renku.http.InfoMessage._
  import org.http4s._

  override def processEvent(request: Request[F]): F[Response[F]] =
    reProvisioningStatus.isReProvisioning() >>= { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else {
        for {
          multipart      <- toMultipart(request)
          eventJson      <- toEvent(multipart)
          requestContent <- getRequestContent(multipart, eventJson)
          result         <- right[Response[F]](eventConsumersRegistry.handle(requestContent) >>= toHttpResult)
        } yield result
      }.merge recoverWith { case NonFatal(error) =>
        toHttpResult(EventSchedulingResult.SchedulingError(error))
      }
    }

  private def toMultipart(
      request: Request[F]
  ): EitherT[F, Response[F], Multipart[F]] = EitherT {
    request
      .as[Multipart[F]]
      .map(_.asRight[Response[F]])
      .recoverWith { case NonFatal(_) =>
        BadRequest(ErrorMessage("Not multipart request")).map(_.asLeft[Multipart[F]])
      }
  }

  private def toEvent(multipart: Multipart[F]): EitherT[F, Response[F], Json] =
    EitherT {
      multipart.parts
        .find(_.name.contains("event"))
        .map(_.as[Json].map(_.asRight[Response[F]]).recoverWith { case NonFatal(_) =>
          BadRequest(ErrorMessage("Malformed event body")).map(_.asLeft[Json])
        })
        .getOrElse(BadRequest(ErrorMessage("Missing event part")).map(_.asLeft[Json]))
    }

  private def getRequestContent(
      multipart: Multipart[F],
      eventJson: Json
  ): EitherT[F, Response[F], EventRequestContent] = EitherT {
    import EventRequestContent._
    multipart.parts
      .find(_.name.contains("payload"))
      .map { part =>
        part.headers
          .get[`Content-Type`]
          .map(toEventRequestContent(part, eventJson))
          .getOrElse(
            BadRequest(ErrorMessage("Content-type not provided for payload")).map(_.asLeft[EventRequestContent])
          )
      }
      .getOrElse(NoPayload(eventJson).asRight[Response[F]].widen[EventRequestContent].pure[F])
  }

  private def toEventRequestContent(part:      Part[F],
                                    eventJson: Json
  ): `Content-Type` => F[Either[Response[F], EventRequestContent]] = {
    case header if header == `Content-Type`(MediaType.application.zip) =>
      part
        .as[Array[Byte]]
        .map(ZippedEventPayload)
        .map(
          WithPayload[ZippedEventPayload](eventJson, _)
            .asRight[Response[F]]
            .widen[EventRequestContent]
        )
    case header if header == `Content-Type`(MediaType.text.plain) =>
      part
        .as[String]
        .map(WithPayload[String](eventJson, _).asRight[Response[F]].widen[EventRequestContent])
    case _ =>
      BadRequest(ErrorMessage("Event payload type unsupported")).map(_.asLeft[EventRequestContent])
  }

  private lazy val toHttpResult: EventSchedulingResult => F[Response[F]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
  }
}

object EventEndpoint {

  def apply[F[_]: Async](
      eventConsumersRegistry: EventConsumersRegistry[F],
      reProvisioningStatus:   ReProvisioningStatus[F]
  ): F[EventEndpoint[F]] =
    MonadThrow[F].catchNonFatal(new EventEndpointImpl[F](eventConsumersRegistry, reProvisioningStatus))
}
