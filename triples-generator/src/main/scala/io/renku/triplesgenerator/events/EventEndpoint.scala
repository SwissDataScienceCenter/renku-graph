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
import cats.effect.{Effect, Timer}
import ch.datascience.events.EventRequestContent
import ch.datascience.events.EventRequestContent.WithPayload
import ch.datascience.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import ch.datascience.graph.model.events.ZippedEventPayload
import ch.datascience.http.ErrorMessage
import io.circe.Json
import io.renku.triplesgenerator.reprovisioning.ReProvisioningStatus
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{Request, Response}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait EventEndpoint[Interpretation[_]] {
  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class EventEndpointImpl[Interpretation[_]: Effect: MonadThrow](
    eventConsumersRegistry: EventConsumersRegistry[Interpretation],
    reProvisioningStatus:   ReProvisioningStatus[Interpretation]
) extends Http4sDsl[Interpretation]
    with EventEndpoint[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s._

  override def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] =
    reProvisioningStatus.isReProvisioning() >>= { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else {
        for {
          multipart      <- toMultipart(request)
          eventJson      <- toEvent(multipart)
          requestContent <- getRequestContent(multipart, eventJson)
          result         <- right[Response[Interpretation]](eventConsumersRegistry.handle(requestContent) >>= toHttpResult)
        } yield result
      }.merge recoverWith { case NonFatal(error) =>
        toHttpResult(EventSchedulingResult.SchedulingError(error))
      }
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

  private def getRequestContent(
      multipart: Multipart[Interpretation],
      eventJson: Json
  ): EitherT[Interpretation, Response[Interpretation], EventRequestContent] = EitherT {
    import EventRequestContent._
    multipart.parts
      .find(_.name.contains("payload"))
      .map { part =>
        part.headers
          .find(_.name == `Content-Type`.name)
          .map(toEventRequestContent(part, eventJson))
          .getOrElse(
            BadRequest(ErrorMessage("Content-type not provided for payload")).map(_.asLeft[EventRequestContent])
          )
      }
      .getOrElse(NoPayload(eventJson).asRight[Response[Interpretation]].widen[EventRequestContent].pure[Interpretation])
  }

  private def toEventRequestContent(part:      Part[Interpretation],
                                    eventJson: Json
  ): Header => Interpretation[Either[Response[Interpretation], EventRequestContent]] = {
    case header if header.value == `Content-Type`(MediaType.application.zip).value =>
      part
        .as[Array[Byte]]
        .map(ZippedEventPayload)
        .map(
          WithPayload[ZippedEventPayload](eventJson, _)
            .asRight[Response[Interpretation]]
            .widen[EventRequestContent]
        )
    case header if header.value == `Content-Type`(MediaType.text.plain).value =>
      part
        .as[String]
        .map(WithPayload[String](eventJson, _).asRight[Response[Interpretation]].widen[EventRequestContent])
    case _ =>
      BadRequest(ErrorMessage("Event payload type unsupported")).map(_.asLeft[EventRequestContent])
  }

  private lazy val toHttpResult: EventSchedulingResult => Interpretation[Response[Interpretation]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
  }
}

object IOEventEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      eventConsumersRegistry: EventConsumersRegistry[IO],
      reProvisioningStatus:   ReProvisioningStatus[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventEndpoint[IO]] = IO(new EventEndpointImpl[IO](eventConsumersRegistry, reProvisioningStatus))
}
