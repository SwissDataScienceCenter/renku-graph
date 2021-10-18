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

package io.renku.eventlog.events

import cats.effect.{ConcurrentEffect, Effect, IO}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.circe.{Encoder, Json}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.EventInfo
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import io.renku.metrics.LabeledHistogram
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventsEndpoint[Interpretation[_]] {
  def findEvents(request: EventsEndpoint.Request): Interpretation[Response[Interpretation]]
}

class EventsEndpointImpl[Interpretation[_]: Effect: MonadThrow: Logger](eventsFinder: EventsFinder[Interpretation],
                                                                        eventLogUrl: EventLogUrl
) extends Http4sDsl[Interpretation]
    with EventsEndpoint[Interpretation] {

  import io.renku.http.ErrorMessage._

  override def findEvents(request: EventsEndpoint.Request): Interpretation[Response[Interpretation]] =
    eventsFinder
      .findEvents(request)
      .map(_.toHttpResponse(Effect[Interpretation], requestedUrl(request), EventLogUrl, EventInfo.infoEncoder))
      .recoverWith(httpResponse(request))

  private def httpResponse(
      request: EventsEndpoint.Request
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    Logger[Interpretation].error(exception)(show"Finding events for project '$request' failed")
    InternalServerError(ErrorMessage(exception))
  }

  private def requestedUrl(request: EventsEndpoint.Request): EventLogUrl = request match {
    case EventsEndpoint.Request.ProjectEvents(path, Some(status), paging) =>
      (eventLogUrl / "events") ? ("project-path" -> path.show) & ("status" -> status) & (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage)
    case EventsEndpoint.Request.ProjectEvents(path, None, paging) =>
      (eventLogUrl / "events") ? ("project-path" -> path.show) & (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage)
    case EventsEndpoint.Request.EventsWithStatus(status, paging) =>
      (eventLogUrl / "events") ? ("status" -> status) & (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage)
  }
}

object EventsEndpoint {

  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name]
  )(implicit concurrentEffect: ConcurrentEffect[IO], logger: Logger[IO]): IO[EventsEndpoint[IO]] = for {
    eventsFinder <- EventsFinder(sessionResource, queriesExecTimes)
    eventlogUrl  <- EventLogUrl()
  } yield new EventsEndpointImpl(eventsFinder, eventlogUrl)

  sealed trait Request {
    val pagingRequest: PagingRequest
  }

  final case class EventInfo(eventId:         EventId,
                             projectPath:     projects.Path,
                             status:          EventStatus,
                             eventDate:       EventDate,
                             executionDate:   ExecutionDate,
                             maybeMessage:    Option[EventMessage],
                             processingTimes: List[StatusProcessingTime]
  )

  final case class StatusProcessingTime(status: EventStatus, processingTime: EventProcessingTime)

  object Request {
    final case class ProjectEvents(projectPath:   projects.Path,
                                   maybeStatus:   Option[EventStatus],
                                   pagingRequest: PagingRequest
    ) extends Request

    final case class EventsWithStatus(status: EventStatus, pagingRequest: PagingRequest) extends Request

    implicit val show: Show[Request] = Show.show {
      case EventsEndpoint.Request.ProjectEvents(path, Some(status), _) => show"project-path: $path; status: $status"
      case EventsEndpoint.Request.ProjectEvents(path, None, _)         => show"project-path: $path"
      case EventsEndpoint.Request.EventsWithStatus(status, _)          => show"status: $status"
    }
  }

  object EventInfo {

    implicit lazy val infoEncoder: Encoder[EventInfo] = eventInfo => {
      import io.circe.literal._
      import io.circe.syntax._

      implicit val processingTimeEncoder: Encoder[StatusProcessingTime] = processingTime => json"""{
        "status":         ${processingTime.status.value},
        "processingTime": ${processingTime.processingTime.value}
      }"""

      json"""{
        "id":              ${eventInfo.eventId.value},
        "projectPath":     ${eventInfo.projectPath.value},
        "status":          ${eventInfo.status.value},
        "processingTimes": ${eventInfo.processingTimes.map(_.asJson)},
        "date" :           ${eventInfo.eventDate.value.asJson},
        "executionDate":   ${eventInfo.executionDate.value.asJson}
      }""" deepMerge {
        eventInfo.maybeMessage.map(message => json"""{"message": ${message.value}}""").getOrElse(Json.obj())
      }
    }
  }
}
