/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Show}
import io.circe.{Encoder, Json}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.Criteria._
import io.renku.eventlog.events.EventsEndpoint.EventInfo
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.rest.paging.PagingRequest
import io.renku.metrics.LabeledHistogram
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventsEndpoint[F[_]] {
  def findEvents(criteria: EventsEndpoint.Criteria, request: Request[F]): F[Response[F]]
}

class EventsEndpointImpl[F[_]: MonadThrow: Logger](eventsFinder: EventsFinder[F], eventLogUrl: EventLogUrl)
    extends Http4sDsl[F]
    with EventsEndpoint[F] {

  import io.renku.http.ErrorMessage._

  override def findEvents(criteria: EventsEndpoint.Criteria, request: Request[F]): F[Response[F]] =
    eventsFinder
      .findEvents(criteria)
      .map(_.toHttpResponse[F, EventLogUrl](eventLogUrl / request.uri.show, EventLogUrl, EventInfo.infoEncoder))
      .recoverWith(httpResponse(request))

  private def httpResponse(request: Request[F]): PartialFunction[Throwable, F[Response[F]]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(show"Finding events for '${request.uri}' failed")
      InternalServerError(ErrorMessage(exception))
  }
}

object EventsEndpoint {

  def apply[F[_]: Async: NonEmptyParallel: Logger](sessionResource: SessionResource[F, EventLogDB],
                                                   queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventsEndpoint[F]] = for {
    eventsFinder <- EventsFinder(sessionResource, queriesExecTimes)
    eventlogUrl  <- EventLogUrl()
  } yield new EventsEndpointImpl(eventsFinder, eventlogUrl)

  final case class Criteria(
      filters: Criteria.Filters,
      sorting: Criteria.Sorting.By = Sorting.default,
      paging:  PagingRequest = PagingRequest.default
  )

  object Criteria {

    sealed trait Filters

    object Filters {
      final case class ProjectEvents(projectPath: projects.Path, maybeStatus: Option[EventStatus]) extends Filters
      final case class EventsWithStatus(status: EventStatus)                                       extends Filters
    }

    object Sorting extends io.renku.http.rest.SortBy {
      import io.renku.http.rest.SortBy.Direction

      type PropertyType = SortProperty

      sealed trait SortProperty extends Property

      final case object EventDate extends Property("eventDate") with SortProperty

      lazy val default: Sorting.By = Sorting.By(EventDate, Direction.Desc)

      override lazy val properties: Set[SortProperty] = Set(EventDate)
    }
  }

  implicit val show: Show[Criteria] = Show.show {
    _.filters match {
      case Filters.ProjectEvents(path, Some(status)) => show"project-path: $path; status: $status"
      case Filters.ProjectEvents(path, None)         => show"project-path: $path"
      case Filters.EventsWithStatus(status)          => show"status: $status"
    }
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
