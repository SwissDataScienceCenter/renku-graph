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
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.EventsEndpoint.Criteria._
import io.renku.eventlog.events.EventsEndpoint.JsonEncoders
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.{EventDate, EventInfo, EventStatus, StatusProcessingTime}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.rest.paging.PagingRequest
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
      .map(_.toHttpResponse[F, EventLogUrl](resourceUrl(request), EventLogUrl, JsonEncoders.eventInfoEncoder))
      .recoverWith(httpResponse(request))

  private def resourceUrl(request: Request[F]) = EventLogUrl(show"$eventLogUrl${request.uri}")

  private def httpResponse(request: Request[F]): PartialFunction[Throwable, F[Response[F]]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(show"Finding events for '${request.uri}' failed")
      InternalServerError(ErrorMessage(exception))
  }
}

object EventsEndpoint {

  def apply[F[_]: Async: NonEmptyParallel: SessionResource: Logger: QueriesExecutionTimes]: F[EventsEndpoint[F]] = for {
    eventsFinder <- EventsFinder[F]
    eventlogUrl  <- EventLogUrl()
  } yield new EventsEndpointImpl(eventsFinder, eventlogUrl)

  final case class Criteria(
      filters: Criteria.Filters,
      sorting: Criteria.Sorting.By = Sorting.default,
      paging:  PagingRequest = PagingRequest.default
  )

  object Criteria {

    sealed trait Filters       extends Product with Serializable
    sealed trait FiltersOnDate extends Filters

    object Filters {
      final case class ProjectEvents(projectId:   projects.Identifier,
                                     maybeStatus: Option[EventStatus],
                                     maybeDates:  Option[FiltersOnDate]
      ) extends Filters
      final case class EventsWithStatus(status: EventStatus, maybeDates: Option[FiltersOnDate]) extends Filters
      final case class EventsSince(eventDate: EventDate)                                        extends FiltersOnDate
      final case class EventsUntil(eventDate: EventDate)                                        extends FiltersOnDate
      final case class EventsSinceAndUntil(since: EventsSince, until: EventsUntil)              extends FiltersOnDate
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
    implicit def idParamShow[I <: projects.Identifier]: Show[I] = Show.show {
      case path: projects.Path     => s"project-path: $path"
      case id:   projects.GitLabId => s"project-id: $id"
    }
    _.filters match {
      case Filters.ProjectEvents(id, Some(status), Some(dates)) => show"$id; status: $status; $dates"
      case Filters.ProjectEvents(id, Some(status), None)        => show"$id; status: $status"
      case Filters.ProjectEvents(id, None, Some(dates))         => show"$id; $dates"
      case Filters.ProjectEvents(id, None, _)                   => show"$id"
      case Filters.EventsWithStatus(status, _)                  => show"status: $status"
      case filterOnDate: Criteria.FiltersOnDate => filterOnDate.show
    }
  }

  implicit val filtersOnDateShow: Show[Criteria.FiltersOnDate] = Show.show {
    case Filters.EventsSince(since)                => show"since: $since"
    case Filters.EventsUntil(until)                => show"until: $until"
    case Filters.EventsSinceAndUntil(since, until) => show"since: ${since.eventDate}; until: ${until.eventDate}"
  }

  object JsonEncoders {
    import io.circe.literal._
    import io.circe.syntax._

    implicit val statusProcessingTimeEncoder: Encoder[StatusProcessingTime] = { processingTime =>
      json"""{
        "status":         ${processingTime.status},
        "processingTime": ${processingTime.processingTime}
      }"""
    }

    implicit val projectIdsEncoder: Encoder[EventInfo.ProjectIds] = ids =>
      json"""{ "id": ${ids.id}, "path": ${ids.path} }"""

    implicit val eventInfoEncoder: Encoder[EventInfo] = eventInfo =>
      json"""{
          "id":              ${eventInfo.eventId},
          "project":     ${eventInfo.project},
          "status":          ${eventInfo.status},
          "processingTimes": ${eventInfo.processingTimes},
          "date" :           ${eventInfo.eventDate},
          "executionDate":   ${eventInfo.executionDate}
        }""".deepMerge(
        eventInfo.maybeMessage.map(m => Json.obj("message" -> m.asJson)).getOrElse(Json.obj())
      )
  }
}
