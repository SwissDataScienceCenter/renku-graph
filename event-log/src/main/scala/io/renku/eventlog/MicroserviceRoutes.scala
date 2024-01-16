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

package io.renku.eventlog

import cats.NonEmptyParallel
import cats.data.ValidatedNel
import cats.effect.kernel.{Ref, Sync}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.eventpayload.EventPayloadEndpoint
import io.renku.eventlog.events.producers.{EventProducersRegistry, SubscriptionsEndpoint}
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.status.StatusEndpoint
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.http.server.binders._
import io.renku.graph.model.events.{CompoundEventId, EventDate, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools.{resourceNotFound, toBadRequest}
import io.renku.http.server.version
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import org.http4s._
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.Try

private class MicroserviceRoutes[F[_]: Sync](
    eventEndpoint:         EventEndpoint[F],
    eventsEndpoint:        EventsEndpoint[F],
    subscriptionsEndpoint: SubscriptionsEndpoint[F],
    eventDetailsEndpoint:  EventDetailsEndpoint[F],
    eventPayloadEndpoint:  EventPayloadEndpoint[F],
    statusEndpoint:        StatusEndpoint[F],
    routesMetrics:         RoutesMetrics[F],
    isMigrating:           Ref[F, Boolean],
    versionRoutes:         version.Routes[F]
) extends Http4sDsl[F] {

  import EventStatusParameter._
  import EventsEndpoint.Criteria.Sort.sort
  import ProjectIdParameter._
  import ProjectSlugParameter._
  import eventDetailsEndpoint._
  import eventEndpoint._
  import eventsEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._
  import statusEndpoint._
  import subscriptionsEndpoint._


  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ GET  -> Root / "events" :? `project-id`(validatedProjectId) +&
      `project-slug`(validatedProjectSlug) +&
      status(status) +&
      since(since) +&
      until(until) +&
      page(page) +&
      perPage(perPage) +&
      sort(sortBy) =>
      respond503IfMigrating(maybeFindEvents(validatedProjectId, validatedProjectSlug, status, since, until, page, perPage, sortBy, request))
    case req @ POST -> Root / "events"                                            => respond503IfMigrating(processEvent(req))
    case       GET  -> Root / "events" / EventId(eventId) / ProjectId(projectId)  => respond503IfMigrating(getDetails(CompoundEventId(eventId, projectId)))
    case       GET  -> Root / "events" / EventId(eventId) / ProjectSlug(projectSlug) / "payload"  => respond503IfMigrating(eventPayloadEndpoint.getEventPayload(eventId, projectSlug))
    case       GET  -> Root / "ping"                                              => Ok("pong")
    case       GET  -> Root / "migration-status"                                  => isMigrating.get.flatMap {isMigrating => Ok(json"""{"isMigrating": $isMigrating}""")}
    case       GET  -> Root / "status"                                            => respond503IfMigrating(`GET /status`)
    case req @ POST -> Root / "subscriptions"                                     => respond503IfMigrating(addSubscription(req))
  }.withMetrics.map(_  <+> versionRoutes())
  // format: on

  private def respond503IfMigrating(otherwise: => F[Response[F]]): F[Response[F]] =
    isMigrating.get >>= {
      case true  => ServiceUnavailable()
      case false => otherwise
    }

  private object ProjectIdParameter {
    private implicit val queryParameterDecoder: QueryParamDecoder[projects.GitLabId] =
      (value: QueryParameterValue) => {
        val error = ParseFailure(s"'${`project-id`}' parameter with invalid value", "")
        Either
          .fromOption(value.value.toIntOption, ifNone = error)
          .flatMap(projects.GitLabId.from)
          .leftMap(_ => error)
          .toValidatedNel
      }

    object `project-id` extends OptionalValidatingQueryParamDecoderMatcher[projects.GitLabId]("project-id") {
      val parameterName:     String = "project-id"
      override val toString: String = parameterName
    }
  }

  private object ProjectSlugParameter {
    private implicit val queryParameterDecoder: QueryParamDecoder[projects.Slug] =
      (value: QueryParameterValue) =>
        projects.Slug
          .from(value.value)
          .leftMap(_ => ParseFailure(s"'${`project-slug`}' parameter with invalid value", ""))
          .toValidatedNel

    object `project-slug` extends OptionalValidatingQueryParamDecoderMatcher[projects.Slug]("project-slug") {
      val parameterName:     String = "project-slug"
      override val toString: String = parameterName
    }
  }

  private object EventStatusParameter {
    private implicit val queryParameterDecoder: QueryParamDecoder[EventStatus] =
      (value: QueryParameterValue) =>
        Try(EventStatus(value.value)).toEither
          .leftMap(_ => ParseFailure(s"'$status' parameter with invalid value", ""))
          .toValidatedNel

    object status extends OptionalValidatingQueryParamDecoderMatcher[EventStatus]("status") {
      val parameterName:     String = "status"
      override val toString: String = parameterName
    }

    private def eventDateDecoder(paramName: String): QueryParamDecoder[EventDate] =
      (value: QueryParameterValue) =>
        Either
          .catchNonFatal(Instant.parse(value.value))
          .flatMap(EventDate.from)
          .leftMap(_ => ParseFailure(s"'$paramName' parameter with invalid value", ""))
          .toValidatedNel

    object since extends OptionalValidatingQueryParamDecoderMatcher[EventDate]("since")(eventDateDecoder("since")) {
      val parameterName:     String = "since"
      override val toString: String = parameterName
    }

    object until extends OptionalValidatingQueryParamDecoderMatcher[EventDate]("until")(eventDateDecoder("until")) {
      val parameterName:     String = "until"
      override val toString: String = parameterName
    }
  }

  private def maybeFindEvents(maybeProjectId:   Option[ValidatedNel[ParseFailure, projects.GitLabId]],
                              maybeProjectSlug: Option[ValidatedNel[ParseFailure, projects.Slug]],
                              maybeStatus:      Option[ValidatedNel[ParseFailure, EventStatus]],
                              maybeSince:       Option[ValidatedNel[ParseFailure, EventDate]],
                              maybeUntil:       Option[ValidatedNel[ParseFailure, EventDate]],
                              maybePage:        Option[ValidatedNel[ParseFailure, Page]],
                              maybePerPage:     Option[ValidatedNel[ParseFailure, PerPage]],
                              maybeSortBy:      ValidatedNel[ParseFailure, List[EventsEndpoint.Criteria.Sort.By]],
                              request:          Request[F]
  ): F[Response[F]] = {
    import EventsEndpoint.Criteria._

    val filtersOnDate: Option[ValidatedNel[ParseFailure, FiltersOnDate]] = maybeSince -> maybeUntil match {
      case (Some(since), None) => since.map(Filters.EventsSince).some
      case (None, Some(until)) => until.map(Filters.EventsUntil).some
      case (Some(since), Some(until)) =>
        (since.map(Filters.EventsSince) -> until.map(Filters.EventsUntil)).mapN(Filters.EventsSinceAndUntil).some
      case _ => None
    }

    (maybeProjectId orElse maybeProjectSlug, maybeStatus, filtersOnDate) match {
      case (None, None, None) => resourceNotFound
      case (Some(validatedIdentifier), maybeStatus, maybeDates) =>
        (
          validatedIdentifier,
          maybeStatus.sequence,
          maybeDates.sequence,
          maybeSortBy,
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (identifier, maybeStatus, maybeDates, sorting, paging) =>
          val sortOrDefault = Sorting.fromList(sorting).getOrElse(EventsEndpoint.Criteria.Sort.default)
          findEvents(
            EventsEndpoint.Criteria(Filters.ProjectEvents(identifier, maybeStatus, maybeDates), sortOrDefault, paging),
            request
          )
        }.fold(toBadRequest, identity)
      case (None, Some(validatedStatus), maybeDates) =>
        (
          validatedStatus,
          maybeDates.sequence,
          maybeSortBy,
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (status, maybeDates, sorting, paging) =>
          val sortOrDefault = Sorting.fromList(sorting).getOrElse(EventsEndpoint.Criteria.Sort.default)
          findEvents(EventsEndpoint.Criteria(Filters.EventsWithStatus(status, maybeDates), sortOrDefault, paging),
                     request
          )
        }.fold(toBadRequest, identity)
      case (None, None, Some(validatedDates)) =>
        (validatedDates, maybeSortBy, PagingRequest(maybePage, maybePerPage))
          .mapN { case (dates, sorting, paging) =>
            val sortOrDefault = Sorting.fromList(sorting).getOrElse(EventsEndpoint.Criteria.Sort.default)
            findEvents(EventsEndpoint.Criteria(dates, sortOrDefault, paging), request)
          }
          .fold(toBadRequest, identity)
    }
  }

}

private object MicroserviceRoutes {
  def apply[F[_]: Async: NonEmptyParallel: SessionResource: Logger: MetricsRegistry: QueriesExecutionTimes](
      consumersRegistry:      EventConsumersRegistry[F],
      eventProducersRegistry: EventProducersRegistry[F],
      isMigrating:            Ref[F, Boolean]
  ): F[MicroserviceRoutes[F]] = for {
    eventEndpoint         <- EventEndpoint[F](consumersRegistry)
    eventsEndpoint        <- EventsEndpoint[F]
    subscriptionsEndpoint <- SubscriptionsEndpoint(eventProducersRegistry)
    eventDetailsEndpoint  <- EventDetailsEndpoint[F]
    eventPayloadEndpoint = EventPayloadEndpoint[F]
    statusEndpoint <- StatusEndpoint[F](eventProducersRegistry)
    versionRoutes  <- version.Routes[F]
  } yield new MicroserviceRoutes(
    eventEndpoint,
    eventsEndpoint,
    subscriptionsEndpoint,
    eventDetailsEndpoint,
    eventPayloadEndpoint,
    statusEndpoint,
    new RoutesMetrics[F],
    isMigrating,
    versionRoutes
  )
}
