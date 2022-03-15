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

package io.renku.eventlog

import cats.data.{Validated, ValidatedNel}
import cats.effect.Resource
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.eventlog.subscriptions.SubscriptionsEndpoint
import io.renku.graph.http.server.binders._
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools.{resourceNotFound, toBadRequest}
import io.renku.metrics.RoutesMetrics
import org.http4s._
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl

import java.time.Instant
import scala.util.Try

private class MicroserviceRoutes[F[_]: Sync](
    eventEndpoint:            EventEndpoint[F],
    eventsEndpoint:           EventsEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    subscriptionsEndpoint:    SubscriptionsEndpoint[F],
    eventDetailsEndpoint:     EventDetailsEndpoint[F],
    routesMetrics:            RoutesMetrics[F],
    isMigrating:              Ref[F, Boolean]
) extends Http4sDsl[F] {

  import EventStatusParameter._
  import EventsEndpoint.Criteria.Sorting.sort
  import ProjectIdParameter._
  import ProjectPathParameter._
  import eventDetailsEndpoint._
  import eventEndpoint._
  import eventsEndpoint._
  import org.http4s.HttpRoutes
  import processingStatusEndpoint._
  import routesMetrics._
  import subscriptionsEndpoint._


  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ GET  -> Root / "events" :? `project-path`(validatedProjectPath) +& status(status) +& since(since) +& until(until) +& page(page) +& perPage(perPage) +& sort(sortBy) => respond503IfMigrating(maybeFindEvents(validatedProjectPath, status, since, until, page, perPage, sortBy, request))
    case request @ POST -> Root / "events"                                            => respond503IfMigrating(processEvent(request))
    case           GET  -> Root / "events"/ EventId(eventId) / ProjectId(projectId)   => respond503IfMigrating(getDetails(CompoundEventId(eventId, projectId)))
    case           GET  -> Root / "processing-status" :? `project-id`(maybeProjectId) => respond503IfMigrating(maybeFindProcessingStatus(maybeProjectId))
    case           GET  -> Root / "ping"                                              => Ok("pong")
    case           GET  -> Root / "migration-status"                                  => isMigrating.get.flatMap {isMigrating => Ok(json"""{"isMigrating": $isMigrating}""")}
    case request @ POST -> Root / "subscriptions"                                     => respond503IfMigrating(addSubscription(request))
  }.withMetrics
  // format: on

  def respond503IfMigrating(otherwise: => F[Response[F]]): F[Response[F]] = isMigrating.get.flatMap {
    case true  => ServiceUnavailable()
    case false => otherwise
  }

  private object ProjectIdParameter {

    private implicit val queryParameterDecoder: QueryParamDecoder[projects.Id] = (value: QueryParameterValue) =>
      {
        for {
          int <- Try(value.value.toInt).toEither
          id  <- projects.Id.from(int)
        } yield id
      }.leftMap(_ => ParseFailure(s"'${`project-id`}' parameter with invalid value", "")).toValidatedNel

    object `project-id` extends OptionalValidatingQueryParamDecoderMatcher[projects.Id]("project-id") {
      val parameterName:     String = "project-id"
      override val toString: String = parameterName
    }
  }

  private object ProjectPathParameter {
    private implicit val queryParameterDecoder: QueryParamDecoder[projects.Path] =
      (value: QueryParameterValue) =>
        projects.Path
          .from(value.value)
          .leftMap(_ => ParseFailure(s"'${`project-path`}' parameter with invalid value", ""))
          .toValidatedNel

    object `project-path` extends OptionalValidatingQueryParamDecoderMatcher[projects.Path]("project-path") {
      val parameterName:     String = "project-path"
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

  private def maybeFindEvents(maybeProjectPath: Option[ValidatedNel[ParseFailure, projects.Path]],
                              maybeStatus:      Option[ValidatedNel[ParseFailure, EventStatus]],
                              maybeSince:       Option[ValidatedNel[ParseFailure, EventDate]],
                              maybeUntil:       Option[ValidatedNel[ParseFailure, EventDate]],
                              maybePage:        Option[ValidatedNel[ParseFailure, Page]],
                              maybePerPage:     Option[ValidatedNel[ParseFailure, PerPage]],
                              maybeSortBy:      Option[ValidatedNel[ParseFailure, EventsEndpoint.Criteria.Sorting.By]],
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

    (maybeProjectPath, maybeStatus, filtersOnDate) match {
      case (None, None, None) => resourceNotFound
      case (Some(validatedPath), maybeStatus, maybeDates) =>
        (
          validatedPath,
          maybeStatus.sequence,
          maybeDates.sequence,
          maybeSortBy getOrElse Validated.validNel(EventsEndpoint.Criteria.Sorting.default),
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (path, maybeStatus, maybeDates, sorting, paging) =>
          findEvents(EventsEndpoint.Criteria(Filters.ProjectEvents(path, maybeStatus, maybeDates), sorting, paging),
                     request
          )
        }.fold(toBadRequest, identity)
      case (None, Some(validatedStatus), maybeDates) =>
        (
          validatedStatus,
          maybeDates.sequence,
          maybeSortBy getOrElse Validated.validNel(EventsEndpoint.Criteria.Sorting.default),
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (status, maybeDates, sorting, paging) =>
          findEvents(EventsEndpoint.Criteria(Filters.EventsWithStatus(status, maybeDates), sorting, paging), request)
        }.fold(toBadRequest, identity)
      case (None, None, Some(validatedDates)) =>
        (validatedDates,
         maybeSortBy getOrElse Validated.validNel(EventsEndpoint.Criteria.Sorting.default),
         PagingRequest(maybePage, maybePerPage)
        ).mapN { case (dates, sorting, paging) =>
          findEvents(EventsEndpoint.Criteria(dates, sorting, paging), request)
        }.fold(toBadRequest, identity)
    }
  }

  private def maybeFindProcessingStatus(
      maybeProjectId: Option[ValidatedNel[ParseFailure, projects.Id]]
  ): F[Response[F]] = maybeProjectId match {
    case None => NotFound(ErrorMessage(s"No '${`project-id`}' parameter"))
    case Some(validatedProjectId) =>
      validatedProjectId.fold(
        errors => BadRequest(ErrorMessage(errors.map(_.getMessage()).toList.mkString("; "))),
        findProcessingStatus
      )
  }
}
