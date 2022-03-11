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

import cats.NonEmptyParallel
import cats.data.{Validated, ValidatedNel}
import cats.effect.kernel.{Ref, Sync}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.eventlog.subscriptions.{EventProducersRegistry, SubscriptionsEndpoint}
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.http.server.binders._
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools.{resourceNotFound, toBadRequest}
import io.renku.http.server.version
import io.renku.metrics.{LabeledHistogram, MetricsRegistry, RoutesMetrics}
import org.http4s._
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.Try

private class MicroserviceRoutes[F[_]: Sync](
    eventEndpoint:            EventEndpoint[F],
    eventsEndpoint:           EventsEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    subscriptionsEndpoint:    SubscriptionsEndpoint[F],
    eventDetailsEndpoint:     EventDetailsEndpoint[F],
    routesMetrics:            RoutesMetrics[F],
    isMigrating:              Ref[F, Boolean],
    versionRoutes:            version.Routes[F]
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
    case request @ GET  -> Root / "events" :? `project-path`(validatedProjectPath) +& status(status) +& page(page) +& perPage(perPage) +& sort(sortBy) => respond503IfMigrating(maybeFindEvents(validatedProjectPath, status, page, perPage, sortBy, request))
    case request @ POST -> Root / "events"                                            => respond503IfMigrating(processEvent(request))
    case           GET  -> Root / "events"/ EventId(eventId) / ProjectId(projectId)   => respond503IfMigrating(getDetails(CompoundEventId(eventId, projectId)))
    case           GET  -> Root / "processing-status" :? `project-id`(maybeProjectId) => respond503IfMigrating(maybeFindProcessingStatus(maybeProjectId))
    case           GET  -> Root / "ping"                                              => Ok("pong")
    case           GET  -> Root / "migration-status"                                  => isMigrating.get.flatMap {isMigrating => Ok(json"""{"isMigrating": $isMigrating}""")}
    case request @ POST -> Root / "subscriptions"                                     => respond503IfMigrating(addSubscription(request))
  }.withMetrics.map(_  <+> versionRoutes())
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
  }

  private def maybeFindEvents(maybeProjectPath: Option[ValidatedNel[ParseFailure, projects.Path]],
                              maybeStatus:      Option[ValidatedNel[ParseFailure, EventStatus]],
                              maybePage:        Option[ValidatedNel[ParseFailure, Page]],
                              maybePerPage:     Option[ValidatedNel[ParseFailure, PerPage]],
                              maybeSortBy:      Option[ValidatedNel[ParseFailure, EventsEndpoint.Criteria.Sorting.By]],
                              request:          Request[F]
  ): F[Response[F]] = {
    import EventsEndpoint.Criteria._

    (maybeProjectPath, maybeStatus) match {
      case (None, None) => resourceNotFound
      case (Some(validatedPath), maybeStatus) =>
        (
          validatedPath,
          maybeStatus.sequence,
          maybeSortBy getOrElse Validated.validNel(EventsEndpoint.Criteria.Sorting.default),
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (path, maybeStatus, sorting, paging) =>
          findEvents(EventsEndpoint.Criteria(Filters.ProjectEvents(path, maybeStatus), sorting, paging), request)
        }.fold(toBadRequest, identity)
      case (None, Some(validatedStatus)) =>
        (
          validatedStatus,
          maybeSortBy getOrElse Validated.validNel(EventsEndpoint.Criteria.Sorting.default),
          PagingRequest(maybePage, maybePerPage)
        ).mapN { case (status, sorting, paging) =>
          findEvents(EventsEndpoint.Criteria(Filters.EventsWithStatus(status), sorting, paging), request)
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

private object MicroserviceRoutes {
  def apply[F[_]: Async: NonEmptyParallel: SessionResource: Logger: MetricsRegistry](
      consumersRegistry:      EventConsumersRegistry[F],
      queriesExecTimes:       LabeledHistogram[F],
      eventProducersRegistry: EventProducersRegistry[F],
      isMigrating:            Ref[F, Boolean]
  ): F[MicroserviceRoutes[F]] = for {
    eventEndpoint            <- EventEndpoint[F](consumersRegistry)
    eventsEndpoint           <- EventsEndpoint(queriesExecTimes)
    processingStatusEndpoint <- ProcessingStatusEndpoint(queriesExecTimes)
    subscriptionsEndpoint    <- SubscriptionsEndpoint(eventProducersRegistry)
    eventDetailsEndpoint     <- EventDetailsEndpoint(queriesExecTimes)
    versionRoutes            <- version.Routes[F]
  } yield new MicroserviceRoutes(eventEndpoint,
                                 eventsEndpoint,
                                 processingStatusEndpoint,
                                 subscriptionsEndpoint,
                                 eventDetailsEndpoint,
                                 new RoutesMetrics[F],
                                 isMigrating,
                                 versionRoutes
  )
}
