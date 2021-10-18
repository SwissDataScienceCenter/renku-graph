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

package io.renku.eventlog

import cats.data.ValidatedNel
import cats.effect.{Clock, ConcurrentEffect, ContextShift, Resource}
import cats.syntax.all._
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
import org.http4s.dsl.Http4sDsl

import scala.util.Try

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    eventEndpoint:            EventEndpoint[F],
    eventsEndpoint:           EventsEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    subscriptionsEndpoint:    SubscriptionsEndpoint[F],
    eventDetailsEndpoint:     EventDetailsEndpoint[F],
    routesMetrics:            RoutesMetrics[F]
)(implicit clock:             Clock[F], contextShift: ContextShift[F])
    extends Http4sDsl[F] {

  import EventStatusParameter._
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
    case GET             -> Root / "events" :? `project-path`(validatedProjectPath) +& status(status) +& page(page) +& perPage(perPage) => maybeFindEvents(validatedProjectPath, status, page, perPage)
    case request @ POST  -> Root / "events"                                            => processEvent(request)
    case           GET   -> Root / "events"/ EventId(eventId) / ProjectId(projectId)   => getDetails(CompoundEventId(eventId, projectId))
    case           GET   -> Root / "processing-status" :? `project-id`(maybeProjectId) => maybeFindProcessingStatus(maybeProjectId)
    case           GET   -> Root / "ping"                                              => Ok("pong")
    case request @ POST  -> Root / "subscriptions"                                     => addSubscription(request)
  }.withMetrics
  
  // format: on
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
                              maybePerPage:     Option[ValidatedNel[ParseFailure, PerPage]]
  ): F[Response[F]] = (maybeProjectPath, maybeStatus) match {
    case (None, None) => resourceNotFound
    case (Some(validatedPath), maybeStatus) =>
      (validatedPath, maybeStatus.sequence, PagingRequest(maybePage, maybePerPage))
        .mapN(EventsEndpoint.Request.ProjectEvents)
        .map(findEvents)
        .fold(toBadRequest, identity)
    case (None, Some(validatedStatus)) =>
      (validatedStatus, PagingRequest(maybePage, maybePerPage))
        .mapN(EventsEndpoint.Request.EventsWithStatus)
        .map(findEvents)
        .fold(toBadRequest, identity)
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
