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
import ch.datascience.graph.http.server.binders._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage._
import ch.datascience.metrics.RoutesMetrics
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.EventEndpoint
import io.renku.eventlog.eventspatching.EventsPatchingEndpoint
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.eventlog.statuschange.StatusChangeEndpoint
import io.renku.eventlog.subscriptions.SubscriptionsEndpoint
import org.http4s._
import org.http4s.dsl.Http4sDsl

import scala.util.Try

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    eventEndpoint:            EventEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    eventsPatchingEndpoint:   EventsPatchingEndpoint[F],
    statusChangeEndpoint:     StatusChangeEndpoint[F],
    subscriptionsEndpoint:    SubscriptionsEndpoint[F],
    eventDetailsEndpoint:     EventDetailsEndpoint[F],
    routesMetrics:            RoutesMetrics[F]
)(implicit clock:             Clock[F], contextShift: ContextShift[F])
    extends Http4sDsl[F] {

  import ProjectIdParameter._
  import eventDetailsEndpoint._
  import eventEndpoint._
  import eventsPatchingEndpoint._
  import org.http4s.HttpRoutes
  import processingStatusEndpoint._
  import routesMetrics._
  import statusChangeEndpoint._
  import subscriptionsEndpoint._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ POST  -> Root / "events"                                            => processEvent(request)
    case request @ PATCH -> Root / "events"                                            => triggerEventsPatching(request)
    case           GET   -> Root / "events"/ EventId(eventId) / ProjectId(projectId)   => getDetails(CompoundEventId(eventId, projectId))
    case request @ PATCH -> Root / "events" / EventId(eventId) / ProjectId(projectId)  => changeStatus(CompoundEventId(eventId, projectId), request)
    case           GET   -> Root / "processing-status" :? `project-id`(maybeProjectId) => maybeFindProcessingStatus(maybeProjectId)
    case           GET   -> Root / "ping"                                              => Ok("pong")
    case request @ POST  -> Root / "subscriptions"                                     => addSubscription(request)
  }.withMetrics
  

  // format: on
  private object LatestPerProjectParameter {

    private implicit val queryParameterDecoder: QueryParamDecoder[Boolean] =
      (value: QueryParameterValue) =>
        Try(value.value.toBoolean).toEither
          .leftMap(_ => ParseFailure(s"'${`latest-per-project`}' parameter with invalid value", ""))
          .toValidatedNel

    object `latest-per-project` extends OptionalValidatingQueryParamDecoderMatcher[Boolean]("latest-per-project") {
      val parameterName:     String = "latest-per-project"
      override val toString: String = parameterName
    }
  }

  private object ProjectIdParameter {

    private implicit val queryParameterDecoder: QueryParamDecoder[projects.Id] =
      (value: QueryParameterValue) =>
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

  private def maybeFindProcessingStatus(
      maybeProjectId: Option[ValidatedNel[ParseFailure, projects.Id]]
  ): F[Response[F]] = maybeProjectId match {
    case None =>
      NotFound(ErrorMessage(s"No '${`project-id`}' parameter"))
    case Some(validatedProjectId) =>
      validatedProjectId.fold(
        errors => BadRequest(ErrorMessage(errors.map(_.getMessage()).toList.mkString("; "))),
        findProcessingStatus
      )
  }
}
