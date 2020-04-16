/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Clock, ConcurrentEffect}
import cats.implicits._
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.dbeventlog.creation.EventCreationEndpoint
import ch.datascience.dbeventlog.latestevents.LatestEventsEndpoint
import ch.datascience.dbeventlog.processingstatus.ProcessingStatusEndpoint
import ch.datascience.dbeventlog.rescheduling.ReSchedulingEndpoint
import ch.datascience.dbeventlog.statuschange.StatusChangeEndpoint
import ch.datascience.dbeventlog.subscriptions.SubscriptionsEndpoint
import ch.datascience.graph.http.server.binders._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.server.QueryParameterTools._
import ch.datascience.metrics.RoutesMetrics
import org.http4s._
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    eventCreationEndpoint:    EventCreationEndpoint[F],
    latestEventsEndpoint:     LatestEventsEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    reSchedulingEndpoint:     ReSchedulingEndpoint[F],
    statusChangeEndpoint:     StatusChangeEndpoint[F],
    subscriptionsEndpoint:    SubscriptionsEndpoint[F],
    routesMetrics:            RoutesMetrics[F]
)(implicit clock:             Clock[F])
    extends Http4sDsl[F] {

  import AddSubscription._
  import eventCreationEndpoint._
  import latestEventsEndpoint._
  import org.http4s.HttpRoutes
  import processingStatusEndpoint._
  import reSchedulingEndpoint._
  import routesMetrics._
  import statusChangeEndpoint._
  import subscriptionsEndpoint._

  // format: off
  lazy val routes: F[HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ POST  -> Root / "events"                                                                  => addEvent(request)
    case           GET   -> Root / "events" / "latest"                                                       => findLatestEvents
    case           GET   -> Root / "events" / "projects" / ProjectId(id) / "status"                          => findProcessingStatus(id)
    case request @ PATCH -> Root / "events" / EventId(eventId) / "projects"/ ProjectId(projectId) / "status" => changeStatus(CompoundEventId(eventId, projectId), request)
    case           POST  -> Root / "events" / "status" / "NEW"                                               => triggerReScheduling
    case request @ POST  -> Root / "events" / "subscriptions" :? status(maybeStatus)                         => maybeAddSubscription(maybeStatus, request)
    case           GET   -> Root / "ping"                                                                    => Ok("pong")
  }.meter flatMap `add GET Root / metrics`
  // format: on

  private object AddSubscription {

    private val statusQueryParamError = ParseFailure("No valid 'status' query parameter", "")

    private implicit val statusDecoder: QueryParamDecoder[String] =
      (value: QueryParameterValue) => {
        Validated
          .cond(
            value.value.trim == "READY",
            value.value.trim,
            statusQueryParamError
          )
          .toValidatedNel
      }

    object status extends OptionalValidatingQueryParamDecoderMatcher[String]("status") {
      val parameterName: String = "status"
    }

    def maybeAddSubscription(maybeStatus: Option[ValidatedNel[ParseFailure, String]],
                             request:     Request[F]): F[Response[F]] =
      maybeStatus
        .fold(statusQueryParamError.invalidNel[String])(identity)
        .fold(toBadRequest(), _ => addSubscription(request))
  }
}
