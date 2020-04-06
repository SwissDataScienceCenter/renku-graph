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

import cats.effect.{Clock, ConcurrentEffect}
import cats.implicits._
import ch.datascience.dbeventlog.creation.EventCreationEndpoint
import ch.datascience.dbeventlog.latestevents.LatestEventsEndpoint
import ch.datascience.dbeventlog.processingstatus.ProcessingStatusEndpoint
import ch.datascience.graph.http.server.binders.ProjectId
import ch.datascience.metrics.RoutesMetrics
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    eventCreationEndpoint:    EventCreationEndpoint[F],
    latestEventsEndpoint:     LatestEventsEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    routesMetrics:            RoutesMetrics[F]
)(implicit clock:             Clock[F])
    extends Http4sDsl[F] {

  import eventCreationEndpoint._
  import latestEventsEndpoint._
  import org.http4s.HttpRoutes
  import processingStatusEndpoint._
  import routesMetrics._

  // format: off
  lazy val routes: F[HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ POST -> Root / "events"                                         => addEvent(request)
    case           GET  -> Root / "events" / "latest"                              => findLatestEvents
    case           GET  -> Root / "events" / "projects" / ProjectId(id) / "status" => findProcessingStatus(id)
    case           GET  -> Root / "ping"                                           => Ok("pong")
  }.meter flatMap `add GET Root / metrics`
  // format: on
}
