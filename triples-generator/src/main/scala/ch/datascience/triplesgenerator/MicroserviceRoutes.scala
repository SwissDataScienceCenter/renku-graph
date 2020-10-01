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

package ch.datascience.triplesgenerator

import cats.effect.{Clock, ConcurrentEffect, Resource}
import ch.datascience.metrics.RoutesMetrics
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingEndpoint
import org.http4s.dsl.Http4sDsl

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    eventProcessingEndpoint: EventProcessingEndpoint[F],
    routesMetrics:           RoutesMetrics[F]
)(implicit clock:            Clock[F])
    extends Http4sDsl[F] {

  import eventProcessingEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ POST -> Root / "events" => processEvent(request)
    case GET            -> Root / "ping"   => Ok("pong")
  }.withMetrics
  // format: on
}
