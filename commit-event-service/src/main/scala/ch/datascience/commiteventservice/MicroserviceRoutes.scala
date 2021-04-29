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

package ch.datascience.commiteventservice

import cats.effect.{Clock, ConcurrentEffect, ContextShift, IO, Resource, Timer}
import ch.datascience.commiteventservice.events.EventEndpoint
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext

private class MicroserviceRoutes[Interpretation[_]: ConcurrentEffect](
    eventEndpoint: EventEndpoint[Interpretation],
    routesMetrics: RoutesMetrics[Interpretation]
)(implicit clock:  Clock[Interpretation])
    extends Http4sDsl[Interpretation] {

  import eventEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._

  // format: off
  lazy val routes: Resource[Interpretation, HttpRoutes[Interpretation]] = HttpRoutes.of[Interpretation] {
    case GET -> Root / "ping" => Ok("pong")
    case request@POST -> Root / "events" => processEvent(request)
  }.withMetrics
  // format: on
}

private object MicroserviceRoutes {
  def apply(
      consumersRegistry: EventConsumersRegistry[IO],
      metricsRegistry:   MetricsRegistry[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[MicroserviceRoutes[IO]] =
    for {
      eventEndpoint <- EventEndpoint(consumersRegistry)
    } yield new MicroserviceRoutes(
      eventEndpoint,
      new RoutesMetrics[IO](metricsRegistry)
    )
}
