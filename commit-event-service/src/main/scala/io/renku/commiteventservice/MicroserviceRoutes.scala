/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice

import cats.MonadThrow
import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.renku.commiteventservice.events.EventEndpoint
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.http.server.version
import io.renku.metrics.RoutesMetrics
import org.http4s.dsl.Http4sDsl

private class MicroserviceRoutes[F[_]: MonadThrow](
    eventEndpoint: EventEndpoint[F],
    routesMetrics: RoutesMetrics[F],
    versionRoutes: version.Routes[F]
) extends Http4sDsl[F] {

  import eventEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case           GET  -> Root / "ping"   => Ok("pong")
    case request @ POST -> Root / "events" => processEvent(request)
  }.withMetrics.map(_  <+> versionRoutes())
  // format: on
}

private object MicroserviceRoutes {
  def apply[F[_]: Async](
      consumersRegistry: EventConsumersRegistry[F],
      routesMetrics:     RoutesMetrics[F]
  ): F[MicroserviceRoutes[F]] = for {
    eventEndpoint <- EventEndpoint[F](consumersRegistry)
    versionRoutes <- version.Routes[F]
  } yield new MicroserviceRoutes(eventEndpoint, routesMetrics, versionRoutes)
}
