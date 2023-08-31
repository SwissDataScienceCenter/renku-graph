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

package io.renku.triplesgenerator

import cats.MonadThrow
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.http.server.binders.ProjectSlug
import io.renku.http.server.version
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.events.EventEndpoint
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.jdk.CollectionConverters._

private class MicroserviceRoutes[F[_]: MonadThrow](
    eventEndpoint:         EventEndpoint[F],
    projectUpdateEndpoint: projects.update.Endpoint[F],
    routesMetrics:         RoutesMetrics[F],
    versionRoutes:         version.Routes[F],
    config:                Config
) extends Http4sDsl[F] {

  import eventEndpoint._
  import org.http4s.HttpRoutes
  import projectUpdateEndpoint.`PATCH /projects/:slug`
  import routesMetrics._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case req @ POST  -> Root / "events"                       => processEvent(req)
    case GET         -> Root / "ping"                         => Ok("pong")
    case req @ PATCH -> Root / "projects" / ProjectSlug(slug) => `PATCH /projects/:slug`(slug, req)
    case GET         -> Root / "config-info"                  => Ok(configInfo)
  }.withMetrics.map(_ <+> versionRoutes())
  // format: on

  private lazy val configInfo = config
    .entrySet()
    .asScala
    .map(mapEntry => mapEntry.getKey -> mapEntry.getValue.render())
    .mkString("\n")
}

private object MicroserviceRoutes {
  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder](consumersRegistry: EventConsumersRegistry[F],
                                                                           tsWriteLock:       TsWriteLock[F],
                                                                           config:            Config
  ): F[MicroserviceRoutes[F]] = for {
    eventEndpoint         <- EventEndpoint(consumersRegistry)
    projectUpdateEndpoint <- projects.update.Endpoint[F](tsWriteLock)
    versionRoutes         <- version.Routes[F]
  } yield new MicroserviceRoutes(eventEndpoint, projectUpdateEndpoint, new RoutesMetrics[F], versionRoutes, config)
}
