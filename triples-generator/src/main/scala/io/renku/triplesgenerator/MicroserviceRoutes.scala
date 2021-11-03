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

package io.renku.triplesgenerator

import cats.MonadThrow
import cats.effect.{Clock, Resource}
import com.typesafe.config.Config
import io.renku.metrics.RoutesMetrics
import io.renku.triplesgenerator.events.EventEndpoint
import org.http4s.dsl.Http4sDsl

import scala.jdk.CollectionConverters._

private class MicroserviceRoutes[F[_]: MonadThrow](
    eventEndpoint: EventEndpoint[F],
    routesMetrics: RoutesMetrics[F],
    config:        Option[Config] = None
)(implicit clock:  Clock[F])
    extends Http4sDsl[F] {

  import eventEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case request @ POST -> Root / "events"        => processEvent(request)
    case GET            -> Root / "ping"          => Ok("pong")
    case GET            -> Root / "config-info"   => Ok(configInfo)
  }.withMetrics
  // format: on

  private lazy val configInfo = config
    .map(
      _.entrySet().asScala
        .map(mapEntry => mapEntry.getKey -> mapEntry.getValue.render())
        .mkString("\n")
    )
    .getOrElse("No config found")
}
