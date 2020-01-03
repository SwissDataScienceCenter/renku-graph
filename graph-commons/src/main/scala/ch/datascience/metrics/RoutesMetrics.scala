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

package ch.datascience.metrics

import cats.effect.{Clock, ConcurrentEffect}
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.middleware.{Metrics => ServerMetrics}

import scala.language.higherKinds

trait RoutesMetrics {

  import MetricsRegistry._

  implicit class RoutesOps[Interpretation[_]](routes: HttpRoutes[Interpretation]) {

    def meter(implicit F: ConcurrentEffect[Interpretation],
              clock:      Clock[Interpretation]): Interpretation[HttpRoutes[Interpretation]] =
      for {
        metrics <- Prometheus[Interpretation](collectorRegistry, "server")
        meteredRoutes = ServerMetrics[Interpretation](metrics)(routes)
      } yield meteredRoutes

  }

  def `add GET Root / metrics`[Interpretation[_]](
      routes:   HttpRoutes[Interpretation]
  )(implicit F: ConcurrentEffect[Interpretation]): Interpretation[HttpRoutes[Interpretation]] =
    for {
      prometheusService <- PrometheusExportService(collectorRegistry).pure[Interpretation]
    } yield prometheusService.routes <+> routes
}
