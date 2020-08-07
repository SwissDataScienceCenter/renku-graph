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

import cats.effect.{Bracket, Clock, ConcurrentEffect}
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.middleware.{Metrics => ServerMetrics}

import scala.language.higherKinds

class RoutesMetrics[Interpretation[_]](metricsRegistry: MetricsRegistry[Interpretation]) {

  implicit class RoutesOps(routes: HttpRoutes[Interpretation]) {

    def meter(implicit F: ConcurrentEffect[Interpretation],
              clock:      Clock[Interpretation],
              bracket:    Bracket[Interpretation, Throwable]): Interpretation[HttpRoutes[Interpretation]] =
      metricsRegistry.maybeCollectorRegistry match {
        case Some(collectorRegistry) =>
          val prometheusService = PrometheusExportService(collectorRegistry)
          val router = for {
            metrics <- Prometheus.metricsOps[Interpretation](prometheusService.collectorRegistry, "server")
            meteredRoutes = ServerMetrics[Interpretation](metrics)(routes)
          } yield meteredRoutes
          router.use(r => F.pure(r))
        case _ => F.pure(routes)
      }
  }

  def `add GET Root / metrics`(
      routes:   HttpRoutes[Interpretation]
  )(implicit F: ConcurrentEffect[Interpretation]): Interpretation[HttpRoutes[Interpretation]] =
    metricsRegistry.maybeCollectorRegistry match {
      case Some(collectorRegistry) =>
        for {
          prometheusService <- PrometheusExportService(collectorRegistry).pure[Interpretation]
        } yield prometheusService.routes <+> routes
      case _ => F.pure(routes)
    }
}
