/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.metrics

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import org.http4s.HttpRoutes
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.middleware.{Metrics => ServerMetrics}

class RoutesMetrics[F[_]: Sync: MetricsRegistry] {

  implicit class RoutesOps(routes: HttpRoutes[F]) {

    def withMetrics: Resource[F, HttpRoutes[F]] =
      MetricsRegistry[F].maybeCollectorRegistry match {
        case Some(collectorRegistry) =>
          Prometheus.metricsOps[F](collectorRegistry, "server").map { metrics =>
            PrometheusExportService(collectorRegistry).routes <+> ServerMetrics[F](
              metrics
            )(routes)
          }
        case _ => Resource.eval(Sync[F].pure(routes))
      }
  }
}
