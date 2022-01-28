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

package io.renku.eventlog.metrics

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.MetricsConfigProvider

import scala.concurrent.duration.FiniteDuration

object MetricsConfigProvider {

  def apply[F[_]: MonadThrow](configuration: Config = ConfigFactory.load()): MetricsConfigProvider[F] =
    new MetricsConfigProvider[F] {
      override def getInterval() = find[FiniteDuration]("event-log.metrics.scheduler-reset-interval", configuration)
    }
}
