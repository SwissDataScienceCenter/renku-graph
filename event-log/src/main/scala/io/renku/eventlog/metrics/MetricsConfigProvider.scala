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

package io.renku.eventlog.metrics

import cats.MonadError
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.MetricsConfigProvider

import scala.concurrent.duration.FiniteDuration

object MetricsConfigProvider {

  def apply[Interpretation[_]](
      configuration: Config = ConfigFactory.load()
  )(implicit ME:     MonadError[Interpretation, Throwable]): MetricsConfigProvider[Interpretation] =
    new MetricsConfigProvider[Interpretation] {
      override def getInterval() = find[FiniteDuration]("event-log.metrics.scheduler-reset-interval", configuration)
    }
}
