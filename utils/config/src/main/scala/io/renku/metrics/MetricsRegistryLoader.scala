/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.effect.Sync
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import io.renku.metrics.MetricsRegistry.{DisabledMetricsRegistry, EnabledMetricsRegistry}

import scala.util.control.NonFatal

object MetricsRegistryLoader {
  private def noneValue[F[_]: MonadThrow]: PartialFunction[Throwable, F[Option[Boolean]]] = { case NonFatal(_) =>
    Option(true).pure[F]
  }

  def apply[F[_]: Sync](config: Config = ConfigFactory.load()): F[MetricsRegistry[F]] = for {
    maybeEnabled <- ConfigLoader.find[F, Option[Boolean]]("metrics.enabled", config) recoverWith noneValue
  } yield maybeEnabled match {
    case Some(false) => new DisabledMetricsRegistry[F]
    case _           => new EnabledMetricsRegistry[F]
  }
}
