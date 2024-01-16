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

package io.renku.logging

import cats.effect._
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.Histogram
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

object ExecutionTimeRecorderLoader {
  def apply[F[_]: Sync: Logger](
      config:         Config = ConfigFactory.load(),
      maybeHistogram: Option[Histogram[F]] = None
  ): F[ExecutionTimeRecorder[F]] = for {
    duration <- ConfigLoader.find[F, FiniteDuration]("logging.elapsed-time-threshold", config)
  } yield new ExecutionTimeRecorderImpl(ElapsedTime(duration), maybeHistogram)

}
