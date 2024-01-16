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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.Histogram
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

final class ExecutionTimeRecorderImpl[F[_]: Sync: Clock: Logger](
    threshold:      ElapsedTime,
    maybeHistogram: Option[Histogram[F]]
) extends ExecutionTimeRecorder[F](threshold) {

  override def measureExecutionTime[A](
      block:               F[A],
      maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, A)] =
    Clock[F]
      .timed(block)
      .flatTap(updateHistogram(maybeHistogramLabel))
      .map { case (elapsedTime, result) => ElapsedTime(elapsedTime) -> result }

  private def updateHistogram[A](maybeLabel: Option[String Refined NonEmpty]): ((FiniteDuration, A)) => F[Unit] = {
    case (duration, _) =>
      maybeHistogram.fold(ifEmpty = ().pure[F])(_.observe(maybeLabel.map(_.value), duration))
  }
}
