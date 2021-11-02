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

package io.renku.logging

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.Histogram
import io.renku.generators.CommonGraphGenerators.elapsedTimes
import io.renku.generators.Generators.Implicits._
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.util.Try

object TestExecutionTimeRecorder {

  def apply[F[_]: MonadThrow: Logger](maybeHistogram: Option[Histogram] = None): TestExecutionTimeRecorder[F] =
    new TestExecutionTimeRecorder[F](threshold = elapsedTimes.generateOne, maybeHistogram)
}

class TestExecutionTimeRecorder[F[_]: MonadThrow: Logger](
    threshold:      ElapsedTime,
    maybeHistogram: Option[Histogram]
) extends ExecutionTimeRecorder[F](threshold) {

  val elapsedTime:            ElapsedTime = threshold
  lazy val executionTimeInfo: String      = s" in ${threshold}ms"

  override def measureExecutionTime[BlockOut](
      block:               => F[BlockOut],
      maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, BlockOut)] = for {
    _ <- MonadThrow[F].unit
    maybeHistogramTimer = startTimer(maybeHistogramLabel)
    result <- block
    _ = maybeHistogramTimer map (_.observeDuration())
  } yield threshold -> result

  private def startTimer(maybeHistogramLabel: Option[String Refined NonEmpty]) = maybeHistogram flatMap { histogram =>
    maybeHistogramLabel
      .map(label => histogram.labels(label.value).startTimer())
      .orElse(Try(histogram.startTimer()).toOption)
  }
}
