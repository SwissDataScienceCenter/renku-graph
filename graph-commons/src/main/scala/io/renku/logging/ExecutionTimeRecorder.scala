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

package io.renku.logging

import cats.effect.{Clock, Sync}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.config.ConfigLoader.find
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, LabeledHistogram, SingleValueHistogram}
import io.renku.tinytypes.{LongTinyType, TinyTypeFactory}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

abstract class ExecutionTimeRecorder[F[_]: Logger](threshold: ElapsedTime) {

  def measureExecutionTime[BlockOut](block:               => F[BlockOut],
                                     maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, BlockOut)]

  def logExecutionTimeWhen[BlockOut](
      condition: PartialFunction[BlockOut, String]
  ): ((ElapsedTime, BlockOut)) => BlockOut = { case (elapsedTime, blockOut) =>
    logWarningIfAboveThreshold(elapsedTime, blockOut, condition)
    blockOut
  }

  def logExecutionTime[BlockOut](withMessage: => String): ((ElapsedTime, BlockOut)) => BlockOut = {
    case (elapsedTime, blockOut) =>
      logWarningIfAboveThreshold(elapsedTime, blockOut, forAnyOutReturn(withMessage))
      blockOut
  }

  private def forAnyOutReturn[BlockOut](message: String): PartialFunction[BlockOut, String] = { case _ =>
    message
  }

  private def logWarningIfAboveThreshold[BlockOut](
      elapsedTime: ElapsedTime,
      blockOut:    BlockOut,
      condition:   PartialFunction[BlockOut, String]
  ): Unit = if (elapsedTime.value >= threshold.value)
    (condition lift blockOut) foreach (message => Logger[F].warn(s"$message in ${elapsedTime}ms"))
}

class ExecutionTimeRecorderImpl[F[_]: Sync: Clock: Logger](
    threshold:      ElapsedTime,
    maybeHistogram: Option[Histogram[F]]
) extends ExecutionTimeRecorder[F](threshold) {

  override def measureExecutionTime[BlockOut](block:               => F[BlockOut],
                                              maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, BlockOut)] = Clock[F]
    .timed {
      maybeHistogram match {
        case None => block
        case Some(histogram) =>
          for {
            maybeTimer <- maybeStartTimer(histogram, maybeHistogramLabel)
            result     <- block
            _          <- maybeTimer.map(_.observeDuration.void).getOrElse(().pure[F])
          } yield result
      }
    }
    .map { case (elapsedTime, result) => ElapsedTime(elapsedTime) -> result }

  private def maybeStartTimer(histogram: Histogram[F], maybeHistogramLabel: Option[String Refined NonEmpty]) =
    histogram -> maybeHistogramLabel match {
      case (h: SingleValueHistogram[F], None)    => h.startTimer().map(_.some)
      case (h: LabeledHistogram[F], Some(label)) => h.startTimer(label.value).map(_.some)
      case (h: SingleValueHistogram[F], Some(label)) =>
        Logger[F].error(s"Label $label sent for a Single Value Histogram ${h.name}")
        None.pure[F]
      case (h: LabeledHistogram[F], None) =>
        Logger[F].error(s"No label sent for a Labeled Histogram ${h.name}")
        None.pure[F]
    }
}

object ExecutionTimeRecorder {

  def apply[F[_]](implicit etr: ExecutionTimeRecorder[F]): ExecutionTimeRecorder[F] = etr

  def apply[F[_]: Sync: Logger](
      config:         Config = ConfigFactory.load(),
      maybeHistogram: Option[Histogram[F]] = None
  ): F[ExecutionTimeRecorder[F]] = for {
    duration <- find[F, FiniteDuration]("logging.elapsed-time-threshold", config)
  } yield new ExecutionTimeRecorderImpl(ElapsedTime(duration), maybeHistogram)

  class ElapsedTime private (val value: Long) extends AnyVal with LongTinyType
  object ElapsedTime extends TinyTypeFactory[ElapsedTime](new ElapsedTime(_)) {
    addConstraint(
      check = _ >= 0,
      message = (_: Long) => s"$typeName cannot be < 0"
    )

    def apply(elapsedTime: FiniteDuration): ElapsedTime = ElapsedTime(elapsedTime.toMillis)
  }
}
